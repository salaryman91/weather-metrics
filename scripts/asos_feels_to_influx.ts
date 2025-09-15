/**
 * ASOS 시간자료(kma_sfctm2.php) 수집 → 체감온도(Heat Index/Wind Chill) 계산 → InfluxDB Cloud 적재
 *
 * 실행(로컬):  npx ts-node scripts/asos_feels_to_influx.ts
 * GitHub Actions: .github/workflows/asos.yml 참고
 *
 * 필요 환경변수:
 *   INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET
 *   APIHUB_BASE=https://apihub.kma.go.kr
 *   APIHUB_KEY=<authKey>
 *   ASOS_STN=108 (예: 서울)
 *   LOC=seoul (선택)
 *
 * 주의: 콘솔 로그에 authKey를 절대 출력하지 않습니다.
 */

// ---------- 타입 & 환경키 헬퍼 ----------
type Env = {
  INFLUX_URL: string;
  INFLUX_TOKEN: string;
  INFLUX_ORG: string;
  INFLUX_BUCKET: string;
  APIHUB_BASE: string;
  APIHUB_KEY: string;
  ASOS_STN?: string;
  LOC?: string;
};
const env = process.env as unknown as Env;
const need = (k: keyof Env): string => {
  const v = env[k];
  if (!v) throw new Error(`Missing env: ${k}`);
  return v;
};

// ---------- 공통 유틸 ----------
/** CSV 한 줄 안전 분할 (따옴표 이스케이프 대응) */
function splitCSVLine(line: string): string[] {
  const out: string[] = [];
  let cur = "";
  let q = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (c === '"') {
      if (q && line[i + 1] === '"') {
        cur += '"';
        i++;
      } else {
        q = !q;
      }
    } else if (c === "," && !q) {
      out.push(cur);
      cur = "";
    } else {
      cur += c;
    }
  }
  out.push(cur);
  return out.map((s) => s.trim());
}

/** 행을 줄 단위로 쪼개고 BOM/빈줄 제거 (주석 라인 유지) */
function toLines(t: string): string[] {
  return t.replace(/\ufeff/g, "").split(/\r?\n/).filter((l) => l.trim().length > 0);
}

/** 한 줄을 CSV 또는 공백 기준으로 파싱(주석 # 제거) */
function splitBy(line: string, mode: "csv" | "ws"): string[] {
  const s = line.replace(/^#\s*/, "").trim();
  return mode === "csv" ? splitCSVLine(s) : s.split(/\s+/);
}

/** 수치 변환(결측치 -9/ -9.0 등은 NaN 처리) */
function toNum(s?: string): number {
  const n = parseFloat(String(s ?? ""));
  return !isFinite(n) || n <= -8.9 ? NaN : n;
}

// ---------- 체감온도 계산 ----------
/** Heat Index(F) → C (Rothfusz regression, 대략적) */
function heatIndexC(tC: number, rh: number): number {
  const T = tC * 9 / 5 + 32;
  const R = rh;
  const HI =
    -42.379 + 2.04901523 * T + 10.14333127 * R - 0.22475541 * T * R
    - 6.83783e-3 * T * T - 5.481717e-2 * R * R + 1.22874e-3 * T * T * R
    + 8.5282e-4 * T * R * R - 1.99e-6 * T * T * R * R;
  return (HI - 32) * 5 / 9;
}

/** Wind Chill 공식(C, 입력 풍속 m/s → km/h 변환) */
function windChillC(tC: number, vMs: number): number {
  const v = vMs * 3.6; // km/h
  if (tC > 10 || v <= 4.8) return tC;
  return 13.12 + 0.6215 * tC - 11.37 * Math.pow(v, 0.16) + 0.3965 * tC * Math.pow(v, 0.16);
}

// ---------- Influx 라이트 ----------
async function writeLP(lines: string[]): Promise<void> {
  const url =
    `${need("INFLUX_URL")}/api/v2/write`
    + `?org=${encodeURIComponent(need("INFLUX_ORG"))}`
    + `&bucket=${encodeURIComponent(need("INFLUX_BUCKET"))}`
    + `&precision=s`;

  const res = await fetch(url, {
    method: "POST",
    headers: {
      Authorization: `Token ${need("INFLUX_TOKEN")}`,
      "Content-Type": "text/plain; charset=utf-8",
    },
    body: lines.join("\n"),
  });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Influx write ${res.status}: ${body}`);
  }
}

// ---------- KMAHub ASOS 파서(공백/CSV + 주석 헤더 모두 대응) ----------
async function fetchLatestASOS(stn: string) {
  const now = new Date();
  const tm2 = now.toISOString().replace(/[-:]/g, "").slice(0, 12) + "00";
  const tm1 = new Date(now.getTime() - 3 * 3600 * 1000).toISOString().replace(/[-:]/g, "").slice(0, 12) + "00";

  const base = need("APIHUB_BASE");
  const url =
    `${base}/api/typ01/url/kma_sfctm2.php`
    + `?stn=${encodeURIComponent(stn)}&tm1=${tm1}&tm2=${tm2}&disp=1&help=1&authKey=${encodeURIComponent(need("APIHUB_KEY"))}`;

  const t0 = Date.now();
  const res = await fetch(url);
  const latency = Date.now() - t0;
  const text = await res.text();
  if (!res.ok) throw new Error(`ASOS ${res.status}: ${text.slice(0, 200)}`);

  const lines = toLines(text);
  const comment = lines.filter((l) => l.trim().startsWith("#"));
  const data = lines.filter((l) => l.trim() && !l.trim().startsWith("#"));

  const headerLine =
    [...comment].reverse().find((l) =>
      /(TM|TIME|DATE).*(TA|기온).*(HM|RH|습도).*(WS|풍속)/i.test(l)
    ) ?? null;

  let mode: "csv" | "ws" = headerLine && headerLine.includes(",") ? "csv" : "ws";
  const header = headerLine ? splitBy(headerLine, mode) : [];

  if (!headerLine && data[0]?.includes(",")) mode = "csv";
  const rows = data.map((l) => splitBy(l, mode)).filter((r) => r.length >= 5);
  if (rows.length === 0) throw new Error("ASOS: no data rows");

  // 1) 이름 기반 1차 매핑
  let iTA = header.findIndex((h) => /^TA$/i.test(h) || /기온/i.test(h));
  let iHM = header.findIndex((h) => /^HM$/i.test(h) || /(RH|습도)/i.test(h));
  let iWS = header.findIndex((h) => /^WS$/i.test(h) || /(풍속|WIND)/i.test(h));
  let iTM = header.findIndex((h) => /^(TM|TIME|DATE)$/i.test(h) || /시각|time/i.test(h));

  // 2) 값 범위 기반 2차 매핑(여러 후보 중 최다 유효값)
  const cols = rows[0].length;
  const numeric = rows.map((r) => r.map(toNum));
  const bestIndex = (ok: (v: number) => boolean) => {
    let best = -1, score = -1;
    for (let c = 0; c < cols; c++) {
      let cnt = 0;
      for (const row of numeric) { const v = row[c]; if (isFinite(v) && ok(v)) cnt++; }
      if (cnt > score) { score = cnt; best = c; }
    }
    return best;
  };
  if (iHM < 0) iHM = bestIndex((v) => v >= 0 && v <= 100);     // RH
  if (iWS < 0) iWS = bestIndex((v) => v >= 0 && v <= 60);       // wind m/s
  if (iTA < 0) iTA = bestIndex((v) => v > -50 && v < 50);       // temp C
  if (iTM < 0 && rows.length) iTM = rows[0].findIndex((v) => /^\d{12,14}$/.test(v));

  // 3) RH가 행태상 "꼬리쪽"에 자주 위치 → 꼬리열 우선 휴리스틱
  if (iHM < 0 || iHM < cols - 12) {
    const tail = Array.from({ length: Math.max(0, Math.min(12, cols)) }, (_, k) => cols - 1 - k).reverse();
    const candidates = tail.filter(c => {
      let cnt = 0;
      for (const row of numeric.slice(-40)) { const v = row[c]; if (isFinite(v) && v >= 0 && v <= 100) cnt++; }
      return cnt >= Math.ceil(Math.min(40, numeric.length) * 0.6);
    });
    if (candidates.length) iHM = candidates[0];
  }

  // 4) HM 주변에 TA/DEW가 붙어있는 경우 많음 → 이웃열 휴리스틱
  const looksTemp = (v: number) => v > -50 && v < 50;
  if (iHM >= 0 && (iTA < 0 || !looksTemp(numeric.at(-1)?.[iTA] ?? NaN))) {
    const neigh = [iHM - 1, iHM + 1].filter(i => i >= 0 && i < cols);
    for (const c of neigh) {
      const vals = numeric.slice(-40).map(r => r[c]).filter(isFinite);
      if (vals.length >= 5) {
        const avg = vals.reduce((a, b) => a + b, 0) / vals.length;
        if (looksTemp(avg)) { iTA = c; break; }
      }
    }
  }

  // 마지막 방어
  if (iTA < 0 || iHM < 0 || iWS < 0) {
    if (process.env.DEBUG_ASOS) {
      console.log("Header(candidate):", header);
      console.log("Sample row:", rows[rows.length - 1]);
      console.log("iTM/iTA/iHM/iWS:", iTM, iTA, iHM, iWS);
    }
    throw new Error("Required columns not found (TA/HM/WS)");
  }

  // 최신 유효행(뒤에서 앞으로)
  for (let k = rows.length - 1; k >= 0; k--) {
    const r = rows[k];
    const tC = toNum(r[iTA]);
    let rh = toNum(r[iHM]);
    const wMs = toNum(r[iWS]);
    if (!isFinite(tC) || !isFinite(wMs)) continue;

    // RH가 5% 미만/100% 초과이면 잘못 매핑된 경우가 많음 → 보정 시도 안 하고 skip
    if (!isFinite(rh) || rh < 5 || rh > 100) continue;

    // 타임스탬프
    let ts = Math.floor(Date.now() / 1000);
    if (iTM >= 0) {
      const raw = rows[k][iTM];
      if (/^\d{12,14}$/.test(raw)) {
        const yyyy = raw.slice(0, 4), MM = raw.slice(4, 6), dd = raw.slice(6, 8), HH = raw.slice(8, 10), mm = raw.slice(10, 12);
        const iso = `${yyyy}-${MM}-${dd}T${HH}:${mm}:00+09:00`;
        const d = new Date(iso);
        if (!isNaN(d.getTime())) ts = Math.floor(d.getTime() / 1000);
      } else {
        const d = new Date(raw.replace(" ", "T") + (/\+/.test(raw) ? "" : "+09:00"));
        if (!isNaN(d.getTime())) ts = Math.floor(d.getTime() / 1000);
      }
    }

    const feels =
      tC >= 27 && rh >= 40 ? heatIndexC(tC, rh)
      : tC <= 10 && wMs > 1.34 ? windChillC(tC, wMs)
      : tC;

    if (process.env.DEBUG_ASOS) {
      console.log({ pickedRow: r, idx: { iTM, iTA, iHM, iWS }, values: { tC, rh, wMs, ts } });
    }
    return { tC, rh, wMs, feels, ts, latency };
  }

  // 모든 행이 RH 비정상으로 스킵된 경우 → 가장 최근 유효행을 강제로 채택(보수적으로)
  const r = rows[rows.length - 1];
  const tC = toNum(r[iTA]);
  const rh = Math.max(5, Math.min(100, toNum(r[iHM]))); // 5~100 클램프
  const wMs = toNum(r[iWS]);
  let ts = Math.floor(Date.now() / 1000);
  if (iTM >= 0) {
    const raw = r[iTM];
    if (/^\d{12,14}$/.test(raw)) {
      const yyyy = raw.slice(0, 4), MM = raw.slice(4, 6), dd = raw.slice(6, 8), HH = raw.slice(8, 10), mm = raw.slice(10, 12);
      const iso = `${yyyy}-${MM}-${dd}T${HH}:${mm}:00+09:00`;
      const d = new Date(iso);
      if (!isNaN(d.getTime())) ts = Math.floor(d.getTime() / 1000);
    }
  }
  const feels =
    isFinite(tC) && isFinite(wMs) && isFinite(rh)
      ? (tC >= 27 && rh >= 40 ? heatIndexC(tC, rh)
        : tC <= 10 && wMs > 1.34 ? windChillC(tC, wMs)
        : tC)
      : tC;
  return { tC, rh, wMs, feels, ts, latency };
}

// ---------- 메인 ----------
(async () => {
  const stn = (env.ASOS_STN || "108").trim();
  const loc = env.LOC || "seoul";

  const { tC, rh, wMs, feels, ts, latency } = await fetchLatestASOS(stn);

  const now = Math.floor(Date.now() / 1000);
  const lines = [
    // 관측 기반 life_index
    `life_index,source=kmahub-asos,loc=${loc},stn=${stn} feels_c=${feels.toFixed(
      2
    )},temp_c=${tC},rh_pct=${rh},wind_ms=${wMs} ${ts}`,
    // 가용성/지연 프로브
    `api_probe,service=asos_feels,env=prod,loc=${loc} success=1i,latency_ms=${latency}i ${now}`,
  ];

  await writeLP(lines);
  console.log(
    `FeelsLike=${feels.toFixed(2)}C, Temp=${tC}C, RH=${rh}%, Wind=${wMs}m/s @ stn=${stn}\n` +
    `Influx write OK`
  );
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
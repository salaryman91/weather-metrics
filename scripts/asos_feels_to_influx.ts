/**
 * ASOS 시간자료(kma_sfctm2.php) 수집 → 체감온도(AT/Wind Chill) 계산 → InfluxDB 적재
 *
 * 실행(로컬):  npx ts-node scripts/asos_feels_to_influx.ts
 *
 * 필요 환경변수:
 *   INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET
 *   APIHUB_BASE=https://apihub.kma.go.kr
 *   APIHUB_KEY=<authKey>
 *   ASOS_STN=108 (예: 서울)
 *   LOC=seoul (선택)
 */

import iconv from "iconv-lite";

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

// ---------- 유틸 ----------
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

function toLinesKR(buf: ArrayBuffer): string[] {
  const text = iconv.decode(Buffer.from(buf), "euc-kr");
  return text.replace(/\ufeff/g, "").split(/\r?\n/);
}

function toNum(s?: string): number {
  const n = parseFloat(String(s ?? "").replace(/[^\d\.\-]/g, ""));
  return Number.isFinite(n) ? n : NaN;
}

const clamp = (v: number, lo: number, hi: number) => Math.min(hi, Math.max(lo, v));

// ---------- 열지수/바람차가 체감 ----------
function apparentTempC(tC: number, rh: number, vMs: number): number {
  // Australian Bureau of Meteorology AT 공식
  const e = (rh / 100) * 6.105 * Math.exp((17.27 * tC) / (237.7 + tC));
  return tC + 0.33 * e - 0.70 * vMs - 4;
}
function windChillC(tC: number, vMs: number): number {
  const v = vMs * 3.6; // km/h
  if (tC > 10 || v <= 4.8) return tC;
  return 13.12 + 0.6215 * tC - 11.37 * Math.pow(v, 0.16) + 0.3965 * tC * Math.pow(v, 0.16);
}
// TD→RH (Magnus)
function rhFromTD(tC: number, tdC: number): number {
  const a = 17.625, b = 243.04;
  const es = Math.exp((a * tC) / (b + tC));
  const e = Math.exp((a * tdC) / (b + tdC));
  return clamp(100 * (e / es), 1, 100);
}

// ---------- Influx ----------
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

// ---------- 파서 ----------
type PickedRow = {
  ts: number;       // epoch seconds (KST)
  tC: number;       // 기온
  rh: number;       // 상대습도(%)
  td?: number;      // 이슬점(선택)
  wMs: number;      // 풍속(m/s)
  raw: string[];    // 원행
};

type FetchOption = { help: 0 | 1; disp: 0 | 1; hours: number };

async function tryFetch(stn: string, opt: FetchOption): Promise<PickedRow | null> {
  const now = new Date();
  const tm2 = now.toISOString().replace(/[-:]/g, "").slice(0, 12) + "00";
  const tm1 = new Date(now.getTime() - opt.hours * 3600 * 1000)
    .toISOString().replace(/[-:]/g, "").slice(0, 12) + "00";
  const base = need("APIHUB_BASE");
  const url =
    `${base}/api/typ01/url/kma_sfctm2.php`
    + `?stn=${encodeURIComponent(stn)}&tm1=${tm1}&tm2=${tm2}`
    + `&disp=${opt.disp}&help=${opt.help}&authKey=${encodeURIComponent(need("APIHUB_KEY"))}`;

  const res = await fetch(url);
  const buf = await res.arrayBuffer();
  if (!res.ok) {
    const head = iconv.decode(Buffer.from(buf).subarray(0, 400), "euc-kr");
    throw new Error(`ASOS ${res.status}: ${head}`);
  }

  const lines = toLinesKR(buf);
  const nonBlank = lines.filter(l => l.trim().length > 0);

  // "자료가 없습니다" 등
  if (nonBlank.some(l => /자료.?없|no data/i.test(l))) return null;

  // 데이터 라인 추출
  let dataLines = nonBlank.filter(l => !l.trim().startsWith("#"));
  if (dataLines.length === 0) {
    dataLines = nonBlank.filter(l => (l.match(/[\d\.]/g) || []).length >= 8);
  }
  if (dataLines.length === 0) return null;

  // 헤더 후보
  const headerLine =
    [...nonBlank].reverse().find((l) =>
      l.trim().startsWith("#") &&
      /(TM|TIME|DATE).*(TA|기온).*(HM|RH|습도).*(WS|풍속)/i.test(l)
    ) ?? null;

  let mode: "csv" | "ws" = (headerLine && headerLine.includes(",")) ? "csv" : "ws";
  if (!headerLine && dataLines[0].includes(",")) mode = "csv";

  const split = (line: string) => {
    const s = headerLine ? line.replace(/^#\s*/, "").trim() : line.trim();
    return mode === "csv" ? splitCSVLine(s) : s.split(/\s+/);
  };

  const header = headerLine ? split(headerLine) : [];
  const rows = dataLines.map(split).filter(r => r.length >= 5);
  if (rows.length === 0) return null;

  // 컬럼 인덱스 탐색
  const cols = rows[0].length;
  const numeric = rows.map((r) => r.map(toNum));

  const findHeaderIdx = (re: RegExp) => header.findIndex(h => re.test(h));
  let iTM = findHeaderIdx(/^(TM|TIME|DATE)$/i);
  let iTA = findHeaderIdx(/^TA$/i) >= 0 ? findHeaderIdx(/^TA$/i) : findHeaderIdx(/기온|TEMP/i);
  let iHM = findHeaderIdx(/^HM$/i) >= 0 ? findHeaderIdx(/^HM$/i) : findHeaderIdx(/(RH|습도)/i);
  let iWS = findHeaderIdx(/^WS$/i) >= 0 ? findHeaderIdx(/^WS$/i) : findHeaderIdx(/(풍속|WIND)/i);
  let iTD = findHeaderIdx(/^TD$/i) >= 0 ? findHeaderIdx(/^TD$/i) : findHeaderIdx(/이슬점|DEW/i);

  const bestIndex = (ok: (v: number) => boolean) => {
    let best = -1, score = -1;
    for (let c = 0; c < cols; c++) {
      let cnt = 0;
      for (const row of numeric) { const v = row[c]; if (Number.isFinite(v) && ok(v)) cnt++; }
      if (cnt > score) { score = cnt; best = c; }
    }
    return best;
  };

  // 헤더 실패 시 값 범위 기반 보정
  if (iTA < 0) iTA = bestIndex(v => v > -50 && v < 50);
  if (iWS < 0) iWS = bestIndex(v => v >= 0 && v <= 60);
  if (iHM < 0) iHM = bestIndex(v => v >= 0 && v <= 100);
  if (iTD < 0) iTD = bestIndex(v => v > -60 && v < 40);
  if (iTM < 0 && rows.length) iTM = rows[0].findIndex(v => /^\d{12,14}$/.test(v));

  // RH 꼬리열/이웃열 휴리스틱
  if (iHM < 0 || iHM < cols - 12) {
    const tail = Array.from({ length: Math.min(12, cols) }, (_, k) => cols - 1 - k).reverse();
    for (const c of tail) {
      const vals = numeric.slice(-40).map(r => r[c]).filter(Number.isFinite);
      const pass = vals.length >= 8 && vals.filter(v => v >= 0 && v <= 100).length / vals.length >= 0.6;
      if (pass) { iHM = c; break; }
    }
  }
  if (iHM >= 0 && iTA >= 0) {
    const neigh = [iHM - 1, iHM + 1].filter(i => i >= 0 && i < cols);
    for (const c of neigh) {
      const vals = numeric.slice(-40).map(r => r[c]).filter(Number.isFinite);
      if (vals.length >= 6) {
        const avg = vals.reduce((a, b) => a + b, 0) / vals.length;
        if (avg > -30 && avg < 40) { iTA = c; break; }
      }
    }
  }

  // 최신에서 과거로 훑으며 유효행 선택
  for (let k = rows.length - 1; k >= 0; k--) {
    const r = rows[k];
    const tC = toNum(r[iTA]);
    const hm = toNum(r[iHM]);
    const td = iTD >= 0 ? toNum(r[iTD]) : NaN;
    const ws = toNum(r[iWS]);

    if (!Number.isFinite(tC) || !(tC > -30 && tC < 45)) continue;

    // RH 확정
    let rhVal = Number.isFinite(hm) ? hm : NaN;
    if (!(rhVal >= 10 && rhVal <= 100) && Number.isFinite(td) && td > -60 && td < 40) {
      rhVal = rhFromTD(tC, td);
    }
    if (!(rhVal >= 10 && rhVal <= 100)) continue;

    // 풍속 확정
    const wMsVal = Number.isFinite(ws) ? clamp(ws, 0, 40) : 0;

    // 타임스탬프
    let ts = Math.floor(Date.now() / 1000);
    if (iTM >= 0) {
      const raw = r[iTM];
      if (/^\d{12,14}$/.test(raw)) {
        const yyyy = raw.slice(0, 4), MM = raw.slice(4, 6), dd = raw.slice(6, 8),
              HH = raw.slice(8, 10), mm = raw.slice(10, 12);
        const iso = `${yyyy}-${MM}-${dd}T${HH}:${mm}:00+09:00`;
        const d = new Date(iso);
        if (!isNaN(d.getTime())) ts = Math.floor(d.getTime() / 1000);
      } else {
        const d = new Date(raw.replace(" ", "T") + (/\+/.test(raw) ? "" : "+09:00"));
        if (!isNaN(d.getTime())) ts = Math.floor(d.getTime() / 1000);
      }
    }

    return {
      ts,
      tC,
      rh: rhVal,
      td: Number.isFinite(td) ? td : undefined,
      wMs: wMsVal,
      raw: r,
    };
  }

  return null;
}

async function fetchLatestASOS(stn: string): Promise<{ tC: number; rh: number; wMs: number; feels: number; ts: number; latency: number; }> {
  const trials: FetchOption[] = [
    { help: 1, disp: 1, hours: 3 },
    { help: 0, disp: 1, hours: 3 },
    { help: 0, disp: 0, hours: 6 },
  ];

  let picked: PickedRow | null = null;
  let lastLatency = 0;

  for (const opt of trials) {
    const t0 = Date.now();
    picked = await tryFetch(stn, opt);
    lastLatency = Date.now() - t0;
    if (picked) break;
  }
  if (!picked) {
    const e = new Error("ASOS: no data rows");
    (e as any).__latency = lastLatency;
    throw e;
  }

  const { tC, rh, wMs, ts } = picked;

  // 3시간 초과 스테일 가드
  const maxAgeSec = 3 * 3600;
  if (Math.floor(Date.now() / 1000) - ts > maxAgeSec) {
    const e = new Error("ASOS: stale data (>3h)");
    (e as any).__latency = lastLatency;
    (e as any).__stale = true;
    throw e;
  }

  // 체감 계산
  const feels =
    (tC >= 26 && rh >= 40) ? apparentTempC(tC, rh, wMs)
    : (tC <= 10 && wMs > 1.34) ? windChillC(tC, wMs)
    : tC;

  return { tC, rh, wMs, feels, ts, latency: lastLatency };
}

// ---------- 메인 ----------
(async () => {
  const stn = (env.ASOS_STN || "108").trim();
  const loc = env.LOC || "seoul";

  try {
    const { tC, rh, wMs, feels, ts, latency } = await fetchLatestASOS(stn);

    const now = Math.floor(Date.now() / 1000);
    const lines = [
      `life_index,source=kmahub-asos,loc=${loc},stn=${stn} ` +
      `feels_c=${feels.toFixed(2)},temp_c=${tC},rh_pct=${rh},wind_ms=${wMs} ${ts}`,
      `api_probe,service=asos_feels,env=prod,loc=${loc} success=1i,latency_ms=${latency}i ${now}`,
    ];

    await writeLP(lines);
    console.log(`FeelsLike=${feels.toFixed(2)}C, Temp=${tC}C, RH=${rh}%, Wind=${wMs}m/s @ stn=${stn}`);
    console.log("Influx write OK");
  } catch (e: any) {
    const locTag = env.LOC || "seoul";
    const now = Math.floor(Date.now() / 1000);
    const latency = Number.isFinite(e?.__latency) ? e.__latency : 0;

    // 실패 시에도 잡이 죽지 않게: 실패 상태만 기록
    const probe = `api_probe,service=asos_feels,env=prod,loc=${locTag} success=0i,latency_ms=${latency}i ${now}`;
    try { await writeLP([probe]); } catch {}
    console.error(e);
    process.exit(0); // 스케줄 유지
  }
})();

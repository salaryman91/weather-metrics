/**
 * ASOS 시간자료(kma_sfctm2.php) → 체감온도(Heat Index/Wind Chill) → InfluxDB Cloud
 *
 * 실행(로컬):  npx ts-node scripts/asos_feels_to_influx.ts
 * GH Actions : .github/workflows/asos.yml
 *
 * ENV:
 *   INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET
 *   APIHUB_BASE=https://apihub.kma.go.kr
 *   APIHUB_KEY=<authKey>
 *   ASOS_STN=108 (예: 서울)
 *   LOC=seoul (선택)
 *
 * DEBUG:
 *   PowerShell: $env:DEBUG_ASOS = "1"
 *   Linux/mac : DEBUG_ASOS=1
 */

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
const need = (k: keyof Env) => {
  const v = env[k];
  if (!v) throw new Error(`Missing env: ${k}`);
  return v;
};
const DBG = !!process.env.DEBUG_ASOS;

/* -------------------- 공통 유틸 -------------------- */
function splitCSVLine(line: string): string[] {
  const out: string[] = [];
  let cur = "", q = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (c === '"') {
      if (q && line[i + 1] === '"') { cur += '"'; i++; }
      else { q = !q; }
    } else if (c === "," && !q) {
      out.push(cur); cur = "";
    } else cur += c;
  }
  out.push(cur);
  return out.map(s => s.trim());
}

const toLines = (t: string) =>
  t.replace(/\ufeff/g, "")
   .split(/\r?\n/)
   .map(s => s.replace(/\s+$/,""))
   .filter(l => l.trim().length > 0);

function splitBy(line: string, mode: "csv" | "ws"): string[] {
  const s = line.replace(/^#\s*/, "").trim();
  return mode === "csv" ? splitCSVLine(s) : s.split(/\s+/);
}

function toNum(s?: string): number {
  const n = parseFloat(String(s ?? "").replace(/,/g, ""));
  // -9/-9.0/-999 등 결측 → NaN
  return !isFinite(n) || n <= -8.9 ? NaN : n;
}

function parseKST12(raw: string): number | null {
  // YYYYMMDDHHmm(SS) or YYYYMMDDHHmm
  if (/^\d{12,14}$/.test(raw)) {
    const yyyy = raw.slice(0,4), MM=raw.slice(4,6), dd=raw.slice(6,8),
          HH = raw.slice(8,10), mm = raw.slice(10,12), ss=(raw.slice(12,14)||"00");
    const iso = `${yyyy}-${MM}-${dd}T${HH}:${mm}:${ss}+09:00`;
    const d = new Date(iso);
    if (!isNaN(d.getTime())) return Math.floor(d.getTime()/1000);
  }
  return null;
}

function bestIndex(rows: number[][], ok: (v:number)=>boolean, prefer?: (vals:number[])=>number) {
  const cols = rows[0]?.length ?? 0;
  let best = -1, score = -1;
  for (let c=0;c<cols;c++) {
    const vals = rows.map(r => r[c]).filter(isFinite);
    const cnt  = vals.filter(ok).length;
    if (!cnt) continue;
    const sc = prefer ? prefer(vals) : cnt;
    if (sc > score) { score = sc; best = c; }
  }
  return best;
}

function median(a: number[]) {
  if (!a.length) return NaN;
  const b = [...a].sort((x,y)=>x-y);
  const m = Math.floor(b.length/2);
  return b.length%2 ? b[m] : (b[m-1]+b[m])/2;
}

/* -------------------- 체감온도 -------------------- */
// Rothfusz (°F), 반환 °C
function heatIndexC(tC: number, rh: number): number {
  const T = tC * 9/5 + 32, R = rh;
  const HI = -42.379 + 2.04901523*T + 10.14333127*R - 0.22475541*T*R
           - 6.83783e-3*T*T - 5.481717e-2*R*R + 1.22874e-3*T*T*R
           + 8.5282e-4*T*R*R - 1.99e-6*T*T*R*R;
  return (HI - 32) * 5/9;
}

function windChillC(tC: number, vMs: number): number {
  const v = vMs * 3.6; // km/h
  if (tC > 10 || v <= 4.8) return tC;
  return 13.12 + 0.6215*tC - 11.37*Math.pow(v,0.16) + 0.3965*tC*Math.pow(v,0.16);
}

/* -------------------- Influx -------------------- */
async function writeLP(lines: string[]) {
  const url = `${need("INFLUX_URL")}/api/v2/write?org=${encodeURIComponent(need("INFLUX_ORG"))}&bucket=${encodeURIComponent(need("INFLUX_BUCKET"))}&precision=s`;
  const res = await fetch(url, {
    method: "POST",
    headers: { Authorization: `Token ${need("INFLUX_TOKEN")}`, "Content-Type": "text/plain; charset=utf-8" },
    body: lines.join("\n")
  });
  if (!res.ok) throw new Error(`Influx write ${res.status}: ${await res.text()}`);
}

/* -------------------- ASOS 파서 -------------------- */
/**
 * 열 이름 단서 찾기 (help=1 코멘트 블럭에 종종 전체 헤더 라인이 함께 제공됨)
 */
function headerTokens(commentLines: string[], rowLen: number): string[] | null {
  for (const line of commentLines) {
    const s = line.replace(/^#\s*/,"").trim();
    if (!/(TM|TIME|DATE)/i.test(s)) continue;
    if (!/(TA|기온)/i.test(s)) continue;
    if (!/(HM|REH|RH|습도)/i.test(s)) continue;
    if (!/(WS|WSD|풍속)/i.test(s)) continue;
    const toks = s.split(/\s+/);
    if (toks.length === rowLen) return toks;
  }
  return null;
}

function idxByName(toks: string[]|null, re: RegExp): number {
  if (!toks) return -1;
  return toks.findIndex(t => re.test(t));
}

async function fetchLatestASOS(stn: string) {
  // 3시간 창으로 요청 → 최신행 사용
  const now = new Date();
  const tm2 = now.toISOString().replace(/[-:]/g,"").slice(0,12) + "00";
  const tm1 = new Date(now.getTime()-3*3600*1000).toISOString().replace(/[-:]/g,"").slice(0,12) + "00";

  const base = need("APIHUB_BASE");
  const url =
    `${base}/api/typ01/url/kma_sfctm2.php?stn=${encodeURIComponent(stn)}&tm1=${tm1}&tm2=${tm2}&disp=1&help=1&authKey=${encodeURIComponent(need("APIHUB_KEY"))}`;

  const t0 = Date.now();
  const res = await fetch(url);
  const latency = Date.now() - t0;
  const text = await res.text();
  if (!res.ok) throw new Error(`ASOS ${res.status}: ${text.slice(0,200)}`);

  const lines = toLines(text);
  const comments = lines.filter(l => l.startsWith("#"));
  const data     = lines.filter(l => !l.startsWith("#"));

  if (data.length === 0) throw new Error("ASOS: no data rows");

  const mode: "csv"|"ws" = data[0].includes(",") ? "csv" : "ws";
  const rowsS = data.map(l => splitBy(l, mode)).filter(r => r.length >= 5);

  // 숫자 행으로 변환
  const rowsN: number[][] = rowsS.map(r => r.map(toNum));

  // 헤더 토큰 → 직접 매핑 시도
  const toks = headerTokens(comments, rowsS[0].length);

  // 1차: 이름으로 찾기 (동의어 포함)
  let iTM = idxByName(toks, /^(TM|YYMMDDHHMI|DATE|TIME)$/i);
  let iTA = idxByName(toks, /^(TA|TEMP|기온)$/i);
  let iHM = idxByName(toks, /^(HM|REH|RH|습도)$/i);
  let iWS = idxByName(toks, /^(WS|WSD|WIND|풍속)$/i);

  // 2차: 통계 기반 보정
  const tailPrefer = (vals:number[]) => -median(vals); // RH는 보통 값이 큰 편(30~100) → median이 큰 컬럼 선호
  if (iTA < 0) iTA = bestIndex(rowsN, v => v > -50 && v < 50);
  if (iWS < 0) iWS = bestIndex(rowsN, v => v >= 0 && v <= 60, vals => vals.filter(v=>v>1.0).length);
  if (iHM < 0) iHM = bestIndex(rowsN, v => v >= 0 && v <= 100, tailPrefer);

  // 시간열 추정 (숫자/문자 혼재 방지: 문자열 원본에서 판단)
  if (iTM < 0) {
    const last = rowsS.at(-1)!;
    iTM = last.findIndex(v => /^\d{12,14}$/.test(String(v)));
  }

  if (DBG) {
    console.log("Header tokens:", toks);
    console.log("Index guess (iTM/iTA/iHM/iWS):", iTM, iTA, iHM, iWS);
    console.log("Sample last row:", rowsS.at(-1));
  }

  if (iTA < 0 || iHM < 0 || iWS < 0) {
    throw new Error("Required columns not found (TA/HM/WS)");
  }

  // 최신 유효행부터 역순 탐색
  for (let k = rowsS.length - 1; k >= 0; k--) {
    const raw = rowsS[k], num = rowsN[k];
    const tC  = num[iTA], rh0 = num[iHM], wMs = num[iWS];
    if (!isFinite(tC) || !isFinite(wMs)) continue;

    // RH 재평가: 더운 시간인데 RH<20%면 의심 → 대체 후보 찾기
    let rh = rh0;
    if (tC >= 15 && (!isFinite(rh) || rh < 20 || rh > 100)) {
      // 0~100 범위 후보 중 median이 큰 컬럼을 우선
      let best=-1, bestMed=-1;
      const cols = num.length;
      for (let c=0;c<cols;c++){
        if (c===iTA || c===iWS || c===iTM) continue;
        const vals = rowsN.map(r => r[c]).filter(isFinite);
        if (!vals.length) continue;
        const ok = vals.filter(v => v>=0 && v<=100);
        if (ok.length/vals.length < 0.8) continue;
        const med = median(ok);
        if (med > bestMed) { bestMed = med; best = c; }
      }
      if (best >= 0) {
        const candidate = num[best];
        if (isFinite(candidate) && candidate >= 0 && candidate <= 100) {
          if (DBG) console.log(`RH re-mapped: ${iHM} -> ${best} (median ${bestMed.toFixed(1)})`);
          rh = candidate;
        }
      }
    }

    if (!isFinite(rh) || rh < 0 || rh > 100) continue;

    // 타임스탬프
    let ts = Math.floor(Date.now()/1000);
    if (iTM >= 0) {
      const tstr = String(raw[iTM] ?? "");
      const t = parseKST12(tstr);
      if (t) ts = t;
    }

    // 체감온도
    const feels =
      (tC >= 27 && rh >= 40) ? heatIndexC(tC, rh)
      : (tC <= 10 && wMs > 1.34) ? windChillC(tC, wMs)
      : tC;

    if (DBG) {
      console.log({ pickedRow: raw, idx:{iTM,iTA,iHM,iWS}, vals:{tC, rh, wMs, feels: +feels.toFixed(2), ts} });
    }
    return { tC, rh, wMs, feels, ts, latency };
  }

  throw new Error("ASOS: no valid row after scanning");
}

/* -------------------- main -------------------- */
(async () => {
  const stn = (env.ASOS_STN || "108").trim();
  const loc = env.LOC || "seoul";

  const { tC, rh, wMs, feels, ts, latency } = await fetchLatestASOS(stn);

  const now = Math.floor(Date.now()/1000);
  const lines = [
    `life_index,source=kmahub-asos,loc=${loc},stn=${stn} feels_c=${+feels.toFixed(2)},temp_c=${tC},rh_pct=${rh},wind_ms=${wMs} ${ts}`,
    `api_probe,service=asos_feels,env=prod,loc=${loc} success=1i,latency_ms=${latency}i ${now}`,
  ];
  await writeLP(lines);

  console.log(`FeelsLike=${feels.toFixed(2)}C, Temp=${tC}C, RH=${rh}%, Wind=${wMs}m/s @ stn=${stn}`);
  console.log("Influx write OK");
})().catch(e => { console.error(e); process.exit(1); });

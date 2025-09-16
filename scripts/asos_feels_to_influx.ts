/**
 * KMAHub ASOS(kma_sfctm2.php) → InfluxDB: 체감온도(Heat Index / Wind Chill)
 *
 * 전략
 * - 최근 2시간 KST 기준, 10분 단위 tm 후보를 생성해 stn 단일시각 응답을 우선 조회
 * - disp=0/help=0, disp=1/help=1 두 가지 포맷을 모두 시도
 * - 코멘트 헤더에서 TA / (HM|REH|RH) / (WS|WSD) 인덱스를 "정확히" 찾을 때만 채택
 * - 헤더 미탐지/값 비정상 시, 해당 응답은 스킵하고 다음 후보로 진행(오기록 방지)
 * - EUC-KR/UTF-8 자동 디코딩(iconv-lite)
 *
 * ENV:
 *   INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET
 *   APIHUB_BASE=https://apihub.kma.go.kr
 *   APIHUB_KEY=<authKey>
 *   ASOS_STN=108
 *   LOC=seoul (opt)
 *
 * DEBUG:
 *   PowerShell: $env:DEBUG_ASOS = "1"
 *   bash      : DEBUG_ASOS=1
 */

import * as iconv from "iconv-lite";

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
  const v = env[k]; if (!v) throw new Error(`Missing env: ${k}`); return v;
};
const DBG = !!process.env.DEBUG_ASOS;

/* -------------------- 공통 유틸 -------------------- */

function toLines(t: string) {
  return t.replace(/\ufeff/g, "")
    .split(/\r?\n/)
    .map(s => s.replace(/\s+$/,""))
    .filter(l => l.trim().length > 0);
}

function splitCSVLine(line: string): string[] {
  const out: string[] = []; let cur = ""; let q = false;
  for (let i=0;i<line.length;i++){
    const c=line[i];
    if (c === '"') {
      if (q && line[i+1] === '"') { cur += '"'; i++; }
      else q = !q;
    } else if (c === "," && !q) { out.push(cur); cur=""; }
    else cur += c;
  }
  out.push(cur);
  return out.map(s => s.trim());
}

function splitBy(line: string, mode: "csv" | "ws") {
  const s = line.replace(/^#\s*/, "").trim();
  return mode === "csv" ? splitCSVLine(s) : s.split(/\s+/);
}

function toNum(s?: string) {
  const n = parseFloat(String(s ?? "").replace(/,/g,""));
  // -9/-999 등 결측 → NaN
  return !isFinite(n) || n <= -8.9 ? NaN : n;
}

async function decodeKR(res: Response): Promise<string> {
  const ab = await res.arrayBuffer();
  const buf = Buffer.from(ab);
  const ct = (res.headers.get("content-type") || "").toLowerCase();
  if (/euc-?kr|ks_c_5601|cp949/.test(ct)) return iconv.decode(buf, "euc-kr");
  if (/utf-?8/.test(ct)) return buf.toString("utf8");
  const utf = buf.toString("utf8");
  if (utf.includes("\uFFFD")) return iconv.decode(buf, "euc-kr");
  return utf;
}

/* -------------------- 시간 후보(KST, 10분 단위) -------------------- */

function kstNow() {
  return new Date(Date.now() + 9*3600*1000);
}
function fmtKSTYYYYMMDDHHmm(d: Date) {
  const yyyy = d.getUTCFullYear();
  const MM   = String(d.getUTCMonth()+1).padStart(2,"0");
  const dd   = String(d.getUTCDate()).padStart(2,"0");
  const HH   = String(d.getUTCHours()).padStart(2,"0");
  const mm   = String(d.getUTCMinutes()).padStart(2,"0");
  return `${yyyy}${MM}${dd}${HH}${mm}`;
}
function tm10Candidates(hours=2): string[] {
  const out: string[] = [];
  const now = kstNow();
  const base = new Date(now.getTime());
  base.setUTCSeconds(0,0);
  base.setUTCMinutes(Math.floor(base.getUTCMinutes()/10)*10); // 10분 바닥
  for (let i=0;i<=hours*6;i++) {
    const d = new Date(base.getTime() - i*10*60*1000);
    out.push(fmtKSTYYYYMMDDHHmm(d));
  }
  return out;
}

/* -------------------- 헤더 파싱 -------------------- */

function splitHeaderData(all: string[]) {
  const comments = all.filter(l => l.trim().startsWith("#"));
  const rest     = all.filter(l => !l.trim().startsWith("#"));
  return { comments, rest };
}

/** 코멘트 헤더에서 실제 데이터 열 갯수와 동일한 "토큰 라인"을 찾아 반환 */
function headerTokens(comments: string[], rowLen: number): string[] | null {
  // 가장 긴 후보 라인부터 검사
  const sorted = [...comments].sort((a,b)=>b.length-a.length);
  for (const line of sorted) {
    const s = line.replace(/^#\s*/, "").trim();
    // 핵심 키워드 모두 존재해야 함
    if (!/(TM|TIME|DATE)/i.test(s)) continue;
    if (!/(TA|TEMP|기온)/i.test(s)) continue;
    if (!/(HM|REH|RH|습도)/i.test(s)) continue;
    if (!/(WS|WSD|WIND|풍속)/i.test(s)) continue;
    // 콤마/공백 분할 시도
    const toksCsv = s.split(",").map(x=>x.trim()).filter(Boolean);
    const csvOK = toksCsv.length === rowLen;
    const toksWs  = s.split(/\s+/);
    const wsOK  = toksWs.length === rowLen;
    if (csvOK) return toksCsv;
    if (wsOK)  return toksWs;
  }
  return null;
}

function idxByName(toks: string[]|null, re: RegExp): number {
  if (!toks) return -1;
  return toks.findIndex(t => re.test(t));
}

/* -------------------- 체감온도 -------------------- */
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
    method:"POST",
    headers:{ Authorization:`Token ${need("INFLUX_TOKEN")}`, "Content-Type":"text/plain; charset=utf-8" },
    body: lines.join("\n")
  });
  if (!res.ok) throw new Error(`Influx write ${res.status}: ${await res.text()}`);
}

/* -------------------- 수집 핵심 -------------------- */

async function tryFetchOne(stn: string, tm: string, disp: 0|1, help: 0|1) {
  const base = need("APIHUB_BASE");
  const key  = need("APIHUB_KEY");
  const url  = `${base}/api/typ01/url/kma_sfctm2.php?stn=${stn}&tm=${tm}&disp=${disp}&help=${help}&authKey=${key}`;

  const t0 = Date.now();
  const res  = await fetch(url);
  const text = await decodeKR(res);
  const latency = Date.now() - t0;

  if (!res.ok) return { ok:false as const, why:`HTTP ${res.status}`, latency };

  const raw = toLines(text);
  const { comments, rest } = splitHeaderData(raw);
  if (rest.length === 0) return { ok:false as const, why:"no data rows", latency };

  const mode: "csv"|"ws" = rest[0].includes(",") ? "csv" : "ws";
  const rowsS = rest.map(l => splitBy(l, mode)).filter(r => r.length >= 5);
  const rowsN = rowsS.map(r => r.map(toNum));

  // 단일 시각 응답이어도 간혹 여러 줄이 있을 수 있어 최신(마지막) 채택
  const rowS = rowsS.at(-1)!;
  const rowN = rowsN.at(-1)!;

  const toks = headerTokens(comments, rowS.length);
  const iTA  = idxByName(toks, /^(TA|TEMP|기온)$/i);
  const iHM  = idxByName(toks, /^(HM|REH|RH|습도)$/i);
  const iWS  = idxByName(toks, /^(WS|WSD|WIND|풍속)$/i);

  if (DBG) {
    console.log("ASOS url=", url.replace(key, "***"));
    console.log("header tokens? ", !!toks, toks || "(none)");
    console.log("idx TA/HM/WS:", iTA, iHM, iWS);
    console.log("sample row:", rowS.slice(0, 16)); // 너무 길면 앞부분만
  }

  // 헤더 매핑 실패 → 이 응답은 사용하지 않음
  if (iTA < 0 || iHM < 0 || iWS < 0) {
    return { ok:false as const, why:"header not matched", latency };
  }

  const tC = rowN[iTA], rh = rowN[iHM], wMs = rowN[iWS];

  // 값 검증
  if (!(isFinite(tC) && tC > -60 && tC < 60)) return { ok:false as const, why:`bad TA ${tC}`, latency };
  if (!(isFinite(rh) && rh >= 0 && rh <= 100)) return { ok:false as const, why:`bad RH ${rh}`, latency };
  if (!(isFinite(wMs) && wMs >= 0 && wMs <= 60)) return { ok:false as const, why:`bad WS ${wMs}`, latency };

  // 체감 계산
  const feels =
    (tC >= 27 && rh >= 40) ? heatIndexC(tC, rh)
  : (tC <= 10 && wMs > 1.34) ? windChillC(tC, wMs)
  : tC;

  // tm → KST epoch
  const yyyy = tm.slice(0,4), MM=tm.slice(4,6), dd=tm.slice(6,8), HH=tm.slice(8,10), mm=tm.slice(10,12);
  const d = new Date(`${yyyy}-${MM}-${dd}T${HH}:${mm}:00+09:00`);
  const ts = Math.floor(d.getTime()/1000);

  if (DBG) console.log({ tm, picked: { tC, rh, wMs, feels:+feels.toFixed(2) }, ts, latency });

  return { ok:true as const, tC, rh, wMs, feels, ts, latency };
}

async function fetchLatestASOS(stn: string) {
  const tms = tm10Candidates(2); // 최근 2시간(10분 간격)
  const variants: Array<[0|1,0|1]> = [
    [0,0], [1,1]
  ];

  let lastErr = "no candidate worked";
  for (const tm of tms) {
    for (const [disp,help] of variants) {
      const r = await tryFetchOne(stn, tm, disp, help);
      if (r.ok) return r;
      lastErr = `${r.why} @tm=${tm}, disp=${disp}, help=${help}`;
      if (DBG) console.log("skip:", lastErr);
    }
  }
  throw new Error(`ASOS fetch failed: ${lastErr}`);
}

/* -------------------- main -------------------- */

(async () => {
  const stn = (env.ASOS_STN || "108").trim();
  const loc = env.LOC || "seoul";

  const { tC, rh, wMs, feels, ts, latency } = await fetchLatestASOS(stn);

  const now = Math.floor(Date.now()/1000);
  const lines = [
    `life_index,source=kmahub-asos,loc=${loc},stn=${stn} feels_c=${+feels.toFixed(2)},temp_c=${tC},rh_pct=${rh},wind_ms=${wMs} ${ts}`,
    `api_probe,service=asos_feels,env=prod,loc=${loc} success=1i,latency_ms=${latency}i ${now}`
  ];
  await writeLP(lines);

  console.log(`FeelsLike=${(+feels.toFixed(2))}C, Temp=${tC}C, RH=${rh}%, Wind=${wMs}m/s @ stn=${stn}`);
  console.log("Influx write OK");
})().catch(e => { console.error(e); process.exit(1); });

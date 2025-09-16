/**
 * KMAHub ASOS → Apparent Temperature(체감온도) 계산 → InfluxDB 적재
 * - 기본: typ01/url/kma_sfctm3.php (기간조회, tm1/tm2)
 * - 폴백: typ01/url/kma_sfctm2.php (단일시각, tm)  ← m3이 0건일 때
 *
 * ENV (필수): INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET
 * ENV (필수): APIHUB_BASE=https://apihub.kma.go.kr, APIHUB_KEY=xxxxx
 * ENV (선택): ASOS_STN=108, LOC=seoul
 *             ASOS_API_PATH_M3=api/typ01/url/kma_sfctm3.php
 *             ASOS_API_PATH_M2=api/typ01/url/kma_sfctm2.php
 *             ASOS_COL_TM=1 ASOS_COL_STN=2 ASOS_COL_WS=4 ASOS_COL_HM=7 ASOS_COL_TA=8 ASOS_COL_TD=9 (1-based)
 */

import iconv from "iconv-lite";

// ---------- Env helpers ----------
function reqEnv(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`Missing env: ${name}`);
  return v;
}
function optInt(name: string, def?: number): number | undefined {
  const v = process.env[name];
  if (v == null || v === "") return def;
  const n = Number(v);
  return Number.isFinite(n) ? n : def;
}

// ---------- Time (KST) ----------
const KST_OFFSET_MIN = 9 * 60;
const pad = (n: number, w = 2) => n.toString().padStart(w, "0");

function floorToMinuteStepKST(dUtc: Date, stepMin = 10): Date {
  const kstMs = dUtc.getTime() + KST_OFFSET_MIN * 60 * 1000;
  const kst = new Date(kstMs);
  const m = kst.getUTCMinutes();
  kst.setUTCMinutes(Math.floor(m / stepMin) * stepMin, 0, 0);
  return new Date(kst.getTime() - KST_OFFSET_MIN * 60 * 1000);
}
function floorToHourKST(dUtc: Date): Date {
  const kstMs = dUtc.getTime() + KST_OFFSET_MIN * 60 * 1000;
  const kst = new Date(kstMs);
  kst.setUTCMinutes(0, 0, 0);
  return new Date(kst.getTime() - KST_OFFSET_MIN * 60 * 1000);
}
function fmtYYYYMMDDHHmm_KST(dUtc: Date): string {
  const kst = new Date(dUtc.getTime() + KST_OFFSET_MIN * 60 * 1000);
  return (
    kst.getUTCFullYear().toString() +
    pad(kst.getUTCMonth() + 1) +
    pad(kst.getUTCDate()) +
    pad(kst.getUTCHours()) +
    pad(kst.getUTCMinutes())
  );
}
function makeWindow(nowUtc = new Date()) {
  const tm2Utc = floorToMinuteStepKST(new Date(nowUtc.getTime() - 10 * 60 * 1000), 10); // now-10m
  const tm1Utc = new Date(tm2Utc.getTime() - 90 * 60 * 1000); // -90m
  return { tm1: fmtYYYYMMDDHHmm_KST(tm1Utc), tm2: fmtYYYYMMDDHHmm_KST(tm2Utc) };
}
function kstTmToEpochNs(tm: string): bigint {
  const y = Number(tm.slice(0, 4));
  const M = Number(tm.slice(4, 6)) - 1;
  const D = Number(tm.slice(6, 8));
  const h = Number(tm.slice(8, 10));
  const m = Number(tm.slice(10, 12));
  const ms = Date.UTC(y, M, D, h - 9, m, 0, 0);
  return BigInt(ms) * 1_000_000n;
}

// ---------- Math ----------
function clamp(v: number, min: number, max: number) {
  return Math.max(min, Math.min(max, v));
}
function rhFromTd(T: number, Td: number): number {
  const a = 17.27, b = 237.7;
  const es = 6.105 * Math.exp((a * T) / (b + T));
  const e = 6.105 * Math.exp((a * Td) / (b + Td));
  return clamp((e / es) * 100, 1, 100);
}
function feelsAT(T: number, RH: number, ws: number): number {
  const e = (RH / 100) * 6.105 * Math.exp((17.27 * T) / (237.7 + T));
  return T + 0.33 * e - 0.7 * ws - 4.0;
}

// ---------- HTTP ----------
async function httpGetArrayBuffer(url: string): Promise<{ buf: ArrayBuffer; latency: number; status: number; ok: boolean; }> {
  const t0 = Date.now();
  const res = await fetch(url);
  const latency = Date.now() - t0;
  const buf = await res.arrayBuffer();
  return { buf, latency, status: res.status, ok: res.ok };
}
async function writeLP(lines: string[]): Promise<void> {
  const url = `${reqEnv("INFLUX_URL").replace(/\/+$/, "")}/api/v2/write?org=${encodeURIComponent(
    reqEnv("INFLUX_ORG")
  )}&bucket=${encodeURIComponent(reqEnv("INFLUX_BUCKET"))}&precision=ns`;
  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "text/plain; charset=utf-8", Authorization: `Token ${reqEnv("INFLUX_TOKEN")}` },
    body: lines.join("\n"),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Influx write failed: ${res.status} ${res.statusText} - ${text}`);
  }
}

// ---------- Parsing ----------
type AsosRow = { tm: string; stn: string; ta?: number; hm?: number; ws?: number; td?: number; };

function splitTokens(line: string): string[] {
  const byComma = line.split(",");
  if (byComma.length > 1) return byComma.map((s) => s.trim()).filter(Boolean);
  return line.split(/\s+/).map((s) => s.trim()).filter(Boolean);
}
function pickNum(tokens: string[], idx?: number): number | undefined {
  if (!idx) return undefined;
  const i = idx - 1;
  if (i < 0 || i >= tokens.length) return undefined;
  const v = Number(tokens[i]);
  return Number.isFinite(v) ? v : undefined;
}
function parseLatestRow(
  lines: string[],
  col: { tm?: number; stn?: number; ta?: number; hm?: number; ws?: number; td?: number },
  filterStn?: string
): AsosRow | null {
  const clean = lines.map((s) => s.trim()).filter((s) => s && !s.startsWith("#") && /\d/.test(s));
  if (clean.length === 0) return null;

  // 뒤에서부터(최신) 탐색: stn 필터가 있으면 일치행만 선택
  for (let idx = clean.length - 1; idx >= 0; idx--) {
    const tokens = splitTokens(clean[idx]);
    const stnToken = col.stn ? tokens[col.stn - 1] : tokens.find((t) => /^\d{3,5}$/.test(t));
    if (filterStn && stnToken !== filterStn) continue;

    let tm = col.tm ? tokens[col.tm - 1] : tokens.find((t) => /^\d{12}$/.test(t));
    let ta = pickNum(tokens, col.ta);
    let hm = pickNum(tokens, col.hm);
    let ws = pickNum(tokens, col.ws);
    let td = pickNum(tokens, col.td);

    // 보수적 자동 추정(없을 때만)
    const nums = tokens.map((t) => Number(t)).filter((v) => Number.isFinite(v));
    if (ta == null) ta = nums.find((v) => v >= -60 && v <= 60);
    if (hm == null) hm = nums.find((v) => v >= 0 && v <= 100);
    if (ws == null) ws = nums.find((v) => v >= 0 && v <= 70);
    if (td == null) td = nums.find((v) => v >= -60 && v <= 40);

    if (tm && stnToken) return { tm, stn: stnToken, ta, hm, ws, td };
  }
  return null;
}

function dumpSample(raw: string) {
  const lines = raw.split(/\r?\n/).slice(0, 8).map(s => s.length > 160 ? s.slice(0,160) + "…" : s);
  console.warn("[ASOS] sample\n" + lines.join("\n"));
}

// ---------- Fetchers ----------
async function fetchPeriod_M3(stn: string, base: string, path: string) {
  let win = makeWindow();
  const tried: string[] = [];
  for (let i = 0; i < 3; i++) {
    const url =
      `${base.replace(/\/+$/, "")}/${path.replace(/^\/+/, "")}` +
      `?stn=${encodeURIComponent(stn)}&tm1=${win.tm1}&tm2=${win.tm2}&help=0&authKey=${encodeURIComponent(reqEnv("APIHUB_KEY"))}`;

    const { buf, latency, status, ok } = await httpGetArrayBuffer(url);
    const text = iconv.decode(Buffer.from(buf), "euc-kr");
    const lines = text.split(/\r?\n/);

    console.log(`[ASOS M3] stn=${stn} tm1=${win.tm1} tm2=${win.tm2} lines=${lines.length} http=${status} ok=${ok} latency=${latency}ms`);

    const row = parseLatestRow(lines, {
      tm: optInt("ASOS_COL_TM", 1),
      stn: optInt("ASOS_COL_STN", 2),
      ta: optInt("ASOS_COL_TA", 8),
      hm: optInt("ASOS_COL_HM", 7),
      ws: optInt("ASOS_COL_WS", 4),
      td: optInt("ASOS_COL_TD", 9),
    }, stn);

    if (row) return { row, latency, source: "m3" };

    tried.push(`${win.tm1}-${win.tm2}`);
    if (i === 0) dumpSample(text); // 최초 실패 시 원문 샘플 로그

    // -60m 백오프
    const tm2Kst = win.tm2;
    const y = Number(tm2Kst.slice(0,4)), M = Number(tm2Kst.slice(4,6))-1, D = Number(tm2Kst.slice(6,8));
    const h = Number(tm2Kst.slice(8,10)), m = Number(tm2Kst.slice(10,12));
    const tm2Utc = new Date(Date.UTC(y,M,D,h-9,m));
    const older = new Date(tm2Utc.getTime() - 60*60*1000);
    win = makeWindow(older);
  }
  throw new Error(`ASOS(M3): no data rows (windows=${tried.join(", ")})`);
}

function buildTmCandidates(nowUtc = new Date(), hours = 6): string[] {
  const base = floorToHourKST(new Date(nowUtc.getTime() - 10 * 60 * 1000)); // now-10m → 정시 내림
  const list: string[] = [];
  for (let i = 0; i <= hours; i++) {
    const d = new Date(base.getTime() - i * 60 * 60 * 1000);
    list.push(fmtYYYYMMDDHHmm_KST(d).slice(0, 10) + "00"); // HH00
  }
  return list;
}

async function fetchSingle_M2(stn: string, base: string, path: string, tmList: string[]) {
  for (const tm of tmList) {
    const url =
      `${base.replace(/\/+$/, "")}/${path.replace(/^\/+/, "")}` +
      `?stn=${encodeURIComponent(stn)}&tm=${tm}&help=0&authKey=${encodeURIComponent(reqEnv("APIHUB_KEY"))}`;

    const { buf, latency, status, ok } = await httpGetArrayBuffer(url);
    const text = iconv.decode(Buffer.from(buf), "euc-kr");
    const lines = text.split(/\r?\n/);

    console.log(`[ASOS M2] stn=${stn} tm=${tm} lines=${lines.length} http=${status} ok=${ok} latency=${latency}ms`);

    const row = parseLatestRow(lines, {
      tm: optInt("ASOS_COL_TM", 1),
      stn: optInt("ASOS_COL_STN", 2),
      ta: optInt("ASOS_COL_TA", 8),
      hm: optInt("ASOS_COL_HM", 7),
      ws: optInt("ASOS_COL_WS", 4),
      td: optInt("ASOS_COL_TD", 9),
    }, stn);

    if (row) return { row, latency, source: "m2" as const };
    // 첫 번째 실패 샘플만 덤프
    if (tm === tmList[0]) dumpSample(text);
  }
  throw new Error(`ASOS(M2): no data rows (tm candidates=${tmList.join(", ")})`);
}

// ---------- Main ----------
(async () => {
  const INFLUX_URL = reqEnv("INFLUX_URL");
  const INFLUX_TOKEN = reqEnv("INFLUX_TOKEN");
  const INFLUX_ORG = reqEnv("INFLUX_ORG");
  const INFLUX_BUCKET = reqEnv("INFLUX_BUCKET");
  void INFLUX_URL; void INFLUX_TOKEN; void INFLUX_ORG; void INFLUX_BUCKET;

  const APIHUB_BASE = reqEnv("APIHUB_BASE");
  const ASOS_STN = process.env.ASOS_STN || "108";
  const LOC = process.env.LOC || "seoul";
  const PATH_M3 = process.env.ASOS_API_PATH_M3 || "api/typ01/url/kma_sfctm3.php";
  const PATH_M2 = process.env.ASOS_API_PATH_M2 || "api/typ01/url/kma_sfctm2.php";

  const t0 = Date.now();
  const nowNs = BigInt(Date.now()) * 1_000_000n;

  try {
    // 1) m3 우선
    let out: { row: AsosRow; latency: number; source: "m3" | "m2" };
    try {
      out = await fetchPeriod_M3(ASOS_STN, APIHUB_BASE, PATH_M3);
    } catch (e) {
      console.warn(String(e));
      // 2) m2 폴백 (최근 정시 0~-6h)
      const tms = buildTmCandidates(new Date(), 6);
      out = await fetchSingle_M2(ASOS_STN, APIHUB_BASE, PATH_M2, tms);
    }

    const { row, latency, source } = out;
    if (!row.tm || !row.stn) throw new Error("ASOS: parsed row missing tm/stn");

    const T = row.ta ?? NaN;
    const WS = clamp(row.ws ?? 0, 0, 70);
    let RH = row.hm ?? NaN;

    if (!Number.isFinite(RH) && Number.isFinite(row.td) && Number.isFinite(T)) {
      RH = rhFromTd(T, row.td!);
      console.warn(`RH missing → estimated from Td: RH≈${RH.toFixed(1)}%`);
    }
    RH = clamp(RH, 1, 100);
    if (!Number.isFinite(T) || !Number.isFinite(RH)) throw new Error(`ASOS: insufficient fields (T=${T}, RH=${RH}, WS=${WS})`);

    const feels = feelsAT(T, RH, WS);

    // 신선도 가드(now-3h)
    const tsNs = kstTmToEpochNs(row.tm);
    const threeHNs = 3n * 60n * 60n * 1_000_000_000n;
    if (nowNs - tsNs > threeHNs) {
      console.warn(`Skip write: stale obs ts=${row.tm} (source=${source})`);
      await writeLP([`api_probe,service=asos_feels,env=prod,loc=${LOC} success=0i,latency_ms=${Date.now() - t0}i ${nowNs}`]);
      return;
    }

    const lp = [
      `life_index,source=kmahub-asos,loc=${LOC},stn=${row.stn} feels_c=${feels.toFixed(2)},temp_c=${T},rh_pct=${RH},wind_ms=${WS} ${tsNs}`,
      `api_probe,service=asos_feels,env=prod,loc=${LOC} success=1i,latency_ms=${latency}i ${nowNs}`,
    ];
    await writeLP(lp);

    console.log(`OK(${source}) AT=${feels.toFixed(2)}C | T=${T}C RH=${RH}% WS=${WS}m/s @ stn=${row.stn} tm=${row.tm}`);
  } catch (e: any) {
    console.error(e?.stack || e?.message || String(e));
    try {
      await writeLP([`api_probe,service=asos_feels,env=prod,loc=${process.env.LOC || "seoul"} success=0i ${nowNs}`]);
    } catch { /* ignore */ }
    process.exit(1);
  }
})();

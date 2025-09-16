/**
 * KMAHub ASOS → Apparent Temperature(체감온도) 계산 → InfluxDB 적재
 *
 * - AT(체감온도) = T + 0.33*e − 0.70*ws − 4.00
 *   where e[hPa] = (RH/100) * 6.105 * exp(17.27*T / (237.7 + T))
 * - RH가 없으면 TD(이슬점)로 RH 역산(마그누스 공식)
 * - 신선도 가드: 관측시각이 now(KST)-3h 이전이면 write 스킵(단, api_probe는 남김)
 *
 * 실행: npx ts-node scripts/asos_feels_to_influx.ts
 * ENV:  INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET (필수)
 *       APIHUB_BASE=https://apihub.kma.go.kr (필수)
 *       APIHUB_KEY=xxxxx                      (필수)
 *       ASOS_STN=108, LOC=seoul               (선택, 기본값 있음)
 *       ASOS_API_PATH=api/typ01/url/kma_sfctm3.php (선택)
 *       ───────── 파싱 인덱스(1-based, 없으면 자동탐색 시도) ─────────
 *       ASOS_COL_TM=1, ASOS_COL_STN=2, ASOS_COL_TA=8, ASOS_COL_HM=7,
 *       ASOS_COL_WS=4, ASOS_COL_TD=9
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

// ---------- Time helpers (KST 안전 처리) ----------
const KST_OFFSET_MIN = 9 * 60;

const pad = (n: number, w = 2) => n.toString().padStart(w, "0");

// KST 기준으로 분 단위 내림(기본 10분 격자)
function floorToMinuteStepKST(dUtc: Date, stepMin = 10): Date {
  const kstMs = dUtc.getTime() + KST_OFFSET_MIN * 60 * 1000;
  const kst = new Date(kstMs);
  const m = kst.getUTCMinutes();
  kst.setUTCMinutes(Math.floor(m / stepMin) * stepMin, 0, 0);
  return new Date(kst.getTime() - KST_OFFSET_MIN * 60 * 1000);
}

// KST 기준 조회 윈도우 계산
function makeWindow(nowUtc = new Date()) {
  const tm2Utc = floorToMinuteStepKST(new Date(nowUtc.getTime() - 10 * 60 * 1000), 10); // now-10m
  const tm1Utc = new Date(tm2Utc.getTime() - 90 * 60 * 1000); // -90m
  return { tm1: fmtYYYYMMDDHHmm(tm1Utc), tm2: fmtYYYYMMDDHHmm(tm2Utc) };
}

// UTC Date → "YYYYMMDDHHmm" (KST 로컬시각 기준으로 포맷)
function fmtYYYYMMDDHHmm(dUtc: Date): string {
  // dUtc를 KST로 변환 후 포맷
  const kst = new Date(dUtc.getTime() + KST_OFFSET_MIN * 60 * 1000);
  const y = kst.getUTCFullYear();
  const M = kst.getUTCMonth() + 1;
  const D = kst.getUTCDate();
  const h = kst.getUTCHours();
  const m = kst.getUTCMinutes();
  return `${y}${pad(M)}${pad(D)}${pad(h)}${pad(m)}`;
}

// "YYYYMMDDHHmm"(KST) → epoch ns
function kstTmToEpochNs(tm: string): bigint {
  const y = Number(tm.slice(0, 4));
  const M = Number(tm.slice(4, 6)) - 1;
  const D = Number(tm.slice(6, 8));
  const h = Number(tm.slice(8, 10));
  const m = Number(tm.slice(10, 12));
  // KST 시각을 UTC로 바꿔 epoch: Date.UTC(y,M,D,h-9,m)
  const ms = Date.UTC(y, M, D, h - 9, m, 0, 0);
  return BigInt(ms) * 1_000_000n;
}

// ---------- Math for humidity/feels ----------
function clamp(v: number, min: number, max: number) {
  return Math.max(min, Math.min(max, v));
}

// Magnus formula: RH from T, Td (섭씨)
function rhFromTd(T: number, Td: number): number {
  const a = 17.27;
  const b = 237.7;
  const es = 6.105 * Math.exp((a * T) / (b + T));
  const e = 6.105 * Math.exp((a * Td) / (b + Td));
  return clamp((e / es) * 100, 1, 100);
}

function feelsAT(T: number, RH: number, ws: number): number {
  const e = (RH / 100) * 6.105 * Math.exp((17.27 * T) / (237.7 + T));
  return T + 0.33 * e - 0.7 * ws - 4.0;
}

// ---------- HTTP helpers ----------
async function httpGetArrayBuffer(url: string): Promise<ArrayBuffer> {
  const t0 = Date.now();
  const res = await (globalThis as any).fetch(url);
  const buf = await res.arrayBuffer();
  (res as any).__latencyMs = Date.now() - t0;
  return Object.assign(buf, { __latencyMs: (res as any).__latencyMs });
}

async function writeLP(lines: string[]): Promise<void> {
  const url = `${reqEnv("INFLUX_URL").replace(/\/+$/, "")}/api/v2/write?org=${encodeURIComponent(
    reqEnv("INFLUX_ORG")
  )}&bucket=${encodeURIComponent(reqEnv("INFLUX_BUCKET"))}&precision=ns`;
  const body = lines.join("\n");
  const t0 = Date.now();
  const res = await (globalThis as any).fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "text/plain; charset=utf-8",
      Authorization: `Token ${reqEnv("INFLUX_TOKEN")}`,
    },
    body,
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Influx write failed: ${res.status} ${res.statusText} - ${text}`);
  }
  const latency = Date.now() - t0;
  console.log(`Influx write ${lines.length} line(s) OK (${latency}ms)`);
}

// ---------- Parsing ASOS text ----------
type AsosRow = {
  tm: string; // YYYYMMDDHHmm (KST)
  stn: string;
  ta?: number; // temp C
  hm?: number; // RH %
  ws?: number; // wind m/s
  td?: number; // dew point C
};

// 토큰 분리: 공백 or 콤마
function splitTokens(line: string): string[] {
  const byComma = line.split(",");
  if (byComma.length > 1) return byComma.map((s) => s.trim()).filter(Boolean);
  return line.split(/\s+/).map((s) => s.trim()).filter(Boolean);
}

// 인덱스 기반 추출(1-based). 실패 시 undefined
function pickNum(tokens: string[], idx?: number): number | undefined {
  if (!idx) return undefined;
  const i = idx - 1;
  if (i < 0 || i >= tokens.length) return undefined;
  const v = Number(tokens[i]);
  return Number.isFinite(v) ? v : undefined;
}

function parseAsosLinesToLatest(
  lines: string[],
  col: { tm?: number; stn?: number; ta?: number; hm?: number; ws?: number; td?: number }
): AsosRow | null {
  const clean = lines
    .map((s) => s.trim())
    .filter((s) => s && !s.startsWith("#") && /\d/.test(s)); // 주석/빈줄 제거

  if (clean.length === 0) return null;

  // 최신행 = 마지막 라인
  const last = clean[clean.length - 1];
  const tokens = splitTokens(last);

  // 1) 인덱스 기반 시도
  let tm = col.tm ? splitTokens(clean[clean.length - 1])[col.tm - 1] : undefined;
  let stn = col.stn ? splitTokens(clean[clean.length - 1])[col.stn - 1] : undefined;
  let ta = pickNum(tokens, col.ta);
  let hm = pickNum(tokens, col.hm);
  let ws = pickNum(tokens, col.ws);
  let td = pickNum(tokens, col.td);

  // 2) 자동 탐색(필요시)
  // tm 자동: 12자리 숫자 시각 토큰
  if (!tm) {
    tm = tokens.find((t) => /^\d{12}$/.test(t));
  }
  // stn 자동: 3~5자리 정수(예: 108)
  if (!stn) {
    stn = tokens.find((t) => /^\d{3,5}$/.test(t));
  }

  // ws/ta/hm/td 자동 후보(보수적 범위)
  const nums = tokens.map((t) => Number(t)).filter((v) => Number.isFinite(v));
  if (ta == null) ta = nums.find((v) => v >= -60 && v <= 60);
  if (hm == null) hm = nums.find((v) => v >= 0 && v <= 100);
  if (ws == null) ws = nums.find((v) => v >= 0 && v <= 70);
  if (td == null) td = nums.find((v) => v >= -60 && v <= 40);

  if (!tm || !stn) return null;
  return { tm, stn, ta, hm, ws, td };
}

// ---------- ASOS fetch & retry ----------
async function fetchLatestASOS(stn: string, base: string, path: string) {
  let win = makeWindow();
  const tried: string[] = [];
  for (let i = 0; i < 3; i++) {
    const url =
      `${base.replace(/\/+$/, "")}/${path.replace(/^\/+/, "")}` +
      `?stn=${encodeURIComponent(stn)}&tm1=${win.tm1}&tm2=${win.tm2}&help=0&authKey=${encodeURIComponent(
        reqEnv("APIHUB_KEY")
      )}`;

    const buf: any = await httpGetArrayBuffer(url);
    const latency = buf.__latencyMs ?? 0;
    const raw = iconv.decode(Buffer.from(buf), "euc-kr");
    const lines = raw.split(/\r?\n/);

    // 디버깅 로그(민감정보 제외)
    console.log(
      `[ASOS] url=${base}/.../${path} stn=${stn} tm1=${win.tm1} tm2=${win.tm2} lines=${lines.length} latency=${latency}ms`
    );

    const col = {
      tm: optInt("ASOS_COL_TM", 1),
      stn: optInt("ASOS_COL_STN", 2),
      ta: optInt("ASOS_COL_TA", 8),
      hm: optInt("ASOS_COL_HM", 7),
      ws: optInt("ASOS_COL_WS", 4),
      td: optInt("ASOS_COL_TD", 9),
    };

    const row = parseAsosLinesToLatest(lines, col);
    if (row) {
      return { row, latency };
    }

    tried.push(`${win.tm1}-${win.tm2}`);
    // 한 시간 더 과거로 백오프
    const tm2Utc = kstTmToUtc(win.tm2);
    const older = new Date(Number(tm2Utc) - 60 * 60 * 1000);
    win = makeWindow(older);
  }

  const triedStr = tried.join(", ");
  throw new Error(`ASOS: no data rows (tried windows=${triedStr})`);
}

// "YYYYMMDDHHmm"(KST) → UTC Date
function kstTmToUtc(tm: string): number {
  const y = Number(tm.slice(0, 4));
  const M = Number(tm.slice(4, 6)) - 1;
  const D = Number(tm.slice(6, 8));
  const h = Number(tm.slice(8, 10));
  const m = Number(tm.slice(10, 12));
  return Date.UTC(y, M, D, h - 9, m);
}

// ---------- Main ----------
(async () => {
  const INFLUX_URL = reqEnv("INFLUX_URL");
  const INFLUX_TOKEN = reqEnv("INFLUX_TOKEN");
  const INFLUX_ORG = reqEnv("INFLUX_ORG");
  const INFLUX_BUCKET = reqEnv("INFLUX_BUCKET");
  void INFLUX_URL; void INFLUX_TOKEN; void INFLUX_ORG; void INFLUX_BUCKET; // lints

  const APIHUB_BASE = reqEnv("APIHUB_BASE");
  const ASOS_STN = process.env.ASOS_STN || "108";
  const LOC = process.env.LOC || "seoul";
  const ASOS_API_PATH = process.env.ASOS_API_PATH || "api/typ01/url/kma_sfctm3.php";

  const t0 = Date.now();

  try {
    const { row, latency } = await fetchLatestASOS(ASOS_STN, APIHUB_BASE, ASOS_API_PATH);

    if (!row.tm || !row.stn) {
      throw new Error("ASOS: parsed row missing tm/stn");
    }

    // 관측값 보정/계산
    const T = row.ta ?? NaN;
    const WS = clamp(row.ws ?? 0, 0, 70);
    let RH = row.hm ?? NaN;

    if (!Number.isFinite(RH) && Number.isFinite(row.td) && Number.isFinite(T)) {
      RH = rhFromTd(T, row.td!);
      console.warn(`RH missing → estimated from Td: RH≈${RH.toFixed(1)}%`);
    }
    RH = clamp(RH, 1, 100);

    if (!Number.isFinite(T) || !Number.isFinite(RH)) {
      throw new Error(`ASOS: insufficient fields (T=${T}, RH=${RH}, WS=${WS})`);
    }

    const feels = feelsAT(T, RH, WS);

    // 신선도 가드(now-3h)
    const tsNs = kstTmToEpochNs(row.tm);
    const nowNs = BigInt(Date.now()) * 1_000_000n;
    const threeHNs = 3n * 60n * 60n * 1_000_000_000n;
    if (nowNs - tsNs > threeHNs) {
      console.warn(`Skip write: stale obs ts=${row.tm}`);
      await writeLP([
        `api_probe,service=asos_feels,env=prod,loc=${LOC} success=0i,latency_ms=${Date.now() - t0}i ${nowNs}`,
      ]);
      return;
    }

    const lp = [
      `life_index,source=kmahub-asos,loc=${LOC},stn=${row.stn} ` +
        `feels_c=${feels.toFixed(2)},temp_c=${T},rh_pct=${RH},wind_ms=${WS} ${tsNs}`,
      `api_probe,service=asos_feels,env=prod,loc=${LOC} success=1i,latency_ms=${latency}i ${nowNs}`,
    ];

    await writeLP(lp);
    console.log(
      `Feels(AT)=${feels.toFixed(2)}C | Temp=${T}C | RH=${RH}% | Wind=${WS}m/s @ stn=${row.stn}, tm=${row.tm}`
    );
  } catch (e: any) {
    const nowNs = BigInt(Date.now()) * 1_000_000n;
    console.error(e?.stack || e?.message || String(e));
    try {
      await writeLP([
        `api_probe,service=asos_feels,env=prod,loc=${process.env.LOC || "seoul"} success=0i ${nowNs}`,
      ]);
    } catch {
      /* ignore probe failure to avoid masking the root error */
    }
    process.exit(1);
  }
})();

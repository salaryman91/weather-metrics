// scripts/uv_to_influx.ts
/**
 * KMAHub 자외선(kma_sfctm_uv.php) → InfluxDB
 * - KST 기준 최근 180분, 10분 스냅(tm) 다중 시도 + 지점 stn→원하는 지점→0(전체) 폴백
 * - help/disp 혼용(EUC-KR/UTF-8) 응답 파싱, 테이블 보더/CSV/WS 자동 인지
 * - UVI 추출 우선순위: UV-B(index) > EUV/25 > EUV*40 > 휴리스틱
 * - 적재: life_index (uv_idx) + method 태그(uvb|euv25|euv40|heur) + base_time_s
 * - SLI/QA: api_probe(service=uv_obs) success/latency/age_s/ver/note
 *
 * 실행:
 *   npx ts-node scripts/uv_to_influx.ts
 */

import "dotenv/config";
import * as iconv from "iconv-lite";

// ===== env & const =====
type Env = {
  INFLUX_URL: string; INFLUX_TOKEN: string; INFLUX_ORG: string; INFLUX_BUCKET: string;
  APIHUB_BASE: string; APIHUB_KEY: string;
  UV_STN?: string; LOC?: string;
};
const env = process.env as unknown as Env;
const need = (k: keyof Env) => { const v = env[k]; if (!v) throw new Error(`[FATAL] Missing env: ${k}`); return v; };
const DBG = !!(process.env.DEBUG_UV || process.env.DEBUG);
const SVC_VER = "uv/2025-09-16.2";
const KST_OFF = 9 * 3600 * 1000;

const esc = (s: string) => String(s).replace(/\\/g, "\\\\").replace(/"/g, '\\"');
const nowSec = () => Math.floor(Date.now() / 1000);

// ===== tiny utils =====
const pad2 = (n: number) => String(n).padStart(2, "0");
const toEpochSec = (iso: string) => Math.floor(new Date(iso).getTime()/1000);
const toIsoKst = (yyyymmdd: string, hhmm: string) => {
  const yyyy = yyyymmdd.slice(0,4), MM = yyyymmdd.slice(4,6), dd = yyyymmdd.slice(6,8);
  const HH = hhmm.slice(0,2), mm = hhmm.slice(2,4);
  return `${yyyy}-${MM}-${dd}T${HH}:${mm}:00+09:00`;
};

async function decodeKR(res: Response): Promise<string> {
  const ab = await res.arrayBuffer(); const buf = Buffer.from(ab);
  const ct = (res.headers.get("content-type") || "").toLowerCase();
  if (/euc-?kr|ks_c_5601|cp949/.test(ct)) return iconv.decode(buf, "euc-kr");
  if (/utf-?8/.test(ct)) return buf.toString("utf8");
  const utf = buf.toString("utf8");
  return utf.includes("\uFFFD") ? iconv.decode(buf, "euc-kr") : utf;
}

function splitCSVLine(line: string): string[] {
  const out: string[] = []; let cur = ""; let q = false;
  for (let i=0;i<line.length;i++){
    const c=line[i];
    if (c === '"') { if (q && line[i+1] === '"') { cur+='"'; i++; } else { q = !q; } }
    else if (c === "," && !q) { out.push(cur); cur=""; }
    else cur += c;
  }
  out.push(cur);
  return out.map(s => s.trim());
}
const toLines = (t: string) =>
  t.replace(/\ufeff/g,"").split(/\r?\n/).map(s => s.replace(/\s+$/,"")).filter(l => l.trim().length>0);

function stripTableDecor(lines: string[]): string[] {
  const border = /^[\s|│┃┆┊\-─━┈┉┄┅=+]+$/;
  return lines.filter(l => !border.test(l))
              .map(l => l.replace(/[│┃┆┊]/g," ").replace(/\s*\|\s*/g," "));
}
function splitBy(line: string, mode: "csv" | "ws") {
  const s = line.replace(/^#\s*/, "").trim();
  return mode === "csv" ? splitCSVLine(s) : s.split(/\s+/);
}
const toNum = (s?: string) => {
  if (s==null) return NaN;
  const n = parseFloat(s.replace(/[|│┃┆┊,]/g,""));
  return !isFinite(n) || n <= -8.9 ? NaN : n;
};
const looksStation = (v:number)=> Number.isInteger(v) && v>=1 && v<10000;

// ===== time candidates (KST, 10-min snap) =====
function tmCandidates(minutesBack = 180): string[] {
  const out: string[] = [];
  const kst = new Date(Date.now() + KST_OFF);
  kst.setUTCSeconds(0,0);
  kst.setUTCMinutes(kst.getUTCMinutes() - (kst.getUTCMinutes() % 10)); // floor to 10-min
  const fmt = (d: Date) =>
    `${d.getUTCFullYear()}${pad2(d.getUTCMonth()+1)}${pad2(d.getUTCDate())}${pad2(d.getUTCHours())}${pad2(d.getUTCMinutes())}`;
  for (let m=0; m<=minutesBack; m+=10) {
    const d = new Date(kst.getTime() - m*60_000);
    out.push(fmt(d));
  }
  return Array.from(new Set(out));
}

// ===== Influx =====
async function writeLP(lines: string[]) {
  if (!lines.length) return;
  const url = `${need("INFLUX_URL")}/api/v2/write?org=${encodeURIComponent(need("INFLUX_ORG"))}&bucket=${encodeURIComponent(need("INFLUX_BUCKET"))}&precision=s`;
  const res = await fetch(url, { method:"POST",
    headers:{ "Authorization":`Token ${need("INFLUX_TOKEN")}`, "Content-Type":"text/plain; charset=utf-8" },
    body: lines.join("\n")
  });
  if (!res.ok) throw new Error(`Influx write ${res.status}: ${await res.text().catch(()=>"...")}`);
}

// ===== header/time/station parsing =====
function splitHeaderData(all: string[]) {
  const comments = all.filter(l => l.trim().startsWith("#"));
  const rest = all.filter(l => !l.trim().startsWith("#"));
  return { comments, rest };
}
function headerTokens(comments: string[], rowLen: number): string[] | null {
  for (const line of comments) {
    const s = line.replace(/^#\s*/, "").trim();
    if (!/(STN|지점)/i.test(s)) continue;
    if (!/(UVB|UV\-B|UVA|EUV|YYMM|TM|TIME|DATE)/i.test(s)) continue;
    const toks = s.split(/\s+/);
    if (toks.length === rowLen) return toks;
  }
  return null;
}
function pickTimeIndex(rows: string[][]): { iTM: number; pair?: [number, number] } {
  if (!rows.length) return { iTM:-1 };
  let iTM = rows[0].findIndex(v => /^\d{12,14}$/.test(v));
  if (iTM >= 0) return { iTM };
  const cols = rows[0].length;
  for (let c=0;c<cols-1;c++) {
    const a=rows[0][c], b=rows[0][c+1];
    if (/^\d{8}$/.test(a) && /^\d{4}$/.test(b)) return { iTM:-1, pair:[c,c+1] };
  }
  return { iTM:-1 };
}
function parseTsKST(raw: string): number | null {
  if (/^\d{12,14}$/.test(raw)) {
    const yyyy=raw.slice(0,4), MM=raw.slice(4,6), dd=raw.slice(6,8), HH=raw.slice(8,10), mm=raw.slice(10,12), ss=(raw.slice(12,14)||"00");
    const d = new Date(`${yyyy}-${MM}-${dd}T${HH}:${mm}:${ss}+09:00`);
    return isNaN(d.getTime()) ? null : Math.floor(d.getTime()/1000);
  }
  const d = new Date(raw.replace(" ", "T") + (/\+/.test(raw) ? "" : "+09:00"));
  return isNaN(d.getTime()) ? null : Math.floor(d.getTime()/1000);
}
function pickStationIndex(rows: string[][]): number {
  if (!rows.length) return -1;
  const cols = rows[0].length;
  let best=-1, score=-1;
  for (let c=0;c<cols;c++){
    let ok=0, tot=0;
    for (const r of rows.slice(-60)) { const v=toNum(r[c]); if (!isFinite(v)) continue; tot++; if (looksStation(v)) ok++; }
    const fit = tot? ok/tot : 0;
    if (fit > score) { score = fit; best = c; }
  }
  return best;
}

// ===== fetch & parse main =====
type Tx = "id" | "div25" | "mul40";
type Used = "uvb" | "euv25" | "euv40" | "heur";
type Found = { uv: number; used: Used; col: number; tx: Tx };

async function fetchLatestUV(stnWanted: string) {
  const base = need("APIHUB_BASE").replace(/\/+$/,"");
  const key = need("APIHUB_KEY");
  const tms = tmCandidates(180);
  const stnList = [stnWanted, "0"];

  let lastErr: Error | null = null;

  for (const stn of stnList) for (const tm of tms) {
    const urlPaths = [
      `${base}/api/typ02/url/kma_sfctm_uv.php?stn=${stn}&tm=${tm}&disp=0&help=0&authKey=${key}`,
      `${base}/api/typ02/url/kma_sfctm_uv.php?stn=${stn}&tm=${tm}&disp=1&help=1&authKey=${key}`,
      `${base}/api/typ01/url/kma_sfctm_uv.php?stn=${stn}&tm=${tm}&disp=0&help=0&authKey=${key}`,
      `${base}/api/typ01/url/kma_sfctm_uv.php?stn=${stn}&tm=${tm}&disp=1&help=1&authKey=${key}`,
    ];
    for (const url of urlPaths) {
      const t0 = Date.now(); const res = await fetch(url); const latency = Date.now() - t0;
      const text = await decodeKR(res);
      if (!res.ok) { lastErr = new Error(`HTTP ${res.status}: ${text.slice(0,160)}`); continue; }

      if (DBG) { console.log("UV url:", url.replace(key,"***")); console.log(text.split(/\r?\n/).slice(0,6)); }

      const rawLines = toLines(text);
      const { comments, rest } = splitHeaderData(rawLines);
      const cleaned = stripTableDecor(rest);
      const mode: "csv" | "ws" = cleaned.some(l => l.includes(",")) ? "csv" : "ws";
      const rowsAll = cleaned.map(l => splitBy(l, mode));
      const rows = rowsAll.filter(r => r.some(x => isFinite(toNum(x))));
      if (!rows.length) { lastErr = new Error("UV: no usable rows"); continue; }

      const toks = headerTokens(comments, rows[0].length);
      let idxEUV=-1, idxUVB_energy=-1, idxUVA_energy=-1, idxUVB_index=-1, idxUVA_index=-1, idxTM=-1, idxSTN=-1;
      if (toks) {
        const find = (re:RegExp)=> toks.findIndex(t=> re.test(t));
        idxEUV        = find(/^EUV$/i);
        idxUVB_energy = find(/^UVB$/i);
        idxUVA_energy = find(/^UVA$/i);
        idxUVB_index  = find(/^UV\-?B$/i);
        idxUVA_index  = find(/^UV\-?A$/i);
        idxTM         = find(/^(YYMM|YYMMDDHHMI|TM|TIME|DATE)$/i);
        idxSTN        = find(/^(STN|STATION|지점)$/i);
      }

      let iTime = idxTM >= 0 ? idxTM : -1; let pairTime: [number, number] | undefined;
      if (iTime < 0) { const pick = pickTimeIndex(rows); iTime = pick.iTM; pairTime = pick.pair; }
      let iStn = idxSTN >= 0 ? idxSTN : -1; if (iStn < 0 && stn === "0") iStn = pickStationIndex(rows);

      const apply = (x:number,t:Tx)=> t==="id"?x:(t==="div25"?x/25:x*40);

      const tryGetUVI = (r: string[]): Found | null => {
        // 지점 필터(전체표에선 원하는 지점만 통과)
        if (stn === "0" && stnWanted && iStn >= 0) {
          const sv=(r[iStn]??"").trim(); if (sv && sv !== stnWanted) return null;
        }
        // 1) UV-B index
        if (idxUVB_index >= 0) {
          const v=toNum(r[idxUVB_index]); if (isFinite(v) && v>=0 && v<=20) return { uv:v, used:"uvb", col:idxUVB_index, tx:"id" };
        }
        // 2) EUV conversions
        if (idxEUV >= 0) {
          const raw=toNum(r[idxEUV]);
          if (isFinite(raw) && raw>=0) {
            const u1=apply(raw,"div25"); if (isFinite(u1) && u1>=0 && u1<=20) return { uv:u1, used:"euv25", col:idxEUV, tx:"div25" };
            const u2=apply(raw,"mul40"); if (isFinite(u2) && u2>=0 && u2<=20) return { uv:u2, used:"euv40", col:idxEUV, tx:"mul40" };
          }
        }
        // 3) heuristic: 다른 숫자 컬럼을 변환해 0..20에 들어오면 채택
        for (let c=0;c<r.length;c++){
          if (c===iTime || c===iStn) continue;
          const raw=toNum(r[c]); if (!isFinite(raw) || looksStation(raw)) continue;
          for (const tx of ["id","div25","mul40"] as Tx[]) {
            const u=apply(raw,tx); if (isFinite(u) && u>=0 && u<=20) return { uv:u, used:"heur", col:c, tx };
          }
        }
        return null;
      };

      for (let k=rows.length-1;k>=0;k--){
        const r = rows[k]; const found = tryGetUVI(r); if (!found) continue;
        let ts = nowSec();
        if (iTime >= 0) { const t = parseTsKST(r[iTime]); if (t) ts = t; }
        else if (pairTime) { const t = parseTsKST(`${r[pairTime[0]]}${r[pairTime[1]]}`); if (t) ts = t; }

        // clamp & normalize
        const uv = Math.max(0, Math.min(20, Number(found.uv.toFixed(1))));
        return { uv, used: found.used, ts, latency };
      }
      lastErr = new Error("UV column not found (no valid row after filtering)");
    }
  }
  throw lastErr ?? new Error("UV column not found");
}

// ===== main =====
(async () => {
  const stn = (env.UV_STN || "").trim();
  if (!stn) throw new Error("UV_STN missing");
  const loc = (env.LOC || "seoul").trim();

  const now = nowSec();
  try {
    const { uv, used, ts, latency } = await fetchLatestUV(stn);
    const age = Math.max(0, now - ts);
    const lines = [
      // life_index 포인트: 측정 시각(ts)을 포인트 타임스탬프로 사용, base_time_s로 별도 보존
      `life_index,source=kmahub-uv,loc=${loc},stn=${stn},method=${used} uv_idx=${uv},base_time_s=${ts}i ${ts}`,
      // SLI/QA 프로브
      `api_probe,service=uv_obs,env=prod,loc=${loc} success=1i,latency_ms=${latency||0}i,age_s=${age}i,ver="${SVC_VER}" ${now}`,
    ];
    if (DBG) console.log("[SAMPLE]\n" + lines.join("\n"));
    await writeLP(lines);
    console.log(`[OK] UV=${uv} (${used}) stn=${stn} age=${age}s wrote=${lines.length}`);
  } catch (e:any) {
    const note = esc(String(e?.message || "err").slice(0,200));
    const probe = `api_probe,service=uv_obs,env=prod,loc=${env.LOC||"seoul"} success=0i,latency_ms=0i,ver="${SVC_VER}",note="${note}" ${now}`;
    try { await writeLP([probe]); } catch {}
    console.error("[FAIL]", e?.message || e);
    process.exitCode = 0; // 운영 관측성 유지
  }
})();

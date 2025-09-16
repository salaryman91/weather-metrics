/**
 * KMAHub 자외선(kma_sfctm_uv.php) → InfluxDB Cloud
 * - 최근 3시간(KST) 10분 간격 후보 tm 조회, stn→0(전체) fallback
 * - help/disp 혼용 응답(EUC-KR/UTF-8) 파싱
 * - 우선순위: UV-B(지수) > EUV÷25 > EUV×40 > 휴리스틱
 * - 적재: life_index (uv_idx) + method 태그 + base_time_s
 * - 프로브: api_probe(service=uv_obs)
 *
 * 실행:
 *   npx dotenv -e .env -- ts-node scripts/uv_to_influx.ts
 */

import * as iconv from "iconv-lite";

// ===== env =====
type Env = {
  INFLUX_URL: string; INFLUX_TOKEN: string; INFLUX_ORG: string; INFLUX_BUCKET: string;
  APIHUB_BASE: string; APIHUB_KEY: string; UV_STN?: string; LOC?: string;
};
const env = process.env as unknown as Env;
const need = (k: keyof Env) => { const v = env[k]; if (!v) throw new Error(`Missing env: ${k}`); return v; };
const DBG = !!process.env.DEBUG_UV;

// ===== small utils =====
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
function splitBy(line: string, mode: "csv" | "ws") {
  const s = line.replace(/^#\s*/, "").trim();
  return mode === "csv" ? splitCSVLine(s) : s.split(/\s+/);
}
const toNum = (s?: string) => { if (s==null) return NaN; const n = parseFloat(s.replace(/[|│┃┆┊,]/g,"")); return !isFinite(n)||n<=-8.9?NaN:n; };
const looksStationCode = (v:number)=> Number.isInteger(v) && v>=1 && v<10000;

async function decodeKR(res: Response): Promise<string> {
  const ab = await res.arrayBuffer(); const buf = Buffer.from(ab);
  const ct = (res.headers.get("content-type") || "").toLowerCase();
  if (/euc-?kr|ks_c_5601|cp949/.test(ct)) return iconv.decode(buf,"euc-kr");
  if (/utf-?8/.test(ct)) return buf.toString("utf8");
  const utf = buf.toString("utf8"); if (utf.includes("\uFFFD")) return iconv.decode(buf,"euc-kr");
  return utf;
}

// ===== influx =====
async function writeLP(lines: string[]) {
  const url = `${need("INFLUX_URL")}/api/v2/write?org=${encodeURIComponent(need("INFLUX_ORG"))}&bucket=${encodeURIComponent(need("INFLUX_BUCKET"))}&precision=s`;
  const res = await fetch(url, {
    method:"POST",
    headers:{ Authorization:`Token ${need("INFLUX_TOKEN")}`, "Content-Type":"text/plain; charset=utf-8" },
    body: lines.join("\n")
  });
  if (!res.ok) throw new Error(`Influx write ${res.status}: ${await res.text().catch(()=>"...")}`);
}

// ===== header/time/station helpers =====
function stripTableDecor(lines: string[]): string[] {
  const border = /^[\s|│┃┆┊\-─━┈┉┄┅=+]+$/;
  return lines.filter(l => !border.test(l))
              .map(l => l.replace(/[│┃┆┊]/g," ").replace(/\s*\|\s*/g," "));
}
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
    const yyyy=raw.slice(0,4), MM=raw.slice(4,6), dd=raw.slice(6,8), HH=raw.slice(8,10), mm=raw.slice(10,12), ss=(raw.slice(12,14) || "00");
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
    for (const r of rows.slice(-60)) { const v=toNum(r[c]); if (!isFinite(v)) continue; tot++; if (looksStationCode(v)) ok++; }
    const fit = tot? ok/tot : 0;
    if (fit > score) { score = fit; best = c; }
  }
  return best;
}
function tmCandidates(minutesBack = 180): string[] {
  const out: string[] = [];
  const kstNowMs = Date.now() + 9*3600_000;
  const base = new Date(kstNowMs); base.setSeconds(0,0);
  const mm = base.getUTCMinutes(); base.setUTCMinutes(mm - (mm%10));
  const fmt = (msKST:number)=> {
    const d=new Date(msKST);
    const yyyy=d.getUTCFullYear(), MM=d.getUTCMonth()+1, dd=d.getUTCDate(), HH=d.getUTCHours(), m=d.getUTCMinutes();
    const p=(n:number)=>String(n).padStart(2,"0");
    return `${yyyy}${p(MM)}${p(dd)}${p(HH)}${p(m)}`;
  };
  for (let m=0; m<=minutesBack; m+=10) out.push(fmt(base.getTime() - m*60_000));
  return Array.from(new Set(out));
}

// ===== fetch & parse =====
type Tx = "id" | "div25" | "mul40";
type Used = "uvb" | "euv25" | "euv40" | "heur";
type Found = { uv: number; used: Used; col: number; tx: Tx };

async function fetchLatestUV(stnWanted: string) {
  const base = need("APIHUB_BASE"); const key = need("APIHUB_KEY");
  const stnList = [stnWanted, "0"];
  const tms = tmCandidates(180);

  let lastErr: Error | null = null;

  for (const stn of stnList) for (const tm of tms) {
    const urls = [
      `${base}/api/typ01/url/kma_sfctm_uv.php?stn=${stn}&tm=${tm}&disp=0&help=0&authKey=${key}`,
      `${base}/api/typ01/url/kma_sfctm_uv.php?stn=${stn}&tm=${tm}&disp=1&help=1&authKey=${key}`,
    ];
    for (const url of urls) {
      const t0 = Date.now(); const res = await fetch(url); const latency = Date.now() - t0;
      const text = await decodeKR(res);
      if (!res.ok) { lastErr = new Error(`HTTP ${res.status}: ${text.slice(0,160)}`); continue; }

      if (DBG) { console.log("UV url:", url.replace(key,"***")); console.log(text.split(/\r?\n/).slice(0,10)); }

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
        idxUVB_index  = find(/^UV\-B$/i);
        idxUVA_index  = find(/^UV\-A$/i);
        idxTM         = find(/^(YYMM|YYMMDDHHMI|TM|TIME|DATE)$/i);
        idxSTN        = find(/^(STN|STATION|지점)$/i);
      }

      let iTime = idxTM >= 0 ? idxTM : -1; let pairTime: [number, number] | undefined;
      if (iTime < 0) { const pick = pickTimeIndex(rows); iTime = pick.iTM; pairTime = pick.pair; }
      let iStn = idxSTN >= 0 ? idxSTN : -1; if (iStn < 0 && stn === "0") iStn = pickStationIndex(rows);

      const apply = (x:number,t:Tx)=> t==="id"?x:(t==="div25"?x/25:x*40);

      const tryGetUVI = (r: string[]): Found | null => {
        if (stn === "0" && stnWanted && iStn >= 0) { const sv=(r[iStn]??"").trim(); if (sv && sv !== stnWanted) return null; }
        // 1) UV-B index
        if (idxUVB_index >= 0) { const v=toNum(r[idxUVB_index]); if (isFinite(v) && v>=0 && v<=20) return { uv:v, used:"uvb", col:idxUVB_index, tx:"id" }; }
        // 2) EUV conversions
        if (idxEUV >= 0) {
          const raw=toNum(r[idxEUV]);
          if (isFinite(raw) && raw>=0) {
            const u1=apply(raw,"div25"); if (isFinite(u1) && u1>=0 && u1<=20) return { uv:u1, used:"euv25", col:idxEUV, tx:"div25" };
            const u2=apply(raw,"mul40"); if (isFinite(u2) && u2>=0 && u2<=20) return { uv:u2, used:"euv40", col:idxEUV, tx:"mul40" };
          }
        }
        // 3) heuristic
        const cols=r.length;
        for (let c=0;c<cols;c++){
          if (c===iTime || c===iStn) continue;
          const raw=toNum(r[c]); if (!isFinite(raw) || looksStationCode(raw)) continue;
          for (const tx of ["id","div25","mul40"] as Tx[]) {
            const u=apply(raw,tx); if (isFinite(u) && u>=0 && u<=20) return { uv:u, used:"heur", col:c, tx };
          }
        }
        return null;
      };

      for (let k=rows.length-1;k>=0;k--){
        const r = rows[k]; const found = tryGetUVI(r); if (!found) continue;
        let ts = Math.floor(Date.now()/1000);
        if (iTime >= 0) { const t = parseTsKST(r[iTime]); if (t) ts = t; }
        else if (pairTime) { const t = parseTsKST(`${r[pairTime[0]]}${r[pairTime[1]]}`); if (t) ts = t; }

        if (DBG) console.log({ tm, stn, used: found.used, uv: found.uv, ts });
        return { uv: found.uv, used: found.used, ts, latency };
      }

      lastErr = new Error("UV column not found (no valid row after filtering)");
    }
  }
  throw lastErr ?? new Error("UV column not found");
}

// ===== main =====
(async () => {
  const stn = (env.UV_STN || "").trim();
  if (!stn) throw new Error("UV_STN 미설정");
  const loc = (env.LOC || "seoul").trim();

  try {
    const t0 = Date.now();
    const { uv, used, ts, latency } = await fetchLatestUV(stn);
    const now = Math.floor(Date.now()/1000);
    const lines = [
      // method 태그 + base_time_s 필드 추가 (신선도/위험구간 계산에 도움)
      `life_index,source=kmahub-uv,loc=${loc},stn=${stn},method=${used} uv_idx=${uv},base_time_s=${ts}i ${ts}`,
      `api_probe,service=uv_obs,env=prod,loc=${loc} success=1i,latency_ms=${latency}i ${now}`
    ];
    await writeLP(lines);
    console.log(`UV=${uv} (${used}) @ stn=${stn}  wrote=${lines.length}`);
  } catch (e:any) {
    const now = Math.floor(Date.now()/1000);
    try {
      await writeLP([`api_probe,service=uv_obs,env=prod,loc=${env.LOC||"seoul"} success=0i,latency_ms=0i ${now}`]);
    } catch {}
    console.error(e?.message || e);
    process.exit(0);
  }
})();
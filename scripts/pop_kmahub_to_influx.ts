import "dotenv/config";

/**
 * KMA POP/Rainfall → Influx (forecast + nowcast)
 * - Forecast hourly rainfall (PCP, mm) & precip probability (POP, %) via getVilageFcst
 * - Current 1-hour rainfall (RN1, mm) via getUltraSrtNcst
 *
 * Measurement/fields (precision=s):
 *   forecast,source=kma-vilage pcp_mm=<float>,pop_pct=<int>,base_time_s=<int>
 *   nowcast,source=kma-ultra-ncst rn1_mm=<float>,base_time_s=<int>
 *   api_probe,service=pop_vilage|rn1_ultra success=<int>,latency_ms=<int>[,n_points=<int>,note="..."]
 */

type Env = {
  INFLUX_URL: string; INFLUX_TOKEN: string; INFLUX_ORG: string; INFLUX_BUCKET: string;
  APIHUB_BASE: string; APIHUB_KEY: string;
  POP_REG?: string; LOC?: string; NX?: string; NY?: string;
};
const env: Env = {
  INFLUX_URL: process.env.INFLUX_URL!,
  INFLUX_ORG: process.env.INFLUX_ORG!,
  INFLUX_BUCKET: process.env.INFLUX_BUCKET!,
  INFLUX_TOKEN: process.env.INFLUX_TOKEN!,
  APIHUB_BASE: (process.env.APIHUB_BASE || "https://apihub.kma.go.kr").replace(/\/+$/,""),
  APIHUB_KEY: process.env.APIHUB_KEY!,
  POP_REG: process.env.POP_REG || "11B10101",
  LOC: process.env.LOC || "seoul",
  NX: process.env.NX || "60",
  NY: process.env.NY || "127",
};
const DBG = process.env.DEBUG_POP === "1" || process.env.DEBUG === "1";
const need = <K extends keyof Env>(k: K) => {
  const v = env[k]; if (!v) throw new Error(`[FATAL] Missing env: ${String(k)}`); return v as NonNullable<Env[K]>;
};

// ----- time helpers (KST) -----
const pad2 = (n: number) => String(n).padStart(2,"0");
const toIsoKst = (yyyymmdd: string, hhmm: string) => {
  const yyyy = yyyymmdd.slice(0,4), MM = yyyymmdd.slice(4,6), dd = yyyymmdd.slice(6,8);
  const HH = hhmm.slice(0,2), mm = hhmm.slice(2,4);
  return `${yyyy}-${MM}-${dd}T${HH}:${mm}:00+09:00`;
};
const toEpochSec = (iso: string) => Math.floor(new Date(iso).getTime()/1000);

// 초단기 base: (분<45) → 직전 시각 HH00, (분≥45) → HH30
function ultraBase(date = new Date()) {
  const d = new Date(date);
  const yyyy = d.getFullYear(); const MM = pad2(d.getMonth()+1); const dd = pad2(d.getDate());
  let HH = d.getHours(); const m = d.getMinutes();
  let base_time = m < 45 ? `${pad2((HH+23)%24)}00` : `${pad2(HH)}30`;
  let base_date = `${yyyy}${MM}${dd}`;
  if (m < 45 && HH === 0) {
    const p = new Date(d.getTime() - 86400_000);
    base_date = `${p.getFullYear()}${pad2(p.getMonth()+1)}${pad2(p.getDate())}`;
  }
  return { base_date, base_time };
}

// ----- Influx -----
async function writeLP(lines: string[]) {
  if (!lines.length) return;
  const url = `${need("INFLUX_URL")}/api/v2/write?org=${encodeURIComponent(need("INFLUX_ORG"))}&bucket=${encodeURIComponent(need("INFLUX_BUCKET"))}&precision=s`;
  const res = await fetch(url, {
    method: "POST",
    headers: { "Authorization": `Token ${need("INFLUX_TOKEN")}`, "Content-Type": "text/plain; charset=utf-8" },
    body: lines.join("\n"),
  });
  if (!res.ok) throw new Error(`Influx write ${res.status}: ${await res.text().catch(()=>"...")}`);
}

// ----- API Hub JSON caller (typ02 → typ01 fallback) -----
async function callKMAJSON(path: string, params: Record<string,string>) {
  const base = need("APIHUB_BASE");
  const sp = new URLSearchParams({ dataType: "JSON", numOfRows: "1000", pageNo: "1", ...params,
    serviceKey: need("APIHUB_KEY"), authKey: need("APIHUB_KEY") // 양쪽 키 파라미터 동시
  });

  async function tryUrl(u: string) {
    const t0 = Date.now();
    const res = await fetch(u);
    const latency = Date.now() - t0;
    const txt = await res.text();
    try {
      const json = JSON.parse(txt);
      const header = json?.response?.header; const body = json?.response?.body;
      const code = header?.resultCode; const msg = header?.resultMsg;
      const items = body?.items?.item ?? [];
      if (DBG) console.log(`[DEBUG] call ${u}\n[DEBUG] header resultCode=${code||"-"} msg=${msg||"-"} latency=${latency}ms items=${items.length}`);
      return { ok: res.ok && (code === "00" || code === "NORMAL_SERVICE"), items, header, body, latency, url: u, raw: txt };
    } catch (e) {
      if (DBG) console.log(`[DEBUG] JSON parse fail: ${(e as Error).message}`);
      return { ok: false, items: [], header: {}, body: {}, latency, url: u, raw: txt };
    }
  }

  const url1 = `${base}/api/typ02/openApi/${path}?${sp.toString()}`;
  let out = await tryUrl(url1);
  if (!out.ok) {
    const url2 = `${base}/api/typ01/openApi/${path}?${sp.toString()}`;
    out = await tryUrl(url2);
  }
  return out;
}

// ----- parsing helpers -----
function parsePCPmm(v: string): number | null {
  const s = (v||"").trim();
  if (!s) return null;
  if (s.includes("없음")) return 0;
  if (s.includes("미만")) return 0.1;
  if (s.includes("이상")) { const m = s.match(/(\d+(?:\.\d+)?)/); return m ? parseFloat(m[1]) : null; }
  const m = s.match(/-?\d+(?:\.\d+)?/);
  return m ? parseFloat(m[0]) : null;
}
function parsePOPpct(v: string): number | null {
  const m = (v||"").match(/\d+/);
  if (!m) return null;
  const n = Math.max(0, Math.min(100, parseInt(m[0],10)));
  return Number.isFinite(n) ? n : null;
}
function parseRN1mm(v: unknown): number {
  if (typeof v === "number") return v;
  if (typeof v === "string") { const m = v.match(/-?\d+(?:\.\d+)?/); return m ? parseFloat(m[0]) : 0; }
  return 0;
}

// ----- fetchers -----
async function fetchForecast(nx: string, ny: string) {
  // 단기예보(동네예보): PCP + POP 함께 수집
  const { base_date, base_time } = ultraBase(); // 기준시각은 00/30 규칙으로 충분
  const out = await callKMAJSON("VilageFcstInfoService_2.0/getVilageFcst", { base_date, base_time, nx, ny });
  if (!out.ok) throw Object.assign(new Error(`[vilage] call failed`), { note: String(out.raw||"").slice(0,200), latency: out.latency, url: out.url });

  type Acc = { pcp?: number; pop?: number };
  const acc = new Map<number, Acc>(); // ts(sec) → {pcp, pop}
  for (const it of (out.items as any[])) {
    const y = String(it.fcstDate), h = String(it.fcstTime);
    const ts = toEpochSec(toIsoKst(y,h));
    if (!Number.isFinite(ts)) continue;
    const cur = acc.get(ts) || {};
    if (it.category === "PCP") {
      const p = parsePCPmm(String(it.fcstValue ?? ""));
      if (p != null) cur.pcp = p;
    } else if (it.category === "POP") {
      const q = parsePOPpct(String(it.fcstValue ?? ""));
      if (q != null) cur.pop = q;
    }
    if (cur.pcp != null || cur.pop != null) acc.set(ts, cur);
  }
  const base_s = toEpochSec(toIsoKst(base_date, base_time));
  const rows = [...acc.entries()].sort((a,b)=>a[0]-b[0]).map(([ts, v]) => ({ ts, ...v, base_s }));
  if (DBG) console.log(`[INFO] forecast rows=${rows.length} base=${base_date} ${base_time}`);
  return { rows, base_date, base_time };
}

async function fetchUltraRN1(nx: string, ny: string) {
  const { base_date, base_time } = ultraBase();
  const out = await callKMAJSON("VilageFcstInfoService_2.0/getUltraSrtNcst", { base_date, base_time, nx, ny });
  if (!out.ok) throw Object.assign(new Error(`[ultra-ncst] call failed`), { note: String(out.raw||"").slice(0,200), latency: out.latency, url: out.url });

  let rn1: number | null = null;
  let ts = 0;
  for (const it of (out.items as any[])) {
    if (it.category === "RN1") {
      rn1 = parseRN1mm(it.obsrValue ?? it.fcstValue ?? "0");
      // 관측시각은 자료상 baseDate/baseTime와 같거나 항목에 포함됨
      const y = String(it.baseDate || it.fcstDate || out.body?.baseDate || base_date);
      const h = String(it.baseTime || it.fcstTime || out.body?.baseTime || base_time);
      ts = toEpochSec(toIsoKst(y, h));
      break;
    }
  }
  return { rn1, ts, base_date, base_time };
}

// ----- main -----
(async () => {
  const loc = env.LOC!.trim(); const reg = env.POP_REG!.trim();
  const nx = env.NX!.trim();   const ny = env.NY!.trim();

  const lines: string[] = [];

  // 1) Forecast (PCP + POP)
  try {
    const f = await fetchForecast(nx, ny);
    const now = Math.floor(Date.now()/1000);
    const minS = now - 12*3600, maxS = now + 72*3600; // 보존 범위
    let n = 0, nPOP = 0, nPCP = 0;
    for (const r of f.rows) {
      if (r.ts < minS || r.ts > maxS) continue;
      const tags = `forecast,source=kma-vilage,loc=${loc},reg=${reg},nx=${nx},ny=${ny}`;
      const fields: string[] = [`base_time_s=${r.base_s}i`];
      if (typeof r.pcp === "number") { fields.unshift(`pcp_mm=${r.pcp.toFixed(2)}`); nPCP++; }
      if (typeof r.pop === "number") { fields.unshift(`pop_pct=${Math.round(r.pop)}i`); nPOP++; }
      lines.push(`${tags} ${fields.join(",")} ${r.ts}`);
      n++;
    }
    const probeTs = Math.floor(Date.now()/1000);
    lines.push(`api_probe,service=pop_vilage,env=prod,loc=${loc} success=1i,latency_ms=${0}i,n_points=${n}i,n_pop=${nPOP}i,n_pcp=${nPCP}i ${probeTs}`);
  } catch (e:any) {
    const probeTs = Math.floor(Date.now()/1000);
    lines.push(`api_probe,service=pop_vilage,env=prod,loc=${loc} success=0i,latency_ms=${e?.latency||0}i,note="${String(e?.note||e?.message||'err').replace(/"/g,'\\"')}" ${probeTs}`);
    if (DBG) console.error("[WARN] forecast fetch failed", e?.message || e);
  }

  // 2) Nowcast (RN1)
  try {
    const u = await fetchUltraRN1(nx, ny);
    if (u.rn1 != null && u.ts > 0) {
      const base_s = toEpochSec(toIsoKst(u.base_date, u.base_time));
      lines.push(`nowcast,source=kma-ultra-ncst,loc=${loc},reg=${reg},nx=${nx},ny=${ny} rn1_mm=${u.rn1.toFixed(1)},base_time_s=${base_s}i ${u.ts}`);
      const probeTs = Math.floor(Date.now()/1000);
      lines.push(`api_probe,service=rn1_ultra,env=prod,loc=${loc} success=1i,latency_ms=${0}i ${probeTs}`);
    } else {
      const probeTs = Math.floor(Date.now()/1000);
      lines.push(`api_probe,service=rn1_ultra,env=prod,loc=${loc} success=0i,latency_ms=0i,note="no RN1" ${probeTs}`);
    }
  } catch (e:any) {
    const probeTs = Math.floor(Date.now()/1000);
    lines.push(`api_probe,service=rn1_ultra,env=prod,loc=${loc} success=0i,latency_ms=${e?.latency||0}i,note="${String(e?.note||e?.message||'err').replace(/"/g,'\\"')}" ${probeTs}`);
    if (DBG) console.error("[WARN] ultra ncst fetch failed", e?.message || e);
  }

  if (!lines.length) {
    console.warn("[WARN] No lines to write");
    return;
  }
  if (DBG) console.log("[SAMPLE]\n" + lines.slice(0,2).join("\n"));

  await writeLP(lines);
  console.log(`[OK] Influx write done — lines=${lines.length}`);
})().catch(err => {
  console.error(err?.message || err);
  process.exitCode = 0;
});
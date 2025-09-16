import "dotenv/config";

/**
 * KMA POP/Rainfall → Influx (forecast + nowcast)
 * - Forecast: PCP(mm), POP(%) via getVilageFcst
 * - Nowcast:  RN1(mm) via getUltraSrtNcst (최근 30분 슬롯 다중 재시도)
 *
 * Measurement (precision=s)
 *   forecast,source=kma-vilage,loc=<>,reg=<>,nx=<>,ny=<>
 *     pop_pct=<int>,pcp_mm=<float>,base_time_s=<int>
 *   nowcast,source=kma-ultra-ncst,loc=<>,reg=<>,nx=<>,ny=<>
 *     rn1_mm=<float>,base_time_s=<int>
 *   api_probe,service=pop_vilage|rn1_ultra,env=prod,loc=<>
 *     success=<int>,latency_ms=<int>[,n_points=<int>,n_pop=<int>,n_pcp=<int>,note="..."]
 */

type Env = {
  INFLUX_URL: string; INFLUX_TOKEN: string; INFLUX_ORG: string; INFLUX_BUCKET: string;
  APIHUB_BASE: string; APIHUB_KEY: string;
  POP_REG?: string; LOC?: string; NX?: string; NY?: string;
};
const env: Env = {
  INFLUX_URL: process.env.INFLUX_URL || "",
  INFLUX_ORG: process.env.INFLUX_ORG || "",
  INFLUX_BUCKET: process.env.INFLUX_BUCKET || "",
  INFLUX_TOKEN: process.env.INFLUX_TOKEN || "",
  APIHUB_BASE: (process.env.APIHUB_BASE || "https://apihub.kma.go.kr").replace(/\/+$/, ""),
  APIHUB_KEY: process.env.APIHUB_KEY || "",
  POP_REG: process.env.POP_REG || "11B10101",
  LOC: process.env.LOC || "seoul",
  NX: process.env.NX || "60",
  NY: process.env.NY || "127",
};
const DBG = process.env.DEBUG_POP === "1" || process.env.DEBUG === "1";
const need = <K extends keyof Env>(k: K) => {
  const v = env[k]; if (!v) throw new Error(`[FATAL] Missing env: ${String(k)}`);
  return v as NonNullable<Env[K]>;
};

// ----- time helpers (KST) -----
const pad2 = (n: number) => String(n).padStart(2, "0");
const toIsoKst = (yyyymmdd: string, hhmm: string) => {
  const yyyy = yyyymmdd.slice(0,4), MM = yyyymmdd.slice(4,6), dd = yyyymmdd.slice(6,8);
  const HH = hhmm.slice(0,2), mm = hhmm.slice(2,4);
  return `${yyyy}-${MM}-${dd}T${HH}:${mm}:00+09:00`;
};
const toEpochSec = (iso: string) => Math.floor(new Date(iso).getTime()/1000);

// 초단기 기준시각(00/30): (분<45) → 직전 HH00, (분≥45) → HH30
function ultraBase(date = new Date()) {
  const d = new Date(date);
  const yyyy = d.getFullYear(); const MM = pad2(d.getMonth()+1); const dd = pad2(d.getDate());
  const HH = d.getHours(); const m = d.getMinutes();
  const base_time = m < 45 ? `${pad2((HH+23)%24)}00` : `${pad2(HH)}30`;
  let base_date = `${yyyy}${MM}${dd}`;
  if (m < 45 && HH === 0) {
    const p = new Date(d.getTime() - 86400_000);
    base_date = `${p.getFullYear()}${pad2(p.getMonth()+1)}${pad2(p.getDate())}`;
  }
  return { base_date, base_time };
}

// 최근 30분 스냅 슬롯(00/30) N개 생성 (KST 기준, 현재를 넘지 않게 스냅)
function ncstSlots30m(count = 4) {
  const slots: Array<{ base_date: string; base_time: string }> = [];
  const kstNow = new Date(Date.now() + 9*3600_000);
  kstNow.setSeconds(0, 0);
  const snapMin = kstNow.getUTCMinutes() >= 30 ? 30 : 0;
  kstNow.setUTCMinutes(snapMin);
  for (let i = 0; i < count; i++) {
    const d = new Date(kstNow.getTime() - i*30*60_000);
    const bd = `${d.getUTCFullYear()}${pad2(d.getUTCMonth()+1)}${pad2(d.getUTCDate())}`;
    const bt = `${pad2(d.getUTCHours())}${pad2(d.getUTCMinutes())}`; // "HH00" or "HH30"
    slots.push({ base_date: bd, base_time: bt });
  }
  return slots;
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
  const sp = new URLSearchParams({
    dataType: "JSON", numOfRows: "1000", pageNo: "1",
    ...params,
    serviceKey: need("APIHUB_KEY"),
    authKey: need("APIHUB_KEY"),
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
      const ok = res.ok && (code === "00" || code === "NORMAL_SERVICE");
      return { ok, items, header, body, latency, url: u, raw: txt };
    } catch (e) {
      if (DBG) console.log(`[DEBUG] JSON parse fail: ${(e as Error).message}`);
      return { ok: false, items: [], header: {}, body: {}, latency, url: u, raw: txt };
    }
  }

  let out = await tryUrl(`${base}/api/typ02/openApi/${path}?${sp.toString()}`);
  if (!out.ok) out = await tryUrl(`${base}/api/typ01/openApi/${path}?${sp.toString()}`);
  return out;
}

// ----- parsing helpers -----
function parsePCPmm(v: string): number | null {
  const s = (v||"").trim();
  if (!s) return null;
  if (/없음/.test(s)) return 0;
  if (/미만/.test(s)) return 0.1;
  if (/이상/.test(s)) { const m = s.match(/(\d+(?:\.\d+)?)/); return m ? parseFloat(m[1]) : null; }
  const m = s.match(/-?\d+(?:\.\d+)?/);
  return m ? parseFloat(m[0]) : null;
}
function parsePOPpct(v: string): number | null {
  const m = (v||"").match(/\d+/);
  if (!m) return null;
  const n = Math.max(0, Math.min(100, parseInt(m[0],10)));
  return Number.isFinite(n) ? n : null;
}

// ----- fetchers -----
async function fetchForecast(nx: string, ny: string) {
  // 동네예보(PCP+POP) — ultraBase(00/30) 기준도 APIHub에서 수용되는 형태(현행 운영 기준)
  const { base_date, base_time } = ultraBase();
  const out = await callKMAJSON("VilageFcstInfoService_2.0/getVilageFcst", { base_date, base_time, nx, ny });
  if (!out.ok) throw Object.assign(new Error(`[vilage] call failed`), { note: String(out.raw||"").slice(0,200), latency: out.latency, url: out.url });

  type Row = { ts: number; pcp?: number; pop?: number; base_s: number };
  const acc = new Map<number, { pcp?: number; pop?: number }>(); // fcst ts(sec) → values
  for (const it of (out.items as any[])) {
    const y = String(it.fcstDate), h = String(it.fcstTime);
    const ts = toEpochSec(toIsoKst(y,h));
    if (!Number.isFinite(ts)) continue;

    const cat = String(it.category);
    const cur = acc.get(ts) || {};
    if (cat === "PCP") {
      const p = parsePCPmm(String(it.fcstValue ?? ""));
      if (p != null) cur.pcp = p;
    } else if (cat === "POP") {
      const q = parsePOPpct(String(it.fcstValue ?? ""));
      if (q != null) cur.pop = q;
    }
    if (cur.pcp != null || cur.pop != null) acc.set(ts, cur);
  }
  const base_s = toEpochSec(toIsoKst(base_date, base_time));
  const rows: Row[] = [...acc.entries()]
    .map(([ts, v]) => ({ ts, ...v, base_s }))
    .sort((a,b)=>a.ts - b.ts);

  if (DBG) console.log(`[INFO] forecast rows=${rows.length} base=${base_date} ${base_time}`);
  return { rows, base_date, base_time, latency: out.latency ?? 0 };
}

async function fetchUltraRN1(nx: string, ny: string) {
  // 최근 30분 스냅 슬롯들을 순회하며 RN1이 있는 첫 응답을 채택
  const slots = ncstSlots30m(4); // 최대 2시간 커버
  let lastLatency = 0;

  for (const { base_date, base_time } of slots) {
    const out = await callKMAJSON("VilageFcstInfoService_2.0/getUltraSrtNcst", { base_date, base_time, nx, ny });
    lastLatency = out.latency ?? 0;
    if (!out.ok || !Array.isArray(out.items) || out.items.length === 0) continue;

    let rn1: number | null = null;
    let ts = 0;

    for (const it of (out.items as any[])) {
      if (String(it.category) !== "RN1") continue;

      const raw = String(it.obsrValue ?? it.fcstValue ?? "");
      const m = raw.match(/-?\d+(?:\.\d+)?/);
      if (!m) continue;

      rn1 = Math.max(0, parseFloat(m[0])); // 음수 방지
      const y = String(it.baseDate || it.fcstDate || out.body?.baseDate || base_date);
      const h = String(it.baseTime || it.fcstTime || out.body?.baseTime || base_time);
      ts = toEpochSec(toIsoKst(y, h));
      break;
    }

    if (rn1 != null && ts > 0) {
      return { rn1, ts, base_date, base_time, latency: lastLatency };
    }
  }
  throw Object.assign(new Error("RN1 not found in recent 30-min slots"), { latency: lastLatency });
}

// ----- main -----
(async () => {
  const loc = need("LOC").trim();
  const reg = need("POP_REG").trim();
  const nx  = need("NX").trim();
  const ny  = need("NY").trim();

  const lines: string[] = [];

  // 1) Forecast (PCP + POP)
  try {
    const f = await fetchForecast(nx, ny);
    const now = Math.floor(Date.now()/1000);
    const minS = now - 12*3600, maxS = now + 72*3600; // 표시/보존 범위
    let n = 0, nPOP = 0, nPCP = 0;

    for (const r of f.rows) {
      if (r.ts < minS || r.ts > maxS) continue;
      const tags = `forecast,source=kma-vilage,loc=${loc},reg=${reg},nx=${nx},ny=${ny}`;
      const fields: string[] = [`base_time_s=${r.base_s}i`];
      if (typeof r.pcp === "number") { fields.unshift(`pcp_mm=${Number(r.pcp.toFixed(2))}`); nPCP++; }
      if (typeof r.pop === "number") { fields.unshift(`pop_pct=${Math.round(r.pop)}i`); nPOP++; }
      lines.push(`${tags} ${fields.join(",")} ${r.ts}`);
      n++;
    }
    lines.push(`api_probe,service=pop_vilage,env=prod,loc=${loc} success=1i,latency_ms=${f.latency}i,n_points=${n}i,n_pop=${nPOP}i,n_pcp=${nPCP}i ${now}`);
  } catch (e:any) {
    const now = Math.floor(Date.now()/1000);
    lines.push(`api_probe,service=pop_vilage,env=prod,loc=${env.LOC||"seoul"} success=0i,latency_ms=${e?.latency||0}i,note="${String(e?.note||e?.message||"err").replace(/"/g,'\\"')}" ${now}`);
    if (DBG) console.error("[WARN] forecast fetch failed:", e?.message || e);
  }

  // 2) Nowcast (RN1 with multi-slot retry)
  try {
    const u = await fetchUltraRN1(nx, ny);
    const base_s = toEpochSec(toIsoKst(u.base_date, u.base_time));
    const rn1Val = Number(u.rn1.toFixed(1)); // 표준화(소수 1자리)
    lines.push(`nowcast,source=kma-ultra-ncst,loc=${loc},reg=${reg},nx=${nx},ny=${ny} rn1_mm=${rn1Val},base_time_s=${base_s}i ${u.ts}`);
    lines.push(`api_probe,service=rn1_ultra,env=prod,loc=${loc} success=1i,latency_ms=${u.latency||0}i ${Math.floor(Date.now()/1000)}`);
  } catch (e:any) {
    const now = Math.floor(Date.now()/1000);
    lines.push(`api_probe,service=rn1_ultra,env=prod,loc=${env.LOC||"seoul"} success=0i,latency_ms=${e?.latency||0}i,note="${String(e?.note||e?.message||"err").replace(/"/g,'\\"')}" ${now}`);
    if (DBG) console.error("[WARN] ultra ncst fetch failed:", e?.message || e);
  }

  if (DBG && lines.length) console.log("[SAMPLE]\n" + lines.slice(0, 4).join("\n"));

  await writeLP(lines);
  console.log(`[OK] Influx write done — lines=${lines.length}`);
})().catch(err => {
  console.error(err?.message || err);
  process.exitCode = 0; // probe 라인 남기기 위해 hard fail 하지 않음
});

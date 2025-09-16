import "dotenv/config";

/**
 * Apparent Temperature (체감온도) + 실제 기온 수집 → Influx
 * - Source: KMA API Hub Ultra Short NOWCAST (getUltraSrtNcst)
 *   categories used: T1H(기온, °C), REH(상대습도, %), WSD(풍속, m/s)
 * - Feels Like 선택 규칙
 *   - if T>=27°C and RH>=40% → Heat Index (NWS Rothfusz)
 *   - else if T<=10°C and wind>=1.34 m/s → Wind Chill (Celsius formula)
 *   - else → Australian Apparent Temperature (Steadman, shade, no radiation)
 *
 * Measurement/fields (precision=s)
 *   life_index,source=kma-ultra-ncst,loc=<loc>,stn=<108>,method=hi|wc|at
 *     temp_c=<float>,rh_pct=<float>,wind_ms=<float>,feels_c=<float>,
 *     heat_index_c=<float?>,wind_chill_c=<float?>,apparent_c=<float?>,
 *     base_time_s=<int>
 *   api_probe,service=feels_ultra,env=prod,loc=<loc> success=<int>,latency_ms=<int>
 *
 * 실행(로컬):
 *   npx dotenv -e .env -- ts-node scripts/asos_feels_to_influx.ts
 * 필요 env: INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET,
 *           APIHUB_BASE, APIHUB_KEY, LOC=seoul, NX=60, NY=127, ASOS_STN=108
 */

type Env = {
  INFLUX_URL: string; INFLUX_TOKEN: string; INFLUX_ORG: string; INFLUX_BUCKET: string;
  APIHUB_BASE: string; APIHUB_KEY: string;
  LOC?: string; NX?: string; NY?: string; ASOS_STN?: string;
};
const env = process.env as unknown as Env;
const need = (k: keyof Env) => { const v = env[k]; if (!v) throw new Error(`[FATAL] Missing env: ${k}`); return v; };
const DBG = !!(process.env.DEBUG_POP || process.env.DEBUG);

// ---- time helpers (KST base rule for Ultra) ----
const pad2 = (n: number) => String(n).padStart(2, "0");
const toIsoKst = (yyyymmdd: string, hhmm: string) => `${yyyymmdd.slice(0,4)}-${yyyymmdd.slice(4,6)}-${yyyymmdd.slice(6,8)}T${hhmm.slice(0,2)}:${hhmm.slice(2,4)}:00+09:00`;
const toEpochSec = (iso: string) => Math.floor(new Date(iso).getTime() / 1000);

function ultraBase(date = new Date()) {
  const d = new Date(date);
  const yyyy = d.getFullYear(); const MM = pad2(d.getMonth()+1); const dd = pad2(d.getDate());
  const HH = d.getHours(); const m = d.getMinutes();
  let base_time = m < 45 ? `${pad2((HH+23)%24)}00` : `${pad2(HH)}30`;
  let base_date = `${yyyy}${MM}${dd}`;
  if (m < 45 && HH === 0) { const p = new Date(d.getTime()-86400000); base_date = `${p.getFullYear()}${pad2(p.getMonth()+1)}${pad2(p.getDate())}`; }
  return { base_date, base_time };
}

// ---- Influx ----
async function writeLP(lines: string[]) {
  if (!lines.length) return;
  const url = `${need('INFLUX_URL')}/api/v2/write?org=${encodeURIComponent(need('INFLUX_ORG'))}&bucket=${encodeURIComponent(need('INFLUX_BUCKET'))}&precision=s`;
  const res = await fetch(url, { method: 'POST', headers: { 'Authorization': `Token ${need('INFLUX_TOKEN')}`, 'Content-Type': 'text/plain; charset=utf-8' }, body: lines.join('\n') });
  if (!res.ok) throw new Error(`Influx write ${res.status}: ${await res.text().catch(()=>"...")}`);
}

// ---- KMA JSON caller (typ02 → typ01 fallback) ----
async function callKMAJSON(path: string, params: Record<string,string>) {
  const base = need('APIHUB_BASE').replace(/\/+$/, '');
  const sp = new URLSearchParams({ dataType: 'JSON', numOfRows: '1000', pageNo: '1', ...params, serviceKey: need('APIHUB_KEY'), authKey: need('APIHUB_KEY') });
  async function tryUrl(u: string) {
    const t0 = Date.now(); const res = await fetch(u); const latency = Date.now() - t0; const txt = await res.text();
    try {
      const json = JSON.parse(txt); const header = json?.response?.header; const body = json?.response?.body; const code = header?.resultCode; const msg = header?.resultMsg; const items = body?.items?.item ?? [];
      if (DBG) console.log(`[DEBUG] call ${u}\n[DEBUG] header resultCode=${code||'-'} msg=${msg||'-'} latency=${latency}ms items=${items.length}`);
      return { ok: res.ok && (code==='00' || code==='NORMAL_SERVICE'), items, header, body, latency, url: u, raw: txt };
    } catch (e) {
      if (DBG) console.log(`[DEBUG] JSON parse fail: ${(e as Error).message}`);
      return { ok: false, items: [], header: {}, body: {}, latency, url: u, raw: txt };
    }
  }
  let out = await tryUrl(`${base}/api/typ02/openApi/VilageFcstInfoService_2.0/getUltraSrtNcst?${sp.toString()}`);
  if (!out.ok) out = await tryUrl(`${base}/api/typ01/openApi/VilageFcstInfoService_2.0/getUltraSrtNcst?${sp.toString()}`);
  return out;
}

// ---- Feels-like formulas ----
const c2f = (c:number)=> c*9/5+32; const f2c = (f:number)=> (f-32)*5/9;

// Heat Index (NWS Rothfusz). Inputs: T(°C), RH(%). Returns °C.
function heatIndexC(tC:number, rh:number): number {
  const T = c2f(tC); const R = rh;
  // Rothfusz regression (°F)
  let HI = -42.379 + 2.04901523*T + 10.14333127*R - .22475541*T*R - .00683783*T*T - .05481717*R*R + .00122874*T*T*R + .00085282*T*R*R - .00000199*T*T*R*R;
  // low humidity adjustment
  if (R < 13 && T >= 80 && T <= 112) {
    HI -= ((13 - R)/4) * Math.sqrt((17 - Math.abs(T - 95)) / 17);
  }
  // high humidity adjustment
  if (R > 85 && T >= 80 && T <= 87) {
    HI += ((R - 85)/10) * ((87 - T)/5);
  }
  return f2c(HI);
}

// Wind Chill (Celsius version). Valid T<=10°C, v>=4.8 km/h
function windChillC(tC:number, windMs:number): number {
  const vKmh = windMs * 3.6;
  return 13.12 + 0.6215*tC - 11.37*Math.pow(vKmh, 0.16) + 0.3965*tC*Math.pow(vKmh, 0.16);
}

// Australian Apparent Temperature (Steadman, shade). ws in m/s
function apparentTempC(tC:number, rh:number, windMs:number): number {
  const e = (rh/100) * 6.105 * Math.exp((17.27*tC)/(237.7 + tC)); // hPa
  return tC + 0.33*e - 0.70*windMs - 4.00;
}

function chooseFeels(tC:number, rh:number, windMs:number) {
  if (tC >= 27 && rh >= 40) return { method: 'hi', value: heatIndexC(tC, rh) };
  if (tC <= 10 && windMs >= 1.34) return { method: 'wc', value: windChillC(tC, windMs) };
  return { method: 'at', value: apparentTempC(tC, rh, windMs) };
}

// ---- fetch, compute, write ----
async function fetchUltraNow(nx:string, ny:string) {
  const { base_date, base_time } = ultraBase();
  const out = await callKMAJSON('VilageFcstInfoService_2.0/getUltraSrtNcst', { base_date, base_time, nx, ny });
  if (!out.ok) throw Object.assign(new Error('[ultra-ncst] call failed'), { latency: out.latency, note: String(out.raw||'').slice(0,200), url: out.url });
  let T: number|undefined, RH: number|undefined, W: number|undefined; let refISO = toIsoKst(base_date, base_time);
  for (const it of (out.items as any[])) {
    const cat = String(it.category);
    if (cat === 'T1H') T = parseFloat(it.obsrValue ?? it.fcstValue);
    else if (cat === 'REH') RH = parseFloat(it.obsrValue ?? it.fcstValue);
    else if (cat === 'WSD') W = parseFloat(it.obsrValue ?? it.fcstValue);
    if (it.baseDate && it.baseTime) refISO = toIsoKst(String(it.baseDate), String(it.baseTime));
  }
  const ts = toEpochSec(refISO);
  return { T, RH, W, base_date, base_time, ts };
}

(async () => {
  const loc = (env.LOC || 'seoul').trim();
  const nx = (env.NX || '60').trim();
  const ny = (env.NY || '127').trim();
  const stn = (env.ASOS_STN || '108').trim(); // 태그 호환

  const lines: string[] = [];
  try {
    const r = await fetchUltraNow(nx, ny);
    if (r.T == null || r.RH == null || r.W == null || !Number.isFinite(r.ts)) throw new Error('Missing T/RH/W/ts');
    const { method, value } = chooseFeels(r.T, r.RH, r.W);
    const hi = heatIndexC(r.T, r.RH);
    const wc = windChillC(r.T, r.W);
    const at = apparentTempC(r.T, r.RH, r.W);
    const base_s = toEpochSec(toIsoKst(r.base_date, r.base_time));

    const tags = `life_index,source=kma-ultra-ncst,loc=${loc},stn=${stn},method=${method}`;
    const fields = [
      `temp_c=${r.T.toFixed(2)}`,
      `rh_pct=${r.RH.toFixed(1)}`,
      `wind_ms=${r.W.toFixed(2)}`,
      `feels_c=${value.toFixed(2)}`,
      `heat_index_c=${hi.toFixed(2)}`,
      `wind_chill_c=${wc.toFixed(2)}`,
      `apparent_c=${at.toFixed(2)}`,
      `base_time_s=${base_s}i`
    ].join(',');
    lines.push(`${tags} ${fields} ${r.ts}`);

    const probeTs = Math.floor(Date.now()/1000);
    lines.push(`api_probe,service=feels_ultra,env=prod,loc=${loc} success=1i,latency_ms=0i ${probeTs}`);
  } catch (e:any) {
    const probeTs = Math.floor(Date.now()/1000);
    lines.push(`api_probe,service=feels_ultra,env=prod,loc=${(env.LOC||'seoul')} success=0i,latency_ms=${e?.latency||0}i,note="${String(e?.note||e?.message||'err').replace(/"/g,'\\"')}" ${probeTs}`);
  }

  if (!lines.length) { console.warn('[WARN] No lines to write'); return; }
  if (DBG) console.log('[SAMPLE]', lines[0]);
  await writeLP(lines);
  console.log(`[OK] Influx write done — lines=${lines.length}`);
})().catch(err => { console.error(err?.message || err); process.exitCode = 0; });
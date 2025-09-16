// scripts/asos_feels_to_influx.ts
import "dotenv/config";

/**
 * Apparent Temperature(체감온도) + 현재기온 수집 → Influx
 *   Source: KMA API Hub Ultra Short NOWCAST (getUltraSrtNcst)
 *   사용 카테고리: T1H(기온, °C), REH(습도, %), WSD(풍속, m/s)
 *
 * Ncst 규칙(실전 안정화):
 *   - 기준시각은 **HH00, HH30** (두 가지 모두 존재)
 *   - 데이터는 보통 기준시각 +10분 안팎부터 유효
 *   - 항상 "가장 최근 유효한 반시간 슬롯"을 찾고, 실패 시 30분 단위로 여러 슬롯을 백오프
 *
 * Measurement (precision=s)
 *   life_index,source=kma-ultra-ncst,loc=<>,stn=<108>,method=hi|wc|at
 *     temp_c=<float>,rh_pct=<float>,wind_ms=<float>,feels_c=<float>,
 *     heat_index_c=<float>,wind_chill_c=<float>,apparent_c=<float>,
 *     base_time_s=<int>
 *   api_probe,service=feels_ultra,env=prod,loc=<loc> success=<int>,latency_ms=<int>
 */

type Env = {
  INFLUX_URL: string; INFLUX_TOKEN: string; INFLUX_ORG: string; INFLUX_BUCKET: string;
  APIHUB_BASE: string; APIHUB_KEY: string;
  LOC?: string; NX?: string; NY?: string; ASOS_STN?: string;
};
const env = process.env as unknown as Env;
const need = (k: keyof Env) => { const v = env[k]; if (!v) throw new Error(`[FATAL] Missing env: ${k}`); return v; };
const DBG = !!(process.env.DEBUG || process.env.DEBUG_FEELS);

// ---------- time utils ----------
const pad2 = (n: number) => String(n).padStart(2, "0");
const toIsoKst = (yyyymmdd: string, hhmm: string) =>
  `${yyyymmdd.slice(0,4)}-${yyyymmdd.slice(4,6)}-${yyyymmdd.slice(6,8)}T${hhmm.slice(0,2)}:${hhmm.slice(2,4)}:00+09:00`;
const toEpochSec = (iso: string) => Math.floor(new Date(iso).getTime() / 1000);
const yyyymmdd = (d: Date) => `${d.getFullYear()}${pad2(d.getMonth()+1)}${pad2(d.getDate())}`;

/** 최근 유효 반시간(anchor) 산정: 
 *   m>=40 → HH30
 *   10<=m<40 → HH00
 *   m<10 → (HH-1)30
 * 이후 30분 단위로 최대 4~6개 슬롯 백오프
 */
function halfHourAnchors(now = new Date(), depth = 6): Array<{base_date:string;base_time:string}> {
  const d = new Date(now);
  const m = d.getMinutes();
  let anchor = new Date(d);

  if (m >= 40) { anchor.setMinutes(30, 0, 0); }
  else if (m >= 10) { anchor.setMinutes(0, 0, 0); }
  else { anchor = new Date(anchor.getTime() - 60*60*1000); anchor.setMinutes(30, 0, 0); }

  const out: Array<{base_date:string;base_time:string}> = [];
  for (let i=0; i<depth; i++) {
    const t = new Date(anchor.getTime() - i * 30*60*1000);
    out.push({ base_date: yyyymmdd(t), base_time: `${pad2(t.getHours())}${pad2(t.getMinutes())}` });
  }
  return out;
}

// ---------- Influx ----------
async function writeLP(lines: string[]) {
  if (!lines.length) return;
  const url = `${need('INFLUX_URL')}/api/v2/write?org=${encodeURIComponent(need('INFLUX_ORG'))}&bucket=${encodeURIComponent(need('INFLUX_BUCKET'))}&precision=s`;
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Authorization': `Token ${need('INFLUX_TOKEN')}`, 'Content-Type': 'text/plain; charset=utf-8' },
    body: lines.join('\n')
  });
  if (!res.ok) throw new Error(`Influx write ${res.status}: ${await res.text().catch(()=>"...")}`);
}

// ---------- KMA JSON (typ02 → typ01) ----------
async function callKMAJSON(params: Record<string,string>) {
  const base = need('APIHUB_BASE').replace(/\/+$/, '');
  const sp = new URLSearchParams({
    dataType: 'JSON', numOfRows: '1000', pageNo: '1',
    ...params,
    serviceKey: need('APIHUB_KEY'),
    authKey: need('APIHUB_KEY'),
  });

  async function tryUrl(u: string) {
    const t0 = Date.now();
    const res = await fetch(u);
    const latency = Date.now() - t0;
    const txt = await res.text();
    try {
      const json = JSON.parse(txt);
      const header = json?.response?.header;
      const body = json?.response?.body;
      const code = header?.resultCode;
      const msg  = header?.resultMsg;
      const items = body?.items?.item ?? [];
      if (DBG) console.log(`[DEBUG] call ${u}\n[DEBUG] header resultCode=${code||'-'} msg=${msg||'-'} latency=${latency}ms items=${items.length}`);
      const ok = res.ok && (code==='00' || code==='NORMAL_SERVICE');
      return { ok, items, header, body, latency, url: u, raw: txt };
    } catch (e) {
      if (DBG) console.log(`[DEBUG] JSON parse fail: ${(e as Error).message}`);
      return { ok: false, items: [], header: {}, body: {}, latency, url: u, raw: txt };
    }
  }

  let out = await tryUrl(`${base}/api/typ02/openApi/VilageFcstInfoService_2.0/getUltraSrtNcst?${sp.toString()}`);
  if (!out.ok) out = await tryUrl(`${base}/api/typ01/openApi/VilageFcstInfoService_2.0/getUltraSrtNcst?${sp.toString()}`);
  return out;
}

// ---------- Feels-like ----------
const c2f = (c:number)=> c*9/5+32; const f2c = (f:number)=> (f-32)*5/9;
function heatIndexC(tC:number, rh:number): number {
  const T = c2f(tC), R = rh;
  let HI = -42.379 + 2.04901523*T + 10.14333127*R - .22475541*T*R - .00683783*T*T - .05481717*R*R
         + .00122874*T*T*R + .00085282*T*R*R - .00000199*T*T*R*R;
  if (R < 13 && T >= 80 && T <= 112) HI -= ((13 - R)/4) * Math.sqrt((17 - Math.abs(T - 95)) / 17);
  if (R > 85 && T >= 80 && T <= 87)   HI += ((R - 85)/10) * ((87 - T)/5);
  return f2c(HI);
}
function windChillC(tC:number, windMs:number): number {
  const vKmh = windMs * 3.6;
  return 13.12 + 0.6215*tC - 11.37*Math.pow(vKmh, .16) + 0.3965*tC*Math.pow(vKmh, .16);
}
function apparentTempC(tC:number, rh:number, windMs:number): number {
  const e = (rh/100) * 6.105 * Math.exp((17.27*tC)/(237.7 + tC));
  return tC + 0.33*e - 0.70*windMs - 4.00;
}
function chooseFeels(tC:number, rh:number, windMs:number) {
  if (tC >= 27 && rh >= 40) return { method: 'hi', value: heatIndexC(tC, rh) };
  if (tC <= 10 && windMs >= 1.34) return { method: 'wc', value: windChillC(tC, windMs) };
  return { method: 'at', value: apparentTempC(tC, rh, windMs) };
}

// ---------- fetch, compute, write ----------
async function fetchUltraNcst(nx:string, ny:string) {
  const slots = halfHourAnchors(undefined, 6); // 최신부터 30분 간격 6개
  let lastErr: any = null;

  for (const { base_date, base_time } of slots) {
    const out = await callKMAJSON({ base_date, base_time, nx, ny });
    if (!out.ok || !Array.isArray(out.items) || out.items.length === 0) { lastErr = out; continue; }

    let T: number|undefined, RH: number|undefined, W: number|undefined;
    for (const it of out.items as any[]) {
      const cat = String(it.category);
      const val = parseFloat(it.obsrValue ?? it.fcstValue);
      if (cat === 'T1H') T = val;
      else if (cat === 'REH') RH = val;
      else if (cat === 'WSD') W = val;
    }
    if (T == null || RH == null || W == null) { lastErr = new Error('Missing T/RH/W'); continue; }

    // ts: 기준시각 + 10분(가시화 버킷과 실제 제공 시점을 맞추기 위함)
    const baseIso = toIsoKst(base_date, base_time);
    const tsBase = toEpochSec(baseIso);
    const ts = tsBase + 600; // +10m

    return { T, RH, W, base_date, base_time, ts, latency: out.latency, base_s: tsBase };
  }
  throw Object.assign(new Error('Ncst empty (all half-hour slots failed)'), { lastErr });
}

(async () => {
  const loc = (env.LOC || 'seoul').trim();
  const nx  = (env.NX  || '60').trim();
  const ny  = (env.NY  || '127').trim();
  const stn = (env.ASOS_STN || '108').trim(); // 태그 호환용

  const lines: string[] = [];
  let latency = 0;
  try {
    const r = await fetchUltraNcst(nx, ny);
    latency = r.latency ?? 0;

    const { method, value } = chooseFeels(r.T!, r.RH!, r.W!);
    const hi = heatIndexC(r.T!, r.RH!);
    const wc = windChillC(r.T!, r.W!);
    const at = apparentTempC(r.T!, r.RH!, r.W!);

    const tags = `life_index,source=kma-ultra-ncst,loc=${loc},stn=${stn},method=${method}`;
    const fields = [
      `temp_c=${r.T!.toFixed(2)}`,
      `rh_pct=${r.RH!.toFixed(1)}`,
      `wind_ms=${r.W!.toFixed(2)}`,
      `feels_c=${value.toFixed(2)}`,
      `heat_index_c=${hi.toFixed(2)}`,
      `wind_chill_c=${wc.toFixed(2)}`,
      `apparent_c=${at.toFixed(2)}`,
      `base_time_s=${r.base_s}i`
    ].join(',');

    lines.push(`${tags} ${fields} ${r.ts}`);
    lines.push(`api_probe,service=feels_ultra,env=prod,loc=${loc} success=1i,latency_ms=${latency}i ${Math.floor(Date.now()/1000)}`);

    if (DBG) {
      console.log('[DEBUG] slot chosen:', r.base_date, r.base_time, '→ ts', r.ts);
      console.log('[SAMPLE]', lines[0]);
    }
  } catch (e:any) {
    if (DBG) console.error('[DEBUG] fetch error', e?.message || e);
    lines.push(`api_probe,service=feels_ultra,env=prod,loc=${(env.LOC||'seoul')} success=0i,latency_ms=${latency}i ${Math.floor(Date.now()/1000)}`);
  }

  await writeLP(lines);
  console.log(`[OK] Influx write done — lines=${lines.length}`);
})().catch(err => { console.error(err?.message || err); process.exitCode = 0; });

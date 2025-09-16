/**
 * KMAHub ASOS(kma_sfctm2.php) → Apparent Temperature(AT) 계산 → InfluxDB
 * - AT = T + 0.33*e − 0.70*ws − 4.00  (e[hPa] = RH/100 * 6.105 * exp(17.27*T/(237.7+T)))
 * - HM 없으면 TD로 RH 역산(마그누스), RH/WS 범위 방어
 * - 관측시각 신선도 가드: now-3h 이전이면 쓰기 스킵
 *
 * 실행: npx ts-node scripts/asos_feels_to_influx.ts
 * 필요 ENV: INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET, APIHUB_BASE, APIHUB_KEY
 *          ASOS_STN=108, LOC=seoul (옵션)
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
  DEBUG_ASOS?: string;
};
const env = process.env as unknown as Env;
const need = (k: keyof Env) => { const v = env[k]; if (!v) throw new Error(`Missing env: ${k}`); return v; };
const DBG = !!env.DEBUG_ASOS;

// ---------- 유틸 ----------
function splitCSVLine(line: string): string[] {
  const out: string[] = []; let cur = ""; let q = false;
  for (let i=0;i<line.length;i++){
    const c=line[i];
    if(c==='"'){ if(q && line[i+1]==='"'){ cur+='"'; i++; } else { q=!q; } }
    else if(c===',' && !q){ out.push(cur.trim()); cur=""; }
    else cur += c;
  }
  out.push(cur.trim());
  return out;
}
const toLines = (t: string) => t.replace(/\ufeff/g,"").split(/\r?\n/).filter(l => l.trim().length>0);
const splitBy = (line: string, mode: "csv"|"ws") => (mode==="csv" ? splitCSVLine(line) : line.replace(/^#\s*/,"").trim().split(/\s+/));

const toNum = (s?: string) => {
  const n = parseFloat(String(s ?? ""));
  // -9, -9.0, -99, -999 등 결측은 NaN 처리
  return !isFinite(n) || n <= -8.9 ? NaN : n;
};

// EUC-KR/UTF-8 자동 복원
async function decodeKR(res: Response): Promise<string> {
  const ab = await res.arrayBuffer(); const buf = Buffer.from(ab);
  const ct = (res.headers.get("content-type") || "").toLowerCase();
  if (/euc-?kr|ks_c_5601|cp949/.test(ct)) return iconv.decode(buf, "euc-kr");
  const utf = buf.toString("utf8");
  if (utf.includes("\uFFFD")) return iconv.decode(buf, "euc-kr");
  return utf;
}

// Influx write
async function writeLP(lines: string[]): Promise<void> {
  const url = `${need("INFLUX_URL")}/api/v2/write?org=${encodeURIComponent(need("INFLUX_ORG"))}&bucket=${encodeURIComponent(need("INFLUX_BUCKET"))}&precision=s`;
  const res = await fetch(url, { method:"POST", headers:{ Authorization:`Token ${need("INFLUX_TOKEN")}`, "Content-Type":"text/plain; charset=utf-8" }, body: lines.join("\n") });
  if (!res.ok) throw new Error(`Influx write ${res.status}: ${await res.text()}`);
}

// 시간 파싱 (YYYYMMDDHHmm / +KST)
function parseTs(raw: string): number | null {
  if (/^\d{12,14}$/.test(raw)) {
    const yyyy=raw.slice(0,4), MM=raw.slice(4,6), dd=raw.slice(6,8), HH=raw.slice(8,10), mm=raw.slice(10,12), ss=(raw.slice(12,14)||"00");
    const d = new Date(`${yyyy}-${MM}-${dd}T${HH}:${mm}:${ss}+09:00`);
    const t = d.getTime(); return isNaN(t) ? null : Math.floor(t/1000);
  }
  const d = new Date(raw.replace(" ","T") + (/\+/.test(raw)?"":"+09:00"));
  const t = d.getTime(); return isNaN(t) ? null : Math.floor(t/1000);
}

// AT(℃)
function apparentTempC(tC: number, rh: number, wMs: number): number {
  const e = (rh/100) * 6.105 * Math.exp(17.27*tC/(237.7+tC)); // hPa
  return tC + 0.33*e - 0.70*wMs - 4.0;
}

// WindChill 보정(극저온용, 선택)
function windChillC(tC: number, vMs: number): number {
  const v = vMs * 3.6; // km/h
  if (tC > 10 || v <= 4.8) return tC;
  return 13.12 + 0.6215*tC - 11.37*Math.pow(v, 0.16) + 0.3965*tC*Math.pow(v, 0.16);
}

// TD→RH 역산(마그누스)
function rhFromT_Td(tC: number, tdC: number): number {
  const es = 6.1094 * Math.exp(17.625*tC/(243.04+tC));
  const e  = 6.1094 * Math.exp(17.625*tdC/(243.04+tdC));
  return Math.max(0, Math.min(100, (e/es)*100));
}

// ---------- KMAHub ASOS fetch ----------
async function fetchLatestASOS(stn: string) {
  const now = new Date();
  const tm2 = now.toISOString().replace(/[-:]/g, "").slice(0,12) + "00";
  const tm1 = new Date(now.getTime() - 3*3600*1000).toISOString().replace(/[-:]/g, "").slice(0,12) + "00";

  const url = `${need("APIHUB_BASE")}/api/typ01/url/kma_sfctm2.php?stn=${encodeURIComponent(stn)}&tm1=${tm1}&tm2=${tm2}&disp=1&help=1&authKey=${encodeURIComponent(need("APIHUB_KEY"))}`;

  const t0 = Date.now();
  const res = await fetch(url);
  const latency = Date.now() - t0;
  const text = await decodeKR(res);
  if (!res.ok) throw new Error(`ASOS ${res.status}: ${text.slice(0,180)}`);

  const lines = toLines(text);
  const comments = lines.filter(l => l.trim().startsWith("#"));
  const data = lines.filter(l => !l.trim().startsWith("#"));

  let mode: "csv"|"ws" = comments.some(l=>l.includes(",")) || data[0]?.includes(",") ? "csv" : "ws";
  const headerLine = [...comments].reverse().find(l => /(TM|TIME|DATE).*(TA|기온).*(HM|RH|습도).*(WS|풍속)/i.test(l)) || null;
  const header = headerLine ? splitBy(headerLine, mode) : [];
  const rows = data.map(l => splitBy(l, mode)).filter(r => r.length >= 5);
  if (!rows.length) throw new Error("ASOS: no data rows");

  // 1) 헤더 명 매핑
  const find = (re: RegExp) => header.findIndex(h => re.test(h));
  let iTM = find(/^(TM|TIME|DATE)$/i);
  let iTA = find(/^(TA|T|기온)$/i);
  let iHM = find(/^(HM|RH|습도)$/i);
  let iTD = find(/^(TD|DEW|이슬점)$/i);
  let iWS = find(/^(WS|WIND|풍속)$/i);

  // 2) 값 기반 보조 매핑
  const numeric = rows.map(r => r.map(toNum));
  const cols = rows[0].length;
  const bestIndex = (ok:(v:number)=>boolean) => {
    let best=-1,score=-1;
    for (let c=0;c<cols;c++){ let cnt=0; for (const row of numeric){ const v=row[c]; if(isFinite(v)&&ok(v))cnt++; } if(cnt>score){score=cnt;best=c;} }
    return best;
  };
  if (iHM<0) iHM = bestIndex(v => v>=0 && v<=100);
  if (iWS<0) iWS = bestIndex(v => v>=0 && v<=60);
  if (iTA<0) iTA = bestIndex(v => v>-50 && v< 50);
  if (iTD<0) iTD = bestIndex(v => v>-60 && v< 40);
  if (iTM<0) iTM = rows[0].findIndex(v => /^\d{12,14}$/.test(v));

  if (DBG) {
    console.log("Header:", header);
    console.log("Idx:", {iTM,iTA,iHM,iTD,iWS});
    console.log("Sample:", rows.at(-1));
  }

  // 최신 유효행에서 값 추출
  for (let k=rows.length-1;k>=0;k--){
    const r = rows[k];
    const tC  = toNum(r[iTA]);
    const rh0 = iHM>=0 ? toNum(r[iHM]) : NaN;
    const tdC = iTD>=0 ? toNum(r[iTD]) : NaN;
    const ws  = toNum(r[iWS]);

    if (!isFinite(tC) || !isFinite(ws)) continue;
    let rh = isFinite(rh0) ? rh0 : (isFinite(tdC) ? rhFromT_Td(tC, tdC) : NaN);
    if (!isFinite(rh)) continue;

    // 방어
    rh = Math.max(5, Math.min(100, rh));
    const wMs = Math.max(0, Math.min(60, ws));

    // 시각
    let ts = Math.floor(Date.now()/1000);
    if (iTM>=0) { const t = parseTs(r[iTM]); if (t) ts = t; }

    const at = apparentTempC(tC, rh, wMs);
    // 극저온에서는 WC가 더 보수적 → 더 낮은 값 채택(선택)
    const feels = (tC<=0 && wMs>1.5) ? Math.min(at, windChillC(tC, wMs)) : at;

    return { tC, rh, wMs, feels, ts, latency };
  }
  throw new Error("ASOS: no valid row (TA/RH/WS)");
}

// ---------- 메인 ----------
(async () => {
  const stn = (env.ASOS_STN || "108").trim();
  const loc = env.LOC || "seoul";

  const { tC, rh, wMs, feels, ts, latency } = await fetchLatestASOS(stn);

  // 신선도 가드: 관측시각이 3h 초과로 오래되면 쓰기 스킵
  const now = Math.floor(Date.now()/1000);
  if (ts < now - 3*3600) {
    console.warn(`Skip write: stale obs ts=${ts}, now=${now}`);
    // 그래도 가용성 로그는 남김
    await writeLP([`api_probe,service=asos_feels,env=prod,loc=${loc} success=0i,latency_ms=${latency}i ${now}`]);
    return;
  }

  const lp = [
    `life_index,source=kmahub-asos,loc=${loc},stn=${stn} feels_c=${feels.toFixed(2)},temp_c=${tC},rh_pct=${rh},wind_ms=${wMs} ${ts}`,
    `api_probe,service=asos_feels,env=prod,loc=${loc} success=1i,latency_ms=${latency}i ${now}`,
  ];
  await writeLP(lp);
  console.log(`Feels(AT)=${feels.toFixed(2)}C, Temp=${tC}C, RH=${rh}%, Wind=${wMs}m/s @ stn=${stn}\nInflux write OK`);
})().catch(e => { console.error(e); process.exit(1); });

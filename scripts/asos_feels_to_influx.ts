/**
 * KMAHub ASOS(kma_sfctm2.php) → 체감온도(Heat Index/Wind Chill) → InfluxDB
 *
 * 실행(로컬):  npx ts-node scripts/asos_feels_to_influx.ts
 * ENV:
 *   INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET
 *   APIHUB_BASE=https://apihub.kma.go.kr
 *   APIHUB_KEY=<authKey>
 *   ASOS_STN=108
 *   LOC=seoul (선택)
 * DEBUG:
 *   PowerShell: $env:DEBUG_ASOS = "1"
 *   Linux/mac : DEBUG_ASOS=1
 */

type Env = {
  INFLUX_URL: string; INFLUX_TOKEN: string; INFLUX_ORG: string; INFLUX_BUCKET: string;
  APIHUB_BASE: string; APIHUB_KEY: string; ASOS_STN?: string; LOC?: string;
};
const env = process.env as unknown as Env;
const need = (k: keyof Env) => { const v = env[k]; if (!v) throw new Error(`Missing env: ${k}`); return v; };
const DBG = !!process.env.DEBUG_ASOS;

/* ---------- 유틸 ---------- */
function splitCSVLine(line: string): string[] {
  const out: string[] = []; let cur = ""; let q = false;
  for (let i=0;i<line.length;i++){
    const c=line[i];
    if (c === '"') { if (q && line[i+1] === '"'){ cur+='"'; i++; } else q = !q; }
    else if (c === "," && !q) { out.push(cur); cur = ""; }
    else cur += c;
  }
  out.push(cur);
  return out.map(s=>s.trim());
}
const toLines = (t: string) =>
  t.replace(/\ufeff/g,"").split(/\r?\n/).map(s=>s.replace(/\s+$/,"")).filter(l=>l.trim().length>0);
function splitBy(line: string, mode: "csv"|"ws"){ const s=line.replace(/^#\s*/,"").trim(); return mode==="csv"? splitCSVLine(s) : s.split(/\s+/); }
function toNum(s?: string){ const n=parseFloat(String(s??"").replace(/,/g,"")); return !isFinite(n) || n <= -8.9 ? NaN : n; }

function parseKST12(raw: string): number | null {
  if (/^\d{12,14}$/.test(raw)) {
    const yyyy=raw.slice(0,4), MM=raw.slice(4,6), dd=raw.slice(6,8), HH=raw.slice(8,10), mm=raw.slice(10,12), ss=(raw.slice(12,14)||"00");
    const iso = `${yyyy}-${MM}-${dd}T${HH}:${mm}:${ss}+09:00`;
    const d = new Date(iso); if (!isNaN(d.getTime())) return Math.floor(d.getTime()/1000);
  }
  return null;
}
function median(a:number[]){ if(!a.length) return NaN; const b=[...a].sort((x,y)=>x-y); const m=Math.floor(b.length/2); return b.length%2? b[m] : (b[m-1]+b[m])/2; }
function bestIndex(rows:number[][], ok:(v:number)=>boolean, score?:(vals:number[])=>number){
  const cols=rows[0]?.length ?? 0; let best=-1, s=-1;
  for(let c=0;c<cols;c++){
    const vals=rows.map(r=>r[c]).filter(isFinite); if(!vals.length) continue;
    const okCnt=vals.filter(ok).length; if(okCnt===0) continue;
    const sc = score? score(vals) : okCnt;
    if (sc > s){ s=sc; best=c; }
  }
  return best;
}

/* ---------- 체감온도 ---------- */
// Rothfusz °F → °C
function heatIndexC(tC:number, rh:number){
  const T=tC*9/5+32, R=rh;
  const HI=-42.379+2.04901523*T+10.14333127*R-0.22475541*T*R
          -0.00683783*T*T -0.05481717*R*R +0.00122874*T*T*R
          +0.00085282*T*R*R -0.00000199*T*T*R*R;
  return (HI-32)*5/9;
}
// Wind Chill (C, m/s → km/h)
function windChillC(tC:number, vMs:number){
  const v=vMs*3.6; if (tC>10 || v<=4.8) return tC;
  return 13.12 + 0.6215*tC - 11.37*Math.pow(v,0.16) + 0.3965*tC*Math.pow(v,0.16);
}
// Magnus식 RH from T/Td
function rhFromTd(tC:number, tdC:number){
  const a=17.625, b=243.04;
  const gamma = (a*tdC)/(b+tdC) - (a*tC)/(b+tC);
  const rh = 100 * Math.exp(gamma);
  return Math.max(0, Math.min(100, rh));
}

/* ---------- Influx ---------- */
async function writeLP(lines: string[]){
  const url = `${need("INFLUX_URL")}/api/v2/write?org=${encodeURIComponent(need("INFLUX_ORG"))}&bucket=${encodeURIComponent(need("INFLUX_BUCKET"))}&precision=s`;
  const res = await fetch(url, {
    method:"POST",
    headers:{ Authorization:`Token ${need("INFLUX_TOKEN")}`, "Content-Type":"text/plain; charset=utf-8" },
    body: lines.join("\n")
  });
  if (!res.ok) throw new Error(`Influx write ${res.status}: ${await res.text()}`);
}

/* ---------- 헤더 토큰 ---------- */
function headerTokens(commentLines: string[], rowLen: number): string[] | null {
  for (const line of commentLines) {
    const s=line.replace(/^#\s*/,"").trim();
    if (!/(TM|TIME|DATE)/i.test(s)) continue;
    if (!/(TA|TEMP|기온)/i.test(s)) continue;
    if (!/(HM|REH|RH|습도)/i.test(s)) continue;
    if (!/(WS|WSD|WIND|풍속)/i.test(s)) continue;
    const toks=s.split(/\s+/);
    if (toks.length===rowLen) return toks;
  }
  return null;
}
function idxByName(toks:string[]|null, re:RegExp){ return toks ? toks.findIndex(t=>re.test(t)) : -1; }

/* ---------- ASOS 수집 & 파싱 ---------- */
async function fetchLatestASOS(stn: string){
  const now=new Date();
  const tm2 = now.toISOString().replace(/[-:]/g,"").slice(0,12)+"00";
  const tm1 = new Date(now.getTime()-3*3600*1000).toISOString().replace(/[-:]/g,"").slice(0,12)+"00";

  const base=need("APIHUB_BASE");
  const url = `${base}/api/typ01/url/kma_sfctm2.php?stn=${encodeURIComponent(stn)}&tm1=${tm1}&tm2=${tm2}&disp=1&help=1&authKey=${encodeURIComponent(need("APIHUB_KEY"))}`;

  const t0=Date.now();
  const res=await fetch(url);
  const latency=Date.now()-t0;
  const text=await res.text();
  if (!res.ok) throw new Error(`ASOS ${res.status}: ${text.slice(0,200)}`);

  const lines=toLines(text);
  const comments=lines.filter(l=>l.startsWith("#"));
  const data=lines.filter(l=>!l.startsWith("#"));
  if (!data.length) throw new Error("ASOS: no data rows");

  const mode: "csv"|"ws" = data[0].includes(",") ? "csv" : "ws";
  const rowsS = data.map(l=>splitBy(l, mode)).filter(r=>r.length>=5);
  const rowsN: number[][] = rowsS.map(r=>r.map(toNum));
  const toks = headerTokens(comments, rowsS[0].length);

  // 1차: 이름 매칭
  let iTM = idxByName(toks, /^(TM|YYMMDDHHMI|DATE|TIME)$/i);
  let iTA = idxByName(toks, /^(TA|TEMP|기온)$/i);
  let iHM = idxByName(toks, /^(HM|REH|RH|습도)$/i);
  let iWS = idxByName(toks, /^(WS|WSD|WIND|풍속)$/i);
  let iTD = idxByName(toks, /^(TD|DEW|이슬점)$/i);

  // 2차: 분포 기반 보정
  const preferRH = (vals:number[]) => median(vals); // RH는 보통 중앙값이 큼
  if (iTA<0) iTA = bestIndex(rowsN, v=>v>-50 && v<50);
  if (iWS<0) iWS = bestIndex(rowsN, v=>v>=0 && v<=60, vals=>vals.filter(v=>v>1.0).length);
  if (iHM<0) iHM = bestIndex(rowsN, v=>v>=0 && v<=100, preferRH);
  if (iTM<0){ const last=rowsS.at(-1)!; iTM = last.findIndex(v=>/^\d{12,14}$/.test(String(v))); }
  if (iTD<0){ // Td는 온도와 같은 범위
    iTD = bestIndex(rowsN, v=>v>-50 && v<50, vals => -median(vals)); // Td는 보통 TA보다 낮음 → 중앙값 작은 열을 선호
  }

  if (DBG) {
    console.log("TOKS:", toks);
    console.log("IDX iTM/iTA/iHM/iWS/iTD:", iTM, iTA, iHM, iWS, iTD);
    console.log("SAMPLE(last):", rowsS.at(-1));
  }

  if (iTA<0 || iWS<0) throw new Error("Required columns not found (TA/WS)");

  // 최신 유효행 역탐색
  for (let k=rowsS.length-1; k>=0; k--){
    const raw=rowsS[k], num=rowsN[k];
    const tC = num[iTA], wMs = num[iWS];
    if (!isFinite(tC) || !isFinite(wMs)) continue;

    // RH 우선 얻기
    let rh = isFinite(num[iHM]) ? num[iHM] : NaN;

    // Td 기반 RH 대체 후보
    let rhTd = NaN;
    if (iTD>=0 && isFinite(num[iTD])) {
      const td=num[iTD];
      if (isFinite(td)) rhTd = rhFromTd(tC, td);
    }

    // 따뜻한 시간에서 RH가 말이 안 되면(T>=18 & RH<20) Td기반으로 대체
    if ((tC>=18 && (!isFinite(rh) || rh<20 || rh>100)) && isFinite(rhTd)) rh = rhTd;

    // 범위 클램프
    if (!isFinite(rh) || rh<0 || rh>100) continue;

    // 시각
    let ts = Math.floor(Date.now()/1000);
    if (iTM>=0){ const t=parseKST12(String(raw[iTM]??"")); if (t) ts=t; }

    // 체감온도 계산
    let feels = (tC>=27 && rh>=40) ? heatIndexC(tC, rh)
              : (tC<=10 && wMs>1.34) ? windChillC(tC, wMs)
              : tC;

    // 품질 가드: 비상식이면 Td 기반으로 재시도
    if (Math.abs(feels - tC) > 12 && isFinite(rhTd)) {
      const feels2 = (tC>=27 && rhTd>=40) ? heatIndexC(tC, rhTd)
                    : (tC<=10 && wMs>1.34) ? windChillC(tC, wMs)
                    : tC;
      if (Math.abs(feels2 - tC) < Math.abs(feels - tC)) {
        if (DBG) console.log("Recalc feels with Td-based RH:", { old:feels, new:feels2, rhOld:rh, rhTd });
        rh = rhTd; feels = feels2;
      }
    }

    if (DBG) console.log({ pickedRow: raw, idx:{iTM,iTA,iHM,iWS,iTD}, vals:{tC, rh, wMs, feels:+feels.toFixed(2)} });
    return { tC, rh, wMs, feels, ts, latency };
  }

  throw new Error("ASOS: no valid row after scanning");
}

/* ---------- main ---------- */
(async () => {
  const stn = (env.ASOS_STN || "108").trim();
  const loc = env.LOC || "seoul";

  const { tC, rh, wMs, feels, ts, latency } = await fetchLatestASOS(stn);

  const now = Math.floor(Date.now()/1000);
  const lines = [
    `life_index,source=kmahub-asos,loc=${loc},stn=${stn} temp_c=${tC},rh_pct=${rh},wind_ms=${wMs},feels_c=${+feels.toFixed(2)} ${ts}`,
    `api_probe,service=asos_feels,env=prod,loc=${loc} success=1i,latency_ms=${latency}i ${now}`,
  ];
  await writeLP(lines);

  console.log(`FeelsLike=${feels.toFixed(2)}C, Temp=${tC}C, RH=${rh}%, Wind=${wMs}m/s @ stn=${stn}`);
  console.log("Influx write OK");
})().catch(e => { console.error(e); process.exit(1); });

/**
 * KMAHub ASOS(kma_sfctm2.php) → 체감온도 → InfluxDB
 * ENV: INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET
 *      APIHUB_BASE=https://apihub.kma.go.kr, APIHUB_KEY=<authKey>
 *      ASOS_STN=108, LOC=seoul(옵션)
 * DEBUG: PowerShell $env:DEBUG_ASOS="1" / bash DEBUG_ASOS=1
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
function clamp(v:number,min:number,max:number){ return Math.max(min, Math.min(max, v)); }

/* ---------- 시간 처리 ---------- */
function parseKST12(raw: string): number | null {
  if (!raw) return null;
  const s = String(raw).trim();
  if (/^\d{12,14}$/.test(s)) {
    const yyyy=s.slice(0,4), MM=s.slice(4,6), dd=s.slice(6,8),
          HH=s.slice(8,10), mm=s.slice(10,12), ss=(s.slice(12,14)||"00");
    const iso = `${yyyy}-${MM}-${dd}T${HH}:${mm}:${ss}+09:00`;
    const d = new Date(iso); if (!isNaN(d.getTime())) return Math.floor(d.getTime()/1000);
  }
  return null;
}
function ymdhmKST(dUTC: Date){
  const k = new Date(dUTC.getTime() + 9*3600*1000);
  const yyyy = k.getUTCFullYear().toString();
  const MM   = String(k.getUTCMonth()+1).padStart(2,"0");
  const dd   = String(k.getUTCDate()).padStart(2,"0");
  const HH   = String(k.getUTCHours()).padStart(2,"0");
  const mm   = String(k.getUTCMinutes()).padStart(2,"0");
  return `${yyyy}${MM}${dd}${HH}${mm}`;
}

/* ---------- 체감온도/습도 ---------- */
function heatIndexC(tC:number, rh:number){
  const T=tC*9/5+32, R=rh;
  const HI=-42.379+2.04901523*T+10.14333127*R-0.22475541*T*R
          -0.00683783*T*T -0.05481717*R*R +0.00122874*T*T*R
          +0.00085282*T*R*R -0.00000199*T*T*R*R;
  return (HI-32)*5/9;
}
function windChillC(tC:number, vMs:number){
  const v=vMs*3.6; if (tC>10 || v<=4.8) return tC;
  return 13.12 + 0.6215*tC - 11.37*Math.pow(v,0.16) + 0.3965*tC*Math.pow(v,0.16);
}
function rhFromTd(tC:number, tdC:number){
  const a=17.625, b=243.04;
  const gamma = (a*tdC)/(b+tdC) - (a*tC)/(b+tC);
  return clamp(100 * Math.exp(gamma), 0, 100);
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

/* ---------- 헤더 토큰(있으면 사용) ---------- */
function headerTokens(commentLines: string[], rowLen: number): string[] | null {
  for (const line of commentLines) {
    const s=line.replace(/^#\s*/,"").trim();
    if (!/(TM|TIME|DATE)/i.test(s)) continue;
    const toks=s.split(/\s+/);
    if (toks.length===rowLen) return toks;
  }
  return null;
}
function idxByName(toks:string[]|null, re:RegExp){ return toks ? toks.findIndex(t=>re.test(t)) : -1; }

/* ---------- ASOS 수집 & 파싱 ---------- */
async function fetchLatestASOS(stn: string){
  // KST 기준 최근 3시간 창
  const nowUTC = new Date();
  const tm2 = ymdhmKST(nowUTC) + "00";
  const tm1 = ymdhmKST(new Date(nowUTC.getTime() - 3*3600*1000)) + "00";

  const base=need("APIHUB_BASE");
  const key = need("APIHUB_KEY");
  const url = `${base}/api/typ01/url/kma_sfctm2.php?stn=${encodeURIComponent(stn)}&tm1=${tm1}&tm2=${tm2}&disp=1&help=1&authKey=${encodeURIComponent(key)}`;

  const t0=Date.now();
  const res=await fetch(url);
  const latency=Date.now()-t0;
  const text=await res.text();
  if (!res.ok) throw new Error(`ASOS ${res.status}: ${text.slice(0,200)}`);

  if (DBG) { console.log("---- ASOS DEBUG url (masked) ----"); console.log(url.replace(key, "***")); }

  const lines=toLines(text);
  const comments=lines.filter(l=>l.startsWith("#"));
  const data=lines.filter(l=>!l.startsWith("#"));
  if (!data.length) throw new Error("ASOS: no data rows");

  const mode: "csv"|"ws" = data[0].includes(",") ? "csv" : "ws";
  const rowsS = data.map(l=>splitBy(l, mode)).filter(r=>r.length>=5);
  const rowsN: number[][] = rowsS.map(r=>r.map(toNum));
  const toks = headerTokens(comments, rowsS[0].length);

  // 1차: 이름 매칭(가능할 때만)
  let iTM = idxByName(toks, /^(TM|YYMMDDHHMI|DATE|TIME)$/i);
  let iTA = idxByName(toks, /^(TA|TEMP|기온)$/i);
  let iHM = idxByName(toks, /^(HM|REH|RH|습도)$/i);
  let iWS = idxByName(toks, /^(WS|WSD|WIND|풍속)$/i);
  let iTD = idxByName(toks, /^(TD|DEW|이슬점)$/i);

  // 2차: 분포 기반 보정(헤더가 맞지 않을 때)
  const cols = rowsS[0].length;
  const pickBy = (ok:(v:number)=>boolean) => {
    let best=-1, cntBest=-1;
    for (let c=0;c<cols;c++){
      let cnt=0; for (const r of rowsN){ const v=r[c]; if (isFinite(v) && ok(v)) cnt++; }
      if (cnt>cntBest){ cntBest=cnt; best=c; }
    }
    return best;
  };

  if (iTA<0) iTA = pickBy(v=>v>-50 && v<50);
  if (iWS<0) iWS = pickBy(v=>v>=0 && v<=60);
  if (iHM<0) iHM = pickBy(v=>v>=0 && v<=100);
  if (iTM<0){
    // 모든 열에서 yyyyMMddHHmm 패턴 최다 출현 열 선택
    let best=-1, cntBest=-1;
    for (let c=0;c<cols;c++){
      let cnt=0; for (const r of rowsS){ if (parseKST12(String(r[c]))){ cnt++; } }
      if (cnt>cntBest){ cntBest=cnt; best=c; }
    }
    iTM = best;
  }
  if (iTD<0) {
    // Td는 보통 TA보다 작다 → TA가 있어야 동작
    if (iTA>=0){
      let best=-1, score=-1;
      for (let c=0;c<cols;c++){
        if (c===iTA) continue;
        let ok=0, seen=0;
        for (let k=0;k<rowsN.length;k++){
          const td=rowsN[k][c], t=rowsN[k][iTA];
          if (!isFinite(td) || !isFinite(t)) continue;
          seen++; if (td <= t) ok++;
        }
        const fit = seen ? ok/seen : 0;
        if (fit>score){ score=fit; best=c; }
      }
      iTD = best;
    }
  }

  if (DBG) {
    console.log("IDX iTM/iTA/iHM/iWS/iTD:", iTM, iTA, iHM, iWS, iTD);
    console.log("SAMPLE(last):", rowsS.at(-1));
  }

  if (iTA<0 || iWS<0 || iTM<0) throw new Error("Required columns not found (TM/TA/WS)");

  // 모든 행에 대해 ts 계산 → 최신(ts 최대) 1건만 사용
  type RowPick = { ts:number, tC:number, td?:number, rhHM?:number, wMs:number, raw:string[] };
  const picks: RowPick[] = [];
  for (let k=0;k<rowsS.length;k++){
    const raw=rowsS[k], num=rowsN[k];
    const tC = num[iTA], wMs = num[iWS];
    const ts = parseKST12(String(raw[iTM]??"")) ?? NaN;
    const td = iTD>=0 ? rowsN[k][iTD] : NaN;
    const rhHM = iHM>=0 ? rowsN[k][iHM] : NaN;
    if (isFinite(ts) && isFinite(tC) && isFinite(wMs)){
      picks.push({ ts, tC, td: isFinite(td)? td : undefined, rhHM: isFinite(rhHM)? rhHM : undefined, raw });
    }
  }
  if (!picks.length) throw new Error("ASOS: no parsable timestamp rows");

  picks.sort((a,b)=>a.ts-b.ts);
  const latest = picks[picks.length-1];

  // RH 우선순위: Td 기반 → HM 기반
  let rh = NaN;
  if (isFinite(latest.td as number)) {
    rh = rhFromTd(latest.tC, latest.td as number);
  } else if (isFinite(latest.rhHM as number)) {
    const v = latest.rhHM as number;
    if (v >= 5 && v <= 100) rh = v;
  }

  // RH가 여전히 비정상이면 실패 처리(스킵)
  if (!isFinite(rh)) {
    if (DBG) console.log("Skip: RH not usable", { latest });
    throw new Error("ASOS: no usable RH (TD/HM)");
  }

  // 체감온도 계산
  let feels = (latest.tC>=27 && rh>=40) ? heatIndexC(latest.tC, rh)
            : (latest.tC<=10 && latest.wMs>1.34) ? windChillC(latest.tC, latest.wMs)
            : latest.tC;

  // 과도한 차이 보정(혹시 HM이 있었고 Td 기반이 더 합리적일 때)
  if (isFinite(latest.td as number)) {
    const rhTd = rhFromTd(latest.tC, latest.td as number);
    const alt  = (latest.tC>=27 && rhTd>=40) ? heatIndexC(latest.tC, rhTd)
               : (latest.tC<=10 && latest.wMs>1.34) ? windChillC(latest.tC, latest.wMs)
               : latest.tC;
    if (Math.abs(alt - latest.tC) < Math.abs(feels - latest.tC)) {
      feels = alt; rh = rhTd;
    }
  }

  if (DBG) {
    console.log({ pickedRow: latest.raw, ts: latest.ts, tC: latest.tC, td: latest.td, rhHM: latest.rhHM, rhUsed: rh, wMs: latest.wMs, feels:+feels.toFixed(2), tm1, tm2 });
  }

  return { tC: latest.tC, rh, wMs: latest.wMs, feels: +(+feels).toFixed(2), ts: latest.ts, latency };
}

/* ---------- main ---------- */
(async () => {
  const stn = (env.ASOS_STN || "108").trim();
  const loc = env.LOC || "seoul";
  const { tC, rh, wMs, feels, ts, latency } = await fetchLatestASOS(stn);

  const now = Math.floor(Date.now()/1000);
  const lines = [
    `life_index,source=kmahub-asos,loc=${loc},stn=${stn} temp_c=${tC},rh_pct=${rh},wind_ms=${wMs},feels_c=${feels} ${ts}`,
    `api_probe,service=asos_feels,env=prod,loc=${loc} success=1i,latency_ms=${latency}i ${now}`,
  ];
  await writeLP(lines);

  console.log(`FeelsLike=${feels}C, Temp=${tC}C, RH=${rh}%, Wind=${wMs}m/s @ stn=${stn}`);
  console.log("Influx write OK");
})().catch(e => { console.error(e); process.exit(1); });

// ASOS 시간자료(kma_sfctm2) → 체감온도 계산 → Influx
type Env = {
  INFLUX_URL: string; INFLUX_TOKEN: string; INFLUX_ORG: string; INFLUX_BUCKET: string;
  APIHUB_BASE: string; APIHUB_KEY: string; ASOS_STN?: string; LOC?: string;
};
const env = process.env as unknown as Env;
const need = (k: keyof Env) => { const v = env[k]; if (!v) throw new Error(`Missing env: ${k}`); return v; };

function splitCSVLine(line: string) {
  const out: string[] = []; let cur = "", q = false;
  for (let i=0;i<line.length;i++){const c=line[i];
    if(c==='"'){ if(q && line[i+1]==='"'){cur+='"';i++;} else q=!q; }
    else if(c===',' && !q){ out.push(cur); cur=""; } else cur+=c;
  } out.push(cur); return out.map(s=>s.trim());
}
const toLines = (t:string)=>t.replace(/\ufeff/g,"").split(/\r?\n/).filter(Boolean);
const isNum = (s?:string)=>s!=null && /^-?\d+(\.\d+)?$/.test(s);

// Heat Index(F) → C (Rothfusz)
function heatIndexC(tC:number, rh:number){
  const T=tC*9/5+32, R=rh;
  const HI=-42.379+2.04901523*T+10.14333127*R-0.22475541*T*R-6.83783e-3*T*T-5.481717e-2*R*R+1.22874e-3*T*T*R+8.5282e-4*T*R*R-1.99e-6*T*T*R*R;
  return (HI-32)*5/9;
}
// Wind Chill(C, km/h)
function windChillC(tC:number, vMs:number){
  const v=vMs*3.6; // km/h
  if (tC>10 || v<=4.8) return tC;
  return 13.12 + 0.6215*tC - 11.37*Math.pow(v,0.16) + 0.3965*tC*Math.pow(v,0.16);
}

async function fetchLatestASOS(stn:string){
  // 최근 3시간 범위에서 최신 1건
  const now=new Date();
  const tm2 = now.toISOString().replace(/[-:]/g,"").slice(0,12)+"00"; // YYYYMMDDHHMM
  const tm1 = new Date(now.getTime()-3*3600*1000).toISOString().replace(/[-:]/g,"").slice(0,12)+"00";
  const url = `${need("APIHUB_BASE")}/api/typ01/url/kma_sfctm2.php?stn=${stn}&tm1=${tm1}&tm2=${tm2}&disp=1&help=1&authKey=${need("APIHUB_KEY")}`;
  const t0=Date.now(); const res=await fetch(url); const latency=Date.now()-t0; const text=await res.text();
  if(!res.ok) throw new Error(`ASOS ${res.status}: ${text.slice(0,200)}`);
  const lines = toLines(text).filter(l=>!l.startsWith("#"));
  const header = splitCSVLine(lines[0]);
  const rows = lines.slice(1).map(splitCSVLine).filter(r=>r.length>=header.length);
  // 컬럼 인덱스
  const iTA = header.findIndex(h=>/^TA$/i.test(h));      // 기온(°C)
  const iHM = header.findIndex(h=>/^HM$/i.test(h));      // 습도(%)
  const iWS = header.findIndex(h=>/^WS$/i.test(h));      // 풍속(m/s)
  const iTM = header.findIndex(h=>/(TM|time|tm|date)/i.test(h));
  if (iTA<0||iHM<0||iWS<0) { console.log("Header",header); throw new Error("Required columns not found (TA/HM/WS)"); }
  // 최신행(끝에서부터 유효값 탐색)
  for (let k=rows.length-1;k>=0;k--){
    const r = rows[k];
    if (isNum(r[iTA]) && isNum(r[iHM]) && isNum(r[iWS])) {
      const tC=parseFloat(r[iTA]!), rh=parseFloat(r[iHM]!), wMs=parseFloat(r[iWS]!);
      let feels=tC;
      if (tC>=27 && rh>=40) feels = heatIndexC(tC,rh);
      else if (tC<=10 && wMs>1.34) feels = windChillC(tC,wMs);
      let ts = Math.floor(Date.now()/1000);
      if (iTM>=0) {
        const raw = (r[iTM]||"").replace(" ","T");
        const d = new Date(/\+/.test(raw)?raw:raw+"+09:00");
        if (!isNaN(d.getTime())) ts = Math.floor(d.getTime()/1000);
      }
      return {tC,rh,wMs,feels,ts,latency,url};
    }
  }
  throw new Error("No valid ASOS row in range");
}

async function writeLP(lines:string[]){
  const url = `${need("INFLUX_URL")}/api/v2/write?org=${encodeURIComponent(need("INFLUX_ORG"))}&bucket=${encodeURIComponent(need("INFLUX_BUCKET"))}&precision=s`;
  const res = await fetch(url,{method:"POST",headers:{Authorization:`Token ${need("INFLUX_TOKEN")}`,"Content-Type":"text/plain; charset=utf-8"},body:lines.join("\n")});
  if(!res.ok) throw new Error(`Influx write ${res.status}: ${await res.text()}`);
}

(async()=>{
  const stn = (env.ASOS_STN||"108").trim();
  const loc = env.LOC||"seoul";
  const {tC,rh,wMs,feels,ts,latency,url} = await fetchLatestASOS(stn);
  const now = Math.floor(Date.now()/1000);
  const lines = [
    `life_index,source=kmahub-asos,loc=${loc},stn=${stn} feels_c=${feels.toFixed(2)},temp_c=${tC},rh_pct=${rh},wind_ms=${wMs} ${ts}`,
    `api_probe,service=asos_feels,env=prod,loc=${loc} success=1i,latency_ms=${latency}i ${now}`
  ];
  await writeLP(lines);
  console.log(`FeelsLike=${feels.toFixed(2)}C @ stn=${stn}\nfrom: ${url}\nInflux write OK`);
})().catch(e=>{ console.error(e); process.exit(1); });
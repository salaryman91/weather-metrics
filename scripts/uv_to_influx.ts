// 자외선관측(kma_sfctm_uv) → Influx
type Env = {
  INFLUX_URL:string; INFLUX_TOKEN:string; INFLUX_ORG:string; INFLUX_BUCKET:string;
  APIHUB_BASE:string; APIHUB_KEY:string; UV_STN?:string; LOC?:string;
};
const env = process.env as unknown as Env;
const need=(k:keyof Env)=>{ const v=env[k]; if(!v) throw new Error(`Missing env: ${k}`); return v; };

function splitCSVLine(line:string){ const out:string[]=[]; let cur="",q=false;
  for(let i=0;i<line.length;i++){const c=line[i];
    if(c==='"'){ if(q&&line[i+1]==='"'){cur+='"';i++;} else q=!q; }
    else if(c===',' && !q){ out.push(cur); cur=""; } else cur+=c; }
  out.push(cur); return out.map(s=>s.trim());
}
const toLines=(t:string)=>t.replace(/\ufeff/g,"").split(/\r?\n/).filter(Boolean);

async function fetchLatestUV(stn:string){
  // 최근 하루 범위
  const now = new Date();
  const tm2 = now.toISOString().replace(/[-:]/g,"").slice(0,12)+"00";
  const tm1 = new Date(now.getTime()-24*3600*1000).toISOString().replace(/[-:]/g,"").slice(0,12)+"00";
  const url = `${need("APIHUB_BASE")}/api/typ01/url/kma_sfctm_uv.php?stn=${stn}&tm1=${tm1}&tm2=${tm2}&disp=1&help=1&authKey=${need("APIHUB_KEY")}`;
  const t0=Date.now(); const res=await fetch(url); const latency=Date.now()-t0; const text=await res.text();
  if(!res.ok) throw new Error(`UV ${res.status}: ${text.slice(0,200)}`);
  const lines = toLines(text).filter(l=>!l.startsWith("#"));
  const header = splitCSVLine(lines[0]);
  const rows = lines.slice(1).map(splitCSVLine).filter(r=>r.length>=header.length);
  // UV 컬럼 찾기
  let iUV = header.findIndex(h=>/uv[-\s_]*b|uv[-\s_]*index|uv/i.test(h));
  if (iUV<0) {
    // 패턴으로 추정(0~15 범위의 수치)
    outer: for (let c=0;c<header.length;c++){
      for (const r of rows.slice(-10)){
        const v = parseFloat(r[c]); if (!isFinite(v) || v<0 || v>20) continue outer;
      }
      iUV=c; break;
    }
  }
  if (iUV<0) { console.log("Header",header); throw new Error("UV column not found"); }
  const iTM = header.findIndex(h=>/(TM|time|tm|date)/i.test(h));
  // 최신 유효행
  for (let k=rows.length-1;k>=0;k--){
    const r = rows[k]; const v = parseFloat(r[iUV]); if (!isFinite(v)) continue;
    let ts = Math.floor(Date.now()/1000);
    if (iTM>=0){ const raw=(r[iTM]||"").replace(" ","T"); const d=new Date(/\+/.test(raw)?raw:raw+"+09:00"); if(!isNaN(d.getTime())) ts=Math.floor(d.getTime()/1000); }
    return {uv:v, ts, latency, url};
  }
  throw new Error("No valid UV row");
}

async function writeLP(lines:string[]){
  const url = `${need("INFLUX_URL")}/api/v2/write?org=${encodeURIComponent(need("INFLUX_ORG"))}&bucket=${encodeURIComponent(need("INFLUX_BUCKET"))}&precision=s`;
  const res = await fetch(url,{method:"POST",headers:{Authorization:`Token ${need("INFLUX_TOKEN")}`,"Content-Type":"text/plain; charset=utf-8"},body:lines.join("\n")});
  if(!res.ok) throw new Error(`Influx write ${res.status}: ${await res.text()}`);
}

(async()=>{
  const stn = (env.UV_STN||"").trim(); if(!stn) throw new Error("UV_STN 미설정: Actions Variables에 UV_STN을 넣어주세요.");
  const loc = env.LOC||"seoul";
  const {uv,ts,latency,url} = await fetchLatestUV(stn);
  const now = Math.floor(Date.now()/1000);
  const lines = [
    `life_index,source=kmahub-uv,loc=${loc},stn=${stn} uv_idx=${uv} ${ts}`,
    `api_probe,service=uv_obs,env=prod,loc=${loc} success=1i,latency_ms=${latency}i ${now}`
  ];
  await writeLP(lines);
  console.log(`UV=${uv} @ stn=${stn}\nfrom: ${url}\nInflux write OK`);
})().catch(e=>{ console.error(e); process.exit(1); });
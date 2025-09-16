/**
 * ASOS 시간자료(kma_sfctm2.php) → 체감온도 계산 → InfluxDB 적재
 * - 표 테두리 제거/헤더 강화
 * - 열 중복 방지(서로 다른 열 보장)
 * - 현실성 검증(이상치 방어)
 *
 * 실행(로컬):  npx ts-node scripts/asos_feels_to_influx.ts
 * 필요 ENV: INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET, APIHUB_BASE, APIHUB_KEY, ASOS_STN, LOC
 */

type Env = {
  INFLUX_URL: string; INFLUX_TOKEN: string; INFLUX_ORG: string; INFLUX_BUCKET: string;
  APIHUB_BASE: string; APIHUB_KEY: string; ASOS_STN?: string; LOC?: string;
};
const env = process.env as unknown as Env;
const need = (k: keyof Env) => { const v = env[k]; if (!v) throw new Error(`Missing env: ${k}`); return v; };
const DBG = !!process.env.DEBUG_ASOS;

// ---------- utils ----------
function splitCSVLine(line: string): string[] {
  const out: string[] = []; let cur = ""; let q = false;
  for (let i=0;i<line.length;i++){ const c=line[i];
    if(c==='"'){ if(q && line[i+1]==='"'){ cur+='"'; i++; } else { q=!q; } }
    else if(c==="," && !q){ out.push(cur); cur=""; }
    else cur+=c;
  } out.push(cur); return out.map(s=>s.trim());
}
function toLines(t: string): string[] {
  return t.replace(/\ufeff/g,"").split(/\r?\n/).filter(l=>l.trim().length>0);
}
function splitBy(line: string, mode: "csv"|"ws"): string[] {
  const s = line.replace(/^#\s*/, "").trim();
  return mode==="csv" ? splitCSVLine(s) : s.split(/\s+/);
}
function stripTableDecor(lines: string[]): string[] {
  return lines
    .map(l => l.replace(/[│┃┆┊]/g, " ").replace(/\s*\|\s*/g, " "))
    .map(l => l.replace(/\s{2,}/g, " ").trim())
    .filter(l => l.length>0);
}
function toNum(s?: string): number {
  const n = parseFloat(String(s ?? ""));
  return !isFinite(n) || n <= -8.9 ? NaN : n; // KMA 결측 -9/-999 등
}

// ---------- feels ----------
function heatIndexC(tC: number, rh: number): number {
  const T = tC * 9/5 + 32, R = rh;
  const HI = -42.379 + 2.04901523*T + 10.14333127*R - 0.22475541*T*R
    - 6.83783e-3*T*T - 5.481717e-2*R*R + 1.22874e-3*T*T*R
    + 8.5282e-4*T*R*R - 1.99e-6*T*T*R*R;
  return (HI - 32) * 5/9;
}
function windChillC(tC: number, vMs: number): number {
  const v = vMs * 3.6; // km/h
  if (tC > 10 || v <= 4.8) return tC;
  return 13.12 + 0.6215*tC - 11.37*Math.pow(v,0.16) + 0.3965*tC*Math.pow(v,0.16);
}
function plausibleFeels(tC:number, rh:number, wMs:number, feels:number): boolean {
  if (!isFinite(tC) || !isFinite(rh) || !isFinite(wMs) || !isFinite(feels)) return false;
  if (tC < -40 || tC > 50) return false;
  if (rh < 1 || rh > 100) return false;
  if (wMs < 0 || wMs > 50) return false;

  if (tC <= 10) {
    // 윈드칠: 체감은 기온 이하, 너무 과한 하강은 배제(≤20°C)
    if (!(feels <= tC && tC - feels <= 20)) return false;
  } else if (tC >= 27 && rh >= 40) {
    // 더위지수: 체감은 기온 이상, 과한 상승 배제(≤15°C)
    if (!(feels >= tC && feels - tC <= 15)) return false;
  } else {
    // 보통 구간: ±8°C 이내
    if (Math.abs(feels - tC) > 8) return false;
  }
  return true;
}

// ---------- influx ----------
async function writeLP(lines: string[]) {
  const url = `${need("INFLUX_URL")}/api/v2/write?org=${encodeURIComponent(need("INFLUX_ORG"))}&bucket=${encodeURIComponent(need("INFLUX_BUCKET"))}&precision=s`;
  const res = await fetch(url, {
    method:"POST",
    headers:{ Authorization:`Token ${need("INFLUX_TOKEN")}`, "Content-Type":"text/plain; charset=utf-8" },
    body: lines.join("\n")
  });
  if (!res.ok) throw new Error(`Influx write ${res.status}: ${await res.text()}`);
}

// ---------- fetch & parse ----------
function bestIndexExclude(numeric: number[][], ok:(v:number)=>boolean, exclude:Set<number>): number {
  if (!numeric.length) return -1;
  const cols = numeric[0].length;
  let best=-1, score=-1;
  for (let c=0;c<cols;c++){
    if (exclude.has(c)) continue;
    let cnt=0;
    for (const row of numeric) { const v=row[c]; if (isFinite(v) && ok(v)) cnt++; }
    if (cnt>score){ score=cnt; best=c; }
  }
  return best;
}

async function fetchLatestASOS(stn: string) {
  const now = new Date();
  const tm2 = now.toISOString().replace(/[-:]/g,"").slice(0,12)+"00";
  const tm1 = new Date(now.getTime()-3*3600*1000).toISOString().replace(/[-:]/g,"").slice(0,12)+"00";

  const base = need("APIHUB_BASE");
  const url = `${base}/api/typ01/url/kma_sfctm2.php?stn=${encodeURIComponent(stn)}&tm1=${tm1}&tm2=${tm2}&disp=1&help=1&authKey=${encodeURIComponent(need("APIHUB_KEY"))}`;

  const t0 = Date.now();
  const res = await fetch(url);
  const latency = Date.now()-t0;
  const text = await res.text();
  if (!res.ok) throw new Error(`ASOS ${res.status}: ${text.slice(0,200)}`);

  const linesAll = toLines(text);
  const comments = stripTableDecor(linesAll.filter(l=>l.trim().startsWith("#")));
  const data     = stripTableDecor(linesAll.filter(l=>l.trim() && !l.trim().startsWith("#")));

  // 헤더 탐색(관측시간/지점/기온/습도/풍속 키워드 모두 포함)
  const headerLine =
    [...comments].reverse().find(l =>
      /(YYMM|TM|TIME|DATE)\b.*\b(STN|지점)\b.*\b(TA|기온)\b.*\b(HM|RH|습도)\b.*\b(WS|풍속)\b/i.test(l)
    ) ?? null;

  let mode: "csv"|"ws" = headerLine && headerLine.includes(",") ? "csv" : "ws";
  if (!headerLine && data[0]?.includes(",")) mode = "csv";

  const header = headerLine ? splitBy(headerLine, mode) : [];
  const rows = data.map(l => splitBy(l, mode)).filter(r => r.length >= 5);
  if (!rows.length) throw new Error("ASOS: no data rows");

  // 1) 이름 기반 1차 매핑
  let iTA = header.findIndex(h => /^TA$/i.test(h) || /기온/i.test(h));
  let iHM = header.findIndex(h => /^HM$/i.test(h) || /(RH|습도)/i.test(h));
  let iWS = header.findIndex(h => /^WS$/i.test(h) || /(풍속|WIND)/i.test(h));
  let iTM = header.findIndex(h => /^(TM|TIME|DATE)$/i.test(h) || /시각|time/i.test(h));

  // 2) 값 기반 2차 매핑(중복 금지)
  const numeric = rows.map(r => r.map(toNum));
  const cols = rows[0].length;
  const used = new Set<number>();
  const okRH  = (v:number)=> v>=0 && v<=100;
  const okWS  = (v:number)=> v>=0 && v<=60;
  const okTA  = (v:number)=> v>-50 && v<50;

  if (iHM < 0) { iHM = bestIndexExclude(numeric, okRH, used); if (iHM>=0) used.add(iHM); }
  if (iTA < 0) {
    // HM 이웃 우선
    if (iHM>=0){
      for (const c of [iHM-1,iHM+1]) {
        if (c>=0 && c<cols && !used.has(c)) {
          const vals = numeric.map(r=>r[c]).filter(isFinite);
          if (vals.length>=10) {
            const avg = vals.reduce((a,b)=>a+b,0)/vals.length;
            if (okTA(avg)) { iTA=c; break; }
          }
        }
      }
    }
    if (iTA<0) { iTA = bestIndexExclude(numeric, okTA, used); }
    if (iTA>=0) used.add(iTA);
  }
  if (iWS < 0) { iWS = bestIndexExclude(numeric, okWS, used); if (iWS>=0) used.add(iWS); }

  // 3) 시간 인덱스 보조
  if (iTM < 0) {
    iTM = rows[0].findIndex(v => /^\d{12,14}$/.test(v));
  }

  // 4) 최종 검증 & 충돌 회피
  const uniq = new Set([iTA,iHM,iWS].filter(i=>i>=0)).size;
  if (iTA<0 || iHM<0 || iWS<0 || uniq<3) {
    if (DBG) {
      console.log("Header:", header);
      console.log("Sample row:", rows.at(-1));
      console.log("Index iTM/iTA/iHM/iWS:", iTM, iTA, iHM, iWS);
    }
    throw new Error("Required columns not found or collided (TA/HM/WS)");
  }

  // 5) 최신 유효 행 선택(뒤에서 앞으로)
  for (let k=rows.length-1;k>=0;k--){
    const r = rows[k];
    const tC = toNum(r[iTA]);
    const rh = toNum(r[iHM]);
    const wMs = toNum(r[iWS]);
    if (!isFinite(tC) || !isFinite(rh) || !isFinite(wMs)) continue;

    // 타임스탬프
    let ts = Math.floor(Date.now()/1000);
    if (iTM>=0) {
      const raw = r[iTM];
      if (/^\d{12,14}$/.test(raw)) {
        const yyyy=raw.slice(0,4), MM=raw.slice(4,6), dd=raw.slice(6,8), HH=raw.slice(8,10), mm=raw.slice(10,12), ss=(raw.slice(12,14)||"00");
        const d = new Date(`${yyyy}-${MM}-${dd}T${HH}:${mm}:${ss}+09:00`);
        if (!isNaN(d.getTime())) ts = Math.floor(d.getTime()/1000);
      }
    }

    // 체감온도
    const feels = (tC >= 27 && rh >= 40) ? heatIndexC(tC, rh)
                  : (tC <= 10 && wMs > 1.34) ? windChillC(tC, wMs)
                  : tC;

    const ok = plausibleFeels(tC, rh, wMs, feels);
    const feelsSafe = ok ? feels : tC;

    if (DBG) console.log({pickedRow:r, idx:{iTM,iTA,iHM,iWS}, values:{tC,rh,wMs,feels,feelsSafe,ts}});

    if (ok) return { tC, rh, wMs, feels: feelsSafe, ts, latency };
  }

  // 6) 마지막 안전 fallback: 가장 최근 행을 원기온 기반으로 저장
  const r = rows.at(-1)!;
  const tC = toNum(r[iTA]);
  const rh = Math.max(1, Math.min(100, toNum(r[iHM])));
  const wMs = Math.max(0, Math.min(50, toNum(r[iWS])));
  let ts = Math.floor(Date.now()/1000);
  if (iTM>=0 && /^\d{12,14}$/.test(rows.at(-1)![iTM])) {
    const raw = rows.at(-1)![iTM];
    const yyyy=raw.slice(0,4), MM=raw.slice(4,6), dd=raw.slice(6,8), HH=raw.slice(8,10), mm=raw.slice(10,12), ss=(raw.slice(12,14)||"00");
    const d = new Date(`${yyyy}-${MM}-${dd}T${HH}:${mm}:${ss}+09:00`);
    if (!isNaN(d.getTime())) ts = Math.floor(d.getTime()/1000);
  }
  const feels = (tC >= 27 && rh >= 40) ? heatIndexC(tC, rh)
                : (tC <= 10 && wMs > 1.34) ? windChillC(tC, wMs)
                : tC;
  const feelsSafe = plausibleFeels(tC,rh,wMs,feels) ? feels : tC;
  return { tC, rh, wMs, feels: feelsSafe, ts, latency };
}

// ---------- main ----------
(async () => {
  const stn = (env.ASOS_STN || "108").trim();
  const loc = env.LOC || "seoul";
  const { tC, rh, wMs, feels, ts, latency } = await fetchLatestASOS(stn);
  const now = Math.floor(Date.now()/1000);

  const lines = [
    `life_index,source=kmahub-asos,loc=${loc},stn=${stn} feels_c=${feels.toFixed(2)},temp_c=${tC},rh_pct=${rh},wind_ms=${wMs} ${ts}`,
    `api_probe,service=asos_feels,env=prod,loc=${loc} success=1i,latency_ms=${latency}i ${now}`
  ];
  await writeLP(lines);
  console.log(`FeelsLike=${feels.toFixed(2)}C, Temp=${tC}C, RH=${rh}%, Wind=${wMs}m/s @ stn=${stn}\nInflux write OK`);
})().catch(e => { console.error(e); process.exit(1); });

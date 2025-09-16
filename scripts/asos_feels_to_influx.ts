/**
 * ASOS 시간자료(kma_sfctm2.php) → 체감온도(Heat Index/Wind Chill) → InfluxDB Cloud
 *
 * 안정화 포인트
 * - help=1/disp=1, help=0/disp=0 모두 시도
 * - EUC-KR/UTF-8 자동 판별(iconv-lite)
 * - 헤더 매핑(우선) + 휴리스틱(보조)로 TA/RH/WS 정확도 강화
 * - 행 단위 sanity check: WS>30, RH<1 또는 RH>100, TA 비정상 시 스킵
 * - “마지막행 강제 채택” 제거 → 잘못된 값 주입 방지
 *
 * 디버그: PowerShell ->  $env:DEBUG_ASOS="1"; npx ts-node scripts/asos_feels_to_influx.ts
 *         bash       ->  DEBUG_ASOS=1 npx ts-node scripts/asos_feels_to_influx.ts
 */

import * as iconv from "iconv-lite";

type Env = {
  INFLUX_URL: string; INFLUX_TOKEN: string; INFLUX_ORG: string; INFLUX_BUCKET: string;
  APIHUB_BASE: string; APIHUB_KEY: string; ASOS_STN?: string; LOC?: string;
};
const env = process.env as unknown as Env;
const need = (k: keyof Env) => { const v = env[k]; if (!v) throw new Error(`Missing env: ${k}`); return v; };
const DBG = !!process.env.DEBUG_ASOS;

// ---------- 공통 유틸 ----------
function splitCSVLine(line: string): string[] {
  const out: string[] = []; let cur = ""; let q = false;
  for (let i=0;i<line.length;i++){
    const c = line[i];
    if (c === '"'){ if (q && line[i+1] === '"'){ cur+='"'; i++; } else { q=!q; } }
    else if (c === "," && !q){ out.push(cur); cur=""; }
    else cur += c;
  }
  out.push(cur);
  return out.map(s => s.trim());
}
const toLines = (t: string) =>
  t.replace(/\ufeff/g,"").split(/\r?\n/).filter(l => l.trim().length>0);

function splitBy(line: string, mode: "csv"|"ws"){ const s=line.replace(/^#\s*/,"").trim(); return mode==="csv"?splitCSVLine(s):s.split(/\s+/); }
const toNum = (s?: string) => { const n = parseFloat(String(s ?? "")); return !isFinite(n) || n <= -8.9 ? NaN : n; };
const looksStationCode = (v: number) => Number.isInteger(v) && v>=1 && v<10000;

// 인코딩 판별
async function decodeKR(res: Response): Promise<string> {
  const buf = Buffer.from(await res.arrayBuffer());
  const ct = (res.headers.get("content-type")||"").toLowerCase();
  if (/euc-?kr|ks_c_5601|cp949/.test(ct)) return iconv.decode(buf, "euc-kr");
  if (/utf-?8/.test(ct)) return buf.toString("utf8");
  const utf = buf.toString("utf8");
  return utf.includes("\uFFFD") ? iconv.decode(buf, "euc-kr") : utf;
}

// Influx write
async function writeLP(lines: string[]) {
  const url = `${need("INFLUX_URL")}/api/v2/write?org=${encodeURIComponent(need("INFLUX_ORG"))}&bucket=${encodeURIComponent(need("INFLUX_BUCKET"))}&precision=s`;
  const res = await fetch(url, { method:"POST", headers:{ Authorization:`Token ${need("INFLUX_TOKEN")}`, "Content-Type":"text/plain; charset=utf-8" }, body: lines.join("\n") });
  if (!res.ok) throw new Error(`Influx write ${res.status}: ${await res.text()}`);
}

// ---------- 헤더/인덱스 추출 ----------
function stripTableDecor(lines: string[]): string[] {
  const border = /^[\s|│┃┆┊\-─━┈┉┄┅=+]+$/;
  return lines
    .filter(l => !border.test(l))
    .map(l => l.replace(/[│┃┆┊]/g," ").replace(/\s*\|\s*/g," "));
}
function splitHeaderData(all: string[]) {
  const comments = all.filter(l => l.trim().startsWith("#"));
  const rest     = all.filter(l => !l.trim().startsWith("#"));
  return { comments, rest };
}
/** 데이터 열 수와 같은 토큰 라인을 찾아 열 이름 반환 */
function headerTokens(comments: string[], rowLen: number): string[]|null {
  for (const line of comments) {
    const s = line.replace(/^#\s*/,"").trim();
    if (!/(TM|TIME|DATE)/i.test(s)) continue;
    // ASOS: TA(기온), HM/REH/RH(습도), WS(풍속) 등이 섞여 옴
    if (!/(TA|기온)/i.test(s)) continue;
    if (!/(HM|REH|RH|습도)/i.test(s)) continue;
    if (!/(WS|풍속)/i.test(s)) continue;
    const toks = s.split(/\s+/);
    if (toks.length === rowLen) return toks;
  }
  return null;
}
function pickTimeIndex(rows: string[][]): { iTM:number; pair?:[number,number] } {
  if (!rows.length) return { iTM:-1 };
  let iTM = rows[0].findIndex(v => /^\d{12,14}$/.test(v));
  if (iTM>=0) return { iTM };
  const C = rows[0].length;
  for (let c=0;c<C-1;c++){
    const a=rows[0][c], b=rows[0][c+1];
    if (/^\d{8}$/.test(a) && /^\d{4}$/.test(b)) return { iTM:-1, pair:[c,c+1] };
  }
  return { iTM:-1 };
}
function parseTsKST(raw: string): number|null {
  if (/^\d{12,14}$/.test(raw)) {
    const yyyy=raw.slice(0,4), MM=raw.slice(4,6), dd=raw.slice(6,8), HH=raw.slice(8,10), mm=raw.slice(10,12), ss=(raw.slice(12,14)||"00");
    const d = new Date(`${yyyy}-${MM}-${dd}T${HH}:${mm}:${ss}+09:00`);
    if (!isNaN(d.getTime())) return Math.floor(d.getTime()/1000);
  } else {
    const d = new Date(raw.replace(" ","T") + (/\+/.test(raw)?"":"+09:00"));
    if (!isNaN(d.getTime())) return Math.floor(d.getTime()/1000);
  }
  return null;
}

// ---------- 체감온도 ----------
function heatIndexC(tC: number, rh: number): number {
  const T = tC*9/5+32, R=rh;
  const HI = -42.379 + 2.04901523*T + 10.14333127*R - 0.22475541*T*R
           - 6.83783e-3*T*T - 5.481717e-2*R*R + 1.22874e-3*T*T*R
           + 8.5282e-4*T*R*R - 1.99e-6*T*T*R*R;
  return (HI-32)*5/9;
}
function windChillC(tC: number, vMs: number): number {
  const v = vMs*3.6; // km/h
  if (tC>10 || v<=4.8) return tC;
  return 13.12 + 0.6215*tC - 11.37*Math.pow(v,0.16) + 0.3965*tC*Math.pow(v,0.16);
}

// ---------- 메인 파서 ----------
async function fetchLatestASOS(stn: string) {
  // KST 최근 6시간 범위
  const nowUtc = Date.now();
  const nowKst = nowUtc + 9*3600_000;
  const tm2 = new Date(nowKst); tm2.setSeconds(0,0);
  const tm1 = new Date(tm2.getTime() - 6*3600_000);

  const fmt = (d: Date) => {
    const yyyy=d.getUTCFullYear(), MM=String(d.getUTCMonth()+1).padStart(2,"0"), dd=String(d.getUTCDate()).padStart(2,"0");
    const HH=String(d.getUTCHours()).padStart(2,"0"), mm=String(d.getUTCMinutes()).padStart(2,"0");
    return `${yyyy}${MM}${dd}${HH}${mm}`;
  };

  const base = need("APIHUB_BASE");
  const key  = need("APIHUB_KEY");

  const tries = [
    `${base}/api/typ01/url/kma_sfctm2.php?stn=${encodeURIComponent(stn)}&tm1=${fmt(tm1)}&tm2=${fmt(tm2)}&disp=1&help=1&authKey=${encodeURIComponent(key)}`,
    `${base}/api/typ01/url/kma_sfctm2.php?stn=${encodeURIComponent(stn)}&tm1=${fmt(tm1)}&tm2=${fmt(tm2)}&disp=0&help=0&authKey=${encodeURIComponent(key)}`,
  ];

  let lastErr: Error | null = null;

  for (const url of tries) {
    const t0 = Date.now();
    const res = await fetch(url);
    const latency = Date.now()-t0;
    const text = await decodeKR(res);
    if (!res.ok) { lastErr = new Error(`ASOS ${res.status}: ${text.slice(0,200)}`); continue; }

    const { comments, rest } = splitHeaderData(toLines(text));
    const cleaned = stripTableDecor(rest);
    const mode: "csv"|"ws" = cleaned.some(l => l.includes(",")) ? "csv" : "ws";

    const rowsAll = cleaned.map(l => splitBy(l, mode));
    const rows = rowsAll.filter(r => r.some(x => isFinite(toNum(x))));
    if (!rows.length) { lastErr = new Error("ASOS: no data rows"); continue; }

    // 1) 헤더 기반 인덱스
    const toks = headerTokens(comments, rows[0].length) || [];
    let iTM = -1, iTA = -1, iRH = -1, iWS = -1;

    if (toks.length) {
      const find = (re: RegExp) => toks.findIndex(t => re.test(t));
      iTM = find(/^(TM|TIME|DATE|YYMM)/i);
      iTA = find(/^(TA|TEMP|기온)$/i);
      iRH = find(/^(HM|REH|RH|습도)$/i);
      iWS = find(/^(WS|WIND|풍속)$/i);
    }

    // 2) 휴리스틱(열 단위)
    const cols = rows[0].length;
    const numeric: number[][] = rows.map(r => r.map(toNum));

    const bestIndex = (ok: (v:number)=>boolean, penalty?: (v:number)=>number) => {
      let best=-1, bestScore=-1;
      for (let c=0;c<cols;c++){
        let cnt=0, pen=0, tot=0;
        for (const row of numeric.slice(-60)) {
          const v=row[c]; if (!isFinite(v)) continue; tot++;
          if (ok(v)) cnt++; if (penalty) pen += penalty(v);
        }
        const score = tot ? (cnt/tot) - (pen/tot) : -1;
        if (score>bestScore){ bestScore=score; best=c; }
      }
      return best;
    };

    const isTemp = (v:number)=> v>-40 && v<50;
    const isRH   = (v:number)=> v>=0 && v<=100;
    const isWS   = (v:number)=> v>=0 && v<=30; // m/s 상한 보수적(>30 거의 없음)
    const smallIntegerPenalty = (v:number)=> (Number.isInteger(v) && v<=36 ? 0.2 : 0); // 현상/방위 코드류 배제 유도

    if (iTA<0) iTA = bestIndex(isTemp);
    if (iRH<0) iRH = bestIndex(isRH);
    if (iWS<0) iWS = bestIndex(isWS, smallIntegerPenalty);

    // 시간 인덱스
    if (iTM<0){ const pick = pickTimeIndex(rows); iTM = pick.iTM; }

    if (DBG) console.log({ header:toks, iTM, iTA, iRH, iWS });

    if (iTA<0 || iRH<0 || iWS<0) { lastErr = new Error("Required columns not found (TA/RH/WS)"); continue; }

    // 3) 최신 유효행 선택(뒤에서 앞으로)
    for (let k=rows.length-1;k>=0;k--){
      const r = rows[k];
      const tC = toNum(r[iTA]);
      const rh = toNum(r[iRH]);
      const wMs= toNum(r[iWS]);

      // 행 sanity check
      if (!isFinite(tC) || tC<=-60 || tC>=60) continue;
      if (!isFinite(rh) || rh<1 || rh>100) continue;
      if (!isFinite(wMs) || wMs<0 || wMs>30) continue; // 풍속 이상치 컷

      // 타임스탬프(KST)
      let ts = Math.floor(Date.now()/1000);
      if (iTM>=0){
        const raw = r[iTM];
        const t = parseTsKST(raw); if (t) ts = t;
      }

      const feels =
        (tC>=27 && rh>=40) ? heatIndexC(tC, rh) :
        (tC<=10 && wMs>1.34) ? windChillC(tC, wMs) :
        tC;

      if (DBG) console.log({ pickedRow:r, idx:{iTM,iTA,iRH,iWS}, values:{ tC, rh, wMs, feels, ts } });
      return { tC, rh, wMs, feels, ts, latency };
    }

    lastErr = new Error("ASOS: no valid row after sanity check");
  }

  throw lastErr ?? new Error("ASOS: failed to fetch/parse");
}

// ---------- 메인 ----------
(async () => {
  const stn = (env.ASOS_STN || "108").trim();
  const loc = env.LOC || "seoul";
  const { tC, rh, wMs, feels, ts, latency } = await fetchLatestASOS(stn);

  const now = Math.floor(Date.now()/1000);
  const lines = [
    `life_index,source=kmahub-asos,loc=${loc},stn=${stn} feels_c=${+feels.toFixed(2)},temp_c=${tC},rh_pct=${rh},wind_ms=${wMs} ${ts}`,
    `api_probe,service=asos_feels,env=prod,loc=${loc} success=1i,latency_ms=${latency}i ${now}`,
  ];
  await writeLP(lines);
  console.log(`FeelsLike=${feels.toFixed(2)}C, Temp=${tC}C, RH=${rh}%, Wind=${wMs}m/s @ stn=${stn}\nInflux write OK`);
})().catch(e => { console.error(e); process.exit(1); });

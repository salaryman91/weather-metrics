/**
 * KMAHub 자외선(kma_sfctm_uv.php) → InfluxDB Cloud
 *
 * - tm 후보: **KST 기준 10분 간격**, 최근 3시간 (HHmm)
 * - stn=지점 → 실패 시 stn=0(전체) 재시도
 * - help=0/1, disp=0/1 모두 파싱
 * - EUC-KR/UTF-8 자동 디코딩(iconv-lite)
 * - 우선순위: UV-B(지수) → EUV(÷25 우선, ×40 보조) → 휴리스틱
 * - PowerShell 디버그:  $env:DEBUG_UV="1"
 */

import * as iconv from "iconv-lite";

// ===== 환경 =====
type Env = {
  INFLUX_URL: string; INFLUX_TOKEN: string; INFLUX_ORG: string; INFLUX_BUCKET: string;
  APIHUB_BASE: string; APIHUB_KEY: string; UV_STN?: string; LOC?: string;
};
const env = process.env as unknown as Env;
const need = (k: keyof Env) => { const v = env[k]; if (!v) throw new Error(`Missing env: ${k}`); return v; };
const DBG = !!process.env.DEBUG_UV;

// ===== 유틸 =====
/** CSV 한 줄 안전 분할 (따옴표 이스케이프 포함) */
function splitCSVLine(line: string): string[] {
  const out: string[] = []; let cur = ""; let q = false;
  for (let i=0;i<line.length;i++){
    const c=line[i];
    if (c === '"') { if (q && line[i+1] === '"') { cur+='"'; i++; } else { q = !q; } }
    else if (c === "," && !q) { out.push(cur); cur=""; }
    else cur += c;
  }
  out.push(cur);
  return out.map(s => s.trim());
}

/** BOM 제거, 개행 분리, 공백줄 제거(주석 줄은 유지) */
const toLines = (t: string) =>
  t.replace(/\ufeff/g,"").split(/\r?\n/).map(s => s.replace(/\s+$/,"")).filter(l => l.trim().length>0);

/** CSV/공백 분기 파서 (# 주석 프리픽스 제거) */
function splitBy(line: string, mode: "csv" | "ws") {
  const s = line.replace(/^#\s*/, "").trim();
  return mode === "csv" ? splitCSVLine(s) : s.split(/\s+/);
}

/** 수치 변환(결측: -9/-9.0/-999 등 → NaN) */
const toNum = (s?: string) => {
  if (s == null) return NaN;
  const clean = s.replace(/[|│┃┆┊]/g, "").replace(/,/g,"");
  const n = parseFloat(clean);
  return !isFinite(n) || n <= -8.9 ? NaN : n;
};

/** 통계적 정수 지점코드(1~9999)로 보이는 값은 제외할 때 사용 */
const looksStationCode = (v: number) => Number.isInteger(v) && v >= 1 && v < 10000;

// ===== 인코딩 판별 디코더 =====
async function decodeKR(res: Response): Promise<string> {
  const ab = await res.arrayBuffer();
  const buf = Buffer.from(ab);
  const ct = (res.headers.get("content-type") || "").toLowerCase();

  if (/euc-?kr|ks_c_5601|cp949/.test(ct)) return iconv.decode(buf, "euc-kr");
  if (/utf-?8/.test(ct)) return buf.toString("utf8");

  const utf = buf.toString("utf8");
  if (utf.includes("\uFFFD")) return iconv.decode(buf, "euc-kr"); // 깨짐 감지 → EUC-KR 재해석
  return utf;
}

// ===== Influx 라이트 =====
async function writeLP(lines: string[]) {
  const url = `${need("INFLUX_URL")}/api/v2/write?org=${encodeURIComponent(need("INFLUX_ORG"))}&bucket=${encodeURIComponent(need("INFLUX_BUCKET"))}&precision=s`;
  const res = await fetch(url, {
    method:"POST",
    headers:{ Authorization:`Token ${need("INFLUX_TOKEN")}`, "Content-Type":"text/plain; charset=utf-8" },
    body: lines.join("\n")
  });
  if (!res.ok) throw new Error(`Influx write ${res.status}: ${await res.text()}`);
}

// ===== 파서 보조 =====
function stripTableDecor(lines: string[]): string[] {
  const border = /^[\s|│┃┆┊\-─━┈┉┄┅=+]+$/;
  return lines
    .filter(l => !border.test(l))
    .map(l => l.replace(/[│┃┆┊]/g, " ").replace(/\s*\|\s*/g, " "));
}
function splitHeaderData(all: string[]) {
  const comments = all.filter(l => l.trim().startsWith("#"));
  const rest = all.filter(l => !l.trim().startsWith("#"));
  return { comments, rest };
}
/** help=0/1 헤더 라인에서 토큰(열 이름들) 추출: 데이터 행 길이와 일치하는 줄만 채택 */
function headerTokens(comments: string[], rowLen: number): string[] | null {
  for (const line of comments) {
    const s = line.replace(/^#\s*/, "").trim();
    // 예시: "YYMMDDHHMI STN UVB UVA EUV UV-B UV-A TEMP1 TEMP2"
    if (!/(STN|지점)/i.test(s)) continue;
    if (!/(UVB|UV\-B|UVA|UV\-A|EUV|YYMM|TM|TIME|DATE)/i.test(s)) continue;
    const toks = s.split(/\s+/);
    if (toks.length === rowLen) return toks;
  }
  return null;
}

function pickTimeIndex(rows: string[][]): { iTM: number; pair?: [number, number] } {
  if (!rows.length) return { iTM: -1 };
  let iTM = rows[0].findIndex(v => /^\d{12,14}$/.test(v)); // YYYYMMDDHHmm(ss)
  if (iTM >= 0) return { iTM };
  const cols = rows[0].length;
  for (let c=0; c<cols-1; c++) {
    const a = rows[0][c], b = rows[0][c+1];
    if (/^\d{8}$/.test(a) && /^\d{4}$/.test(b)) return { iTM: -1, pair: [c,c+1] }; // YYYYMMDD + HHmm
  }
  return { iTM: -1 };
}

function parseTsKST(raw: string): number | null {
  if (/^\d{12,14}$/.test(raw)) {
    const yyyy=raw.slice(0,4), MM=raw.slice(4,6), dd=raw.slice(6,8),
          HH=raw.slice(8,10), mm=raw.slice(10,12), ss=(raw.slice(12,14) || "00");
    const iso = `${yyyy}-${MM}-${dd}T${HH}:${mm}:${ss}+09:00`;
    const d = new Date(iso); if (!isNaN(d.getTime())) return Math.floor(d.getTime()/1000);
  } else {
    const d = new Date(raw.replace(" ", "T") + (/\+/.test(raw) ? "" : "+09:00"));
    if (!isNaN(d.getTime())) return Math.floor(d.getTime()/1000);
  }
  return null;
}

function pickStationIndex(rows: string[][]): number {
  if (!rows.length) return -1;
  const cols = rows[0].length;
  let best=-1, score=-1;
  for (let c=0; c<cols; c++) {
    let ok=0, tot=0;
    for (const r of rows.slice(-60)) {
      const v = toNum(r[c]); if (!isFinite(v)) continue;
      tot++; if (looksStationCode(v)) ok++;
    }
    const fit = tot? ok/tot : 0;
    if (fit > score) { score = fit; best = c; }
  }
  return best;
}

/** KST(UTC+9) 기준 10분 간격 후보(기본 180분=3h) */
function tmCandidates(minutesBack = 180): string[] {
  const out: string[] = [];
  const kstNowMs = Date.now() + 9 * 3600_000; // UTC→KST
  const base = new Date(kstNowMs);
  base.setSeconds(0, 0); // SS.ms = 00
  // 10분 눈금으로 내림
  const mm = base.getUTCMinutes();
  const snap = mm - (mm % 10);
  base.setUTCMinutes(snap);

  const fmt = (msKST: number) => {
    const d = new Date(msKST);
    const yyyy = d.getUTCFullYear().toString();
    const MM   = String(d.getUTCMonth()+1).padStart(2,"0");
    const dd   = String(d.getUTCDate()).padStart(2,"0");
    const HH   = String(d.getUTCHours()).padStart(2,"0");
    const mm   = String(d.getUTCMinutes()).padStart(2,"0");
    return `${yyyy}${MM}${dd}${HH}${mm}`;
  };

  for (let m=0; m<=minutesBack; m+=10) {
    out.push(fmt(base.getTime() - m*60_000));
  }
  return Array.from(new Set(out));
}

// ===== UV 수집 =====
type Tx = "id" | "div25" | "mul40";
type Found = { uv: number; used: "UV-B" | "EUV_div25" | "EUV_mul40" | "heuristic"; col: number; tx: Tx };

async function fetchLatestUV(stnWanted: string) {
  const base = need("APIHUB_BASE");
  const key  = need("APIHUB_KEY");

  const stnList = [stnWanted, "0"];   // 지점 → 전체 순서
  const tms = tmCandidates(180);      // 최근 3h, 10분 간격

  let lastErr: Error | null = null;

  for (const stn of stnList) {
    for (const tm of tms) {
      const urls = [
        `${base}/api/typ01/url/kma_sfctm_uv.php?stn=${stn}&tm=${tm}&disp=0&help=0&authKey=${key}`,
        `${base}/api/typ01/url/kma_sfctm_uv.php?stn=${stn}&tm=${tm}&disp=1&help=1&authKey=${key}`,
      ];

      for (const url of urls) {
        const t0 = Date.now();
        const res = await fetch(url);
        const latency = Date.now() - t0;
        const text = await decodeKR(res);

        if (!res.ok) { lastErr = new Error(`HTTP ${res.status}: ${text.slice(0,180)}`); continue; }

        if (DBG) {
          console.log("---- UV DEBUG url ----");
          console.log(url.replace(key, "***"));
          console.log("---- first 10 lines ----");
          console.log(text.split(/\r?\n/).slice(0,10));
        }

        // 전처리
        const rawLines = toLines(text);
        const { comments, rest } = splitHeaderData(rawLines);
        const cleaned = stripTableDecor(rest);
        const mode: "csv" | "ws" = cleaned.some(l => l.includes(",")) ? "csv" : "ws";

        const rowsAll = cleaned.map(l => splitBy(l, mode));
        // 숫자가 1개라도 있는 줄만 허용(빈 주석/제목 제거)
        const rows = rowsAll.filter(r => r.some(x => isFinite(toNum(x))));
        if (rows.length === 0) { lastErr = new Error("UV: no usable rows"); continue; }

        // 헤더 토큰 → 열 이름 매핑(있으면 사용)
        const toks = headerTokens(comments, rows[0].length);
        let idxEUV = -1, idxUVB_energy = -1, idxUVA_energy = -1, idxUVB_index = -1, idxUVA_index = -1, idxTM = -1, idxSTN = -1;
        if (toks) {
          const find = (re: RegExp) => toks.findIndex(t => re.test(t));
          idxEUV        = find(/^EUV$/i);
          idxUVB_energy = find(/^UVB$/i);     // MED/10min (에너지량)
          idxUVA_energy = find(/^UVA$/i);     // J/cm2/10min (에너지량)
          idxUVB_index  = find(/^UV\-B$/i);   // 지수
          idxUVA_index  = find(/^UV\-A$/i);   // (참고)
          idxTM         = find(/^(YYMM|YYMMDDHHMI|TM|TIME|DATE)$/i);
          idxSTN        = find(/^(STN|STATION|지점)$/i);
        }

        // 시간/지점 열 인덱스
        let iTime = idxTM >= 0 ? idxTM : -1;
        let pairTime: [number, number] | undefined;
        if (iTime < 0) { const pick = pickTimeIndex(rows); iTime = pick.iTM; pairTime = pick.pair; }

        let iStn = idxSTN >= 0 ? idxSTN : -1;
        if (iStn < 0 && stn === "0") iStn = pickStationIndex(rows);

        const apply = (x:number,t:Tx)=> t==="id"?x:(t==="div25"?x/25:x*40);

        // 행 단위 후보 선택(우선순위대로 여러 열을 시도)
        const tryGetUVI = (r: string[]): Found | null => {
          // 전체지점 응답이면 원하는 지점만 필터
          if (stn === "0" && stnWanted && iStn >= 0) {
            const stnVal = String(r[iStn] ?? "").trim();
            if (stnVal && stnVal !== stnWanted) return null;
          }
          // 1) UV-B(지수) 최우선
          if (idxUVB_index >= 0) {
            const v = toNum(r[idxUVB_index]);
            if (isFinite(v) && v >= 0 && v <= 20) return { uv: v, used: "UV-B", col: idxUVB_index, tx: "id" };
          }
          // 2) EUV → 지수 환산(÷25 우선, ×40 보조)
          if (idxEUV >= 0) {
            const raw = toNum(r[idxEUV]);
            if (isFinite(raw) && raw >= 0) {
              const u1 = apply(raw, "div25");
              if (isFinite(u1) && u1 >= 0 && u1 <= 20) return { uv: u1, used: "EUV_div25", col: idxEUV, tx: "div25" };
              const u2 = apply(raw, "mul40");
              if (isFinite(u2) && u2 >= 0 && u2 <= 20) return { uv: u2, used: "EUV_mul40", col: idxEUV, tx: "mul40" };
            }
          }
          // 3) 휴리스틱(시간/지점/정수코드 열 제외)
          {
            const cols = r.length;
            for (let c=0;c<cols;c++){
              if (c === iTime || c === iStn) continue;
              const raw = toNum(r[c]); if (!isFinite(raw)) continue;
              if (looksStationCode(raw)) continue;

              const seq: Tx[] = ["id","div25","mul40"];
              for (const tx of seq) {
                const u = apply(raw, tx);
                if (isFinite(u) && u >= 0 && u <= 20) {
                  return { uv: u, used: "heuristic", col: c, tx };
                }
              }
            }
          }
          return null;
        };

        // 최신행부터 스캔 → 첫 유효값 채택
        for (let k=rows.length-1;k>=0;k--){
          const r = rows[k];
          const found = tryGetUVI(r);
          if (!found) continue;

          const { uv, col, tx, used } = found; // null 가드 이후 구조분해

          // 타임스탬프(KST)
          let ts = Math.floor(Date.now()/1000);
          if (iTime >= 0) {
            const t = parseTsKST(r[iTime]); if (t) ts = t;
          } else if (pairTime) {
            const rawT = `${r[pairTime[0]]}${r[pairTime[1]]}`;
            const t = parseTsKST(rawT); if (t) ts = t;
          }

          if (DBG) {
            console.log({
              pickedRow: r, tm, stn, method: used, col, tx, uvi: uv, ts,
              iStn, iTime, pairTime, header: toks,
            });
          }
          return { uv, ts, latency };
        }

        lastErr = new Error("UV column not found (no valid row after filtering)");
      }
    }
  }
  throw lastErr ?? new Error("UV column not found");
}

// ===== 메인 =====
(async () => {
  const stn = (env.UV_STN || "").trim();
  if (!stn) throw new Error("UV_STN 미설정: Actions Variables에 UV_STN을 넣어주세요.");
  const loc = env.LOC || "seoul";

  const { uv, ts, latency } = await fetchLatestUV(stn);
  const now = Math.floor(Date.now()/1000);

  const lines = [
    `life_index,source=kmahub-uv,loc=${loc},stn=${stn} uv_idx=${uv} ${ts}`,
    `api_probe,service=uv_obs,env=prod,loc=${loc} success=1i,latency_ms=${latency}i ${now}`
  ];
  await writeLP(lines);
  console.log(`UV=${uv} @ stn=${stn}\nInflux write OK`);
})().catch(e => { console.error(e); process.exit(1); });

/**
 * KMAHub 자외선(kma_sfctm_uv.php) → InfluxDB Cloud
 * - tm(요청 시각)을 KST 기준 "정확한 10분 단위"로 생성
 * - stn=지점 / stn=0(전체) 모두 시도, help=0/1 응답 포맷 모두 파싱
 * - EUC-KR/UTF-8 자동 판별(iconv-lite)
 * - 값 우선순위: UV-B(지수) → EUV 환산(÷25, 보조 ×40) → 휴리스틱
 * - Influx에 life_index(uv_idx), api_probe(success/latency) 쓰기
 *
 * 로컬 디버그:
 *   $env:DEBUG_UV = "1"             # PowerShell
 *   npx ts-node scripts/uv_to_influx.ts
 *   $env:DEBUG_UV = $null           # 해제
 */

import * as iconv from "iconv-lite";

// ===== ENV =====
type Env = {
  INFLUX_URL: string;
  INFLUX_TOKEN: string;
  INFLUX_ORG: string;
  INFLUX_BUCKET: string;
  APIHUB_BASE: string;
  APIHUB_KEY: string;
  UV_STN?: string;
  LOC?: string;
};
const env = process.env as unknown as Env;
const need = (k: keyof Env) => {
  const v = env[k];
  if (!v) throw new Error(`Missing env: ${k}`);
  return v;
};
const DBG = !!process.env.DEBUG_UV;

// ===== small utils =====
function splitCSVLine(line: string): string[] {
  const out: string[] = [];
  let cur = "";
  let q = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (c === '"') {
      if (q && line[i + 1] === '"') {
        cur += '"';
        i++;
      } else {
        q = !q;
      }
    } else if (c === "," && !q) {
      out.push(cur);
      cur = "";
    } else {
      cur += c;
    }
  }
  out.push(cur);
  return out.map((s) => s.trim());
}

const toLines = (t: string) =>
  t
    .replace(/\ufeff/g, "")
    .split(/\r?\n/)
    .map((s) => s.replace(/\s+$/, ""))
    .filter((l) => l.trim().length > 0);

function splitBy(line: string, mode: "csv" | "ws") {
  const s = line.trim();
  return mode === "csv" ? splitCSVLine(s) : s.split(/\s+/);
}

const toNum = (s?: string) => {
  if (s == null) return NaN;
  const clean = s.replace(/[|│┃┆┊]/g, "").replace(/,/g, "");
  const n = parseFloat(clean);
  // KMA 결측값: -9 / -9.0 / -999 등 → NaN
  return !isFinite(n) || n <= -8.9 ? NaN : n;
};

// ===== encoding detection =====
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

// ===== Influx =====
async function writeLP(lines: string[]) {
  const url = `${need("INFLUX_URL")}/api/v2/write?org=${encodeURIComponent(
    need("INFLUX_ORG")
  )}&bucket=${encodeURIComponent(need("INFLUX_BUCKET"))}&precision=s`;
  const res = await fetch(url, {
    method: "POST",
    headers: {
      Authorization: `Token ${need("INFLUX_TOKEN")}`,
      "Content-Type": "text/plain; charset=utf-8",
    },
    body: lines.join("\n"),
  });
  if (!res.ok) throw new Error(`Influx write ${res.status}: ${await res.text()}`);
}

// ===== parser helpers =====
function stripTableDecor(lines: string[]): string[] {
  const border = /^[\s|│┃┆┊\-─━┈┉┄┅=+]+$/;
  return lines
    .filter((l) => !border.test(l))
    .map((l) => l.replace(/[│┃┆┊]/g, " ").replace(/\s*\|\s*/g, " "));
}

function splitHeaderData(all: string[]) {
  const comments = all.filter((l) => l.trim().startsWith("#"));
  const rest = all.filter((l) => !l.trim().startsWith("#"));
  return { comments, rest };
}

/** 헤더 주석에서 컬럼 토큰 추출 (열 개수 일치 시만 채택) */
function headerTokens(comments: string[], rowLen: number): string[] | null {
  for (const line of comments) {
    const s = line.replace(/^#\s*/, "").trim();
    // 예: "YYMMDDHHMI STN UVB UVA EUV UV-B UV-A TEMP1 TEMP2"
    if (!/(STN|지점)/i.test(s)) continue;
    if (!/(UVB|UV\-B|UVA|UV\-A|EUV|YYMM|TM|TIME|DATE)/i.test(s)) continue;
    const toks = s.split(/\s+/);
    if (toks.length === rowLen) return toks;
  }
  return null;
}

function pickTimeIndex(rows: string[][]): { iTM: number; pair?: [number, number] } {
  if (!rows.length) return { iTM: -1 };
  // 단일 "YYYYMMDDHHmm" 또는 "YYYYMMDDHHmmss"
  let iTM = rows[0].findIndex((v) => /^\d{12,14}$/.test(v));
  if (iTM >= 0) return { iTM };
  // "YYYYMMDD" + "HHmm" 분리 케이스
  const cols = rows[0].length;
  for (let c = 0; c < cols - 1; c++) {
    const a = rows[0][c],
      b = rows[0][c + 1];
    if (/^\d{8}$/.test(a) && /^\d{4}$/.test(b)) return { iTM: -1, pair: [c, c + 1] };
  }
  return { iTM: -1 };
}

function parseTs(raw: string): number | null {
  if (/^\d{12,14}$/.test(raw)) {
    const yyyy = raw.slice(0, 4),
      MM = raw.slice(4, 6),
      dd = raw.slice(6, 8),
      HH = raw.slice(8, 10),
      mm = raw.slice(10, 12),
      ss = raw.slice(12, 14) || "00";
    // KST 기준
    const iso = `${yyyy}-${MM}-${dd}T${HH}:${mm}:${ss}+09:00`;
    const d = new Date(iso);
    if (!isNaN(d.getTime())) return Math.floor(d.getTime() / 1000);
  } else {
    const d = new Date(raw.replace(" ", "T") + (/\+/.test(raw) ? "" : "+09:00"));
    if (!isNaN(d.getTime())) return Math.floor(d.getTime() / 1000);
  }
  return null;
}

function pickStationIndex(rows: string[][]): number {
  if (!rows.length) return -1;
  const cols = rows[0].length;
  let best = -1,
    score = -1;
  for (let c = 0; c < cols; c++) {
    let ok = 0,
      tot = 0;
    for (const r of rows.slice(-60)) {
      const v = toNum(r[c]);
      if (!isFinite(v)) continue;
      tot++;
      if (Number.isInteger(v) && v >= 0 && v < 10000) ok++;
    }
    const fit = tot ? ok / tot : 0;
    if (fit > score) {
      score = fit;
      best = c;
    }
  }
  return best;
}

/** KST(Asia/Seoul) 기준 10분 단위 tm 목록 생성 (기본: 최근 6시간 = 36스텝) */
function tmCandidates10m(backSteps = 36): string[] {
  const STEP = 10 * 60 * 1000; // 10분
  const KST = 9 * 60 * 60 * 1000;

  // KST epoch로 보정 후 10분 내림
  const nowUtc = Date.now();
  let kstMs = nowUtc + KST;
  kstMs -= kstMs % STEP;

  // 주어진 epoch를 KST 캘린더로 포맷
  const toParts = (msKst: number) => {
    // Date에 바로 msKst를 넣고 Asia/Seoul로 포맷하면 KST 시점을 얻을 수 있음
    const d = new Date(msKst);
    const fmt = new Intl.DateTimeFormat("en-GB", {
      timeZone: "Asia/Seoul",
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      hour12: false,
    }).formatToParts(d);
    const m = Object.fromEntries(fmt.map((p) => [p.type, p.value]));
    return `${m.year}${m.month}${m.day}${m.hour}${m.minute}`;
  };

  const out: string[] = [];
  for (let i = 0; i < backSteps; i++) {
    out.push(toParts(kstMs - i * STEP));
  }
  return out;
}

// ===== UV fetch & parse =====
type Tx = "id" | "div25" | "mul40";
type Found = {
  uv: number;
  used: "UV-B" | "EUV_div25" | "EUV_mul40" | "heuristic";
  col: number;
  tx: Tx;
};

async function fetchLatestUV(stnWanted: string) {
  const base = need("APIHUB_BASE");
  const key = need("APIHUB_KEY");

  const stnList = [stnWanted, "0"]; // 특정 지점 → 전체 순서로 시도
  const tms = tmCandidates10m(36); // 최근 6시간(10분 간격)

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

        if (!res.ok) {
          lastErr = new Error(`HTTP ${res.status}: ${text.slice(0, 180)}`);
          continue;
        }

        if (DBG) {
          console.log("---- UV DEBUG url ----");
          console.log(url.replace(key, "***"));
          console.log("---- first 10 lines ----");
          console.log(text.split(/\r?\n/).slice(0, 10));
        }

        // 전처리
        const rawLines = toLines(text);
        const { comments, rest } = splitHeaderData(rawLines);
        const cleaned = stripTableDecor(rest);
        const mode: "csv" | "ws" = cleaned.some((l) => l.includes(",")) ? "csv" : "ws";

        const rowsAll = cleaned.map((l) => splitBy(l, mode));
        // 숫자가 하나라도 있는 라인만 데이터로 간주
        const rows = rowsAll.filter((r) => r.some((x) => isFinite(toNum(x))));
        if (rows.length === 0) {
          lastErr = new Error("UV: no usable rows");
          continue;
        }

        // 헤더 토큰 매핑
        const toks = headerTokens(comments, rows[0].length);
        let idxEUV = -1,
          idxUVB_energy = -1,
          idxUVA_energy = -1,
          idxUVB_index = -1,
          idxUVA_index = -1,
          idxTM = -1,
          idxSTN = -1;
        if (toks) {
          const find = (re: RegExp) => toks.findIndex((t) => re.test(t));
          idxEUV = find(/^EUV$/i);
          idxUVB_energy = find(/^UVB$/i); // 에너지량
          idxUVA_energy = find(/^UVA$/i); // 에너지량
          idxUVB_index = find(/^UV\-B$/i); // 지수
          idxUVA_index = find(/^UV\-A$/i); // 지수(참고)
          idxTM = find(/^(YYMM|YYMMDDHHMI|TM|TIME|DATE)$/i);
          idxSTN = find(/^(STN|STATION|지점)$/i);
        }

        // 시간/지점 인덱스 추정
        let iTime = idxTM >= 0 ? idxTM : -1;
        let pairTime: [number, number] | undefined;
        if (iTime < 0) {
          const pick = pickTimeIndex(rows);
          iTime = pick.iTM;
          pairTime = pick.pair;
        }
        let iStn = idxSTN >= 0 ? idxSTN : -1;
        if (iStn < 0 && stn === "0") iStn = pickStationIndex(rows);

        const apply = (x: number, t: Tx) => (t === "id" ? x : t === "div25" ? x / 25 : x * 40);

        // 행 단위로 후보 탐색
        const tryGetUVI = (r: string[]): Found | null => {
          // 전체지점 응답이면 stnWanted만 통과
          if (stn === "0" && stnWanted && iStn >= 0) {
            const stnVal = String(r[iStn] ?? "").trim();
            if (stnVal && stnVal !== stnWanted) return null;
          }
          // 1) UV-B(지수) 최우선
          if (idxUVB_index >= 0) {
            const v = toNum(r[idxUVB_index]);
            if (isFinite(v) && v >= 0) return { uv: v, used: "UV-B", col: idxUVB_index, tx: "id" };
          }
          // 2) EUV 환산(÷25 → ×40 보조)
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
          const cols = r.length;
          for (let c = 0; c < cols; c++) {
            if (c === iTime || c === iStn) continue;
            const raw = toNum(r[c]);
            if (!isFinite(raw)) continue;
            // 지점 코드로 보이는 정수는 제외
            if (Number.isInteger(raw) && raw >= 1 && raw < 10000) continue;

            const seq: Tx[] = ["id", "div25", "mul40"];
            for (const tx of seq) {
              const u = apply(raw, tx);
              if (isFinite(u) && u >= 0 && u <= 20) return { uv: u, used: "heuristic", col: c, tx };
            }
          }
          return null;
        };

        // 최신행부터 역순 스캔 → 첫 유효값 채택
        for (let k = rows.length - 1; k >= 0; k--) {
          const r = rows[k];
          const found = tryGetUVI(r);
          if (!found) continue;

          const { uv, col, tx, used } = found; // null 가드 이후 안전

          // 타임스탬프
          let ts = Math.floor(Date.now() / 1000);
          if (iTime >= 0) {
            const t = parseTs(r[iTime]);
            if (t) ts = t;
          } else if (pairTime) {
            const rawT = `${r[pairTime[0]]}${r[pairTime[1]]}`;
            const t = parseTs(rawT);
            if (t) ts = t;
          }

          if (DBG) {
            console.log({
              pickedRow: r,
              tm,
              stn,
              method: used,
              col,
              tx,
              uvi: uv,
              ts,
              iStn,
              iTime,
              pairTime,
              header: toks || undefined,
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

// ===== main =====
(async () => {
  const stn = (env.UV_STN || "").trim();
  if (!stn) throw new Error("UV_STN 미설정: Actions Variables에 UV_STN을 넣어주세요.");
  const loc = env.LOC || "seoul";

  const { uv, ts, latency } = await fetchLatestUV(stn);
  const now = Math.floor(Date.now() / 1000);

  const lines = [
    `life_index,source=kmahub-uv,loc=${loc},stn=${stn} uv_idx=${uv} ${ts}`,
    `api_probe,service=uv_obs,env=prod,loc=${loc} success=1i,latency_ms=${latency}i ${now}`,
  ];
  await writeLP(lines);

  console.log(`UV=${uv} @ stn=${stn}\nInflux write OK`);
})().catch((e) => {
  console.error(e);
  process.exit(1);
});

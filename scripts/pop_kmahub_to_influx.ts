/**
 * KMAHub 단기 육상예보 fct_afs_dl2 → POP(강수확률) 타임라인 전량 적재
 *   measurement=pop, source=kmahub-dl2, field=pop_pct(0~100, int)
 *
 * 실행:
 *   npx ts-node scripts/pop_kmahub_to_influx.ts
 *
 * 필요 .env / Actions:
 *   INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET
 *   APIHUB_BASE=https://apihub.kma.go.kr
 *   APIHUB_KEY=<authKey>
 *   POP_REG=<예보구역코드> (예: 11B10101)
 *   LOC=seoul (선택)
 *
 * 디버그:
 *   DEBUG_POP=1   // 헤더·샘플 로깅
 */

import * as iconv from "iconv-lite";

type Env = {
  INFLUX_URL: string;
  INFLUX_TOKEN: string;
  INFLUX_ORG: string;
  INFLUX_BUCKET: string;
  APIHUB_BASE: string;
  APIHUB_KEY: string;
  POP_REG?: string;
  LOC?: string;
};
const env = process.env as unknown as Env;
const need = (k: keyof Env) => {
  const v = env[k];
  if (!v) throw new Error(`Missing env: ${k}`);
  return v;
};
const DBG = !!process.env.DEBUG_POP;

// ---------- 유틸 ----------
/** 안전 CSV split (따옴표/이중따옴표 이스케이프 지원) */
function splitCSVLine(line: string): string[] {
  const out: string[] = []; let cur = ""; let q = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (c === '"') {
      if (q && line[i + 1] === '"') { cur += '"'; i++; }
      else { q = !q; }
    } else if (c === "," && !q) {
      out.push(cur); cur = "";
    } else {
      cur += c;
    }
  }
  out.push(cur);
  return out.map((s) => s.trim());
}

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

/** YYYYMMDDHHmm[ss] / ISO(공백 포함) → epoch(sec) [KST 처리] */
function parseTsKST(raw: string): number | null {
  const s = raw.trim().replace(" ", "T");
  // 12~14자리(초 포함) 지원
  if (/^\d{12,14}$/.test(s)) {
    const yyyy = s.slice(0, 4), MM = s.slice(4, 6), dd = s.slice(6, 8),
          HH = s.slice(8, 10), mm = s.slice(10, 12), ss = (s.slice(12, 14) || "00");
    const iso = `${yyyy}-${MM}-${dd}T${HH}:${mm}:${ss}+09:00`;
    const d = new Date(iso);
    return isNaN(d.getTime()) ? null : Math.floor(d.getTime() / 1000);
  }
  const iso = s.includes("T") ? s : s.replace(" ", "T");
  const d = new Date(/\+/.test(iso) ? iso : iso + "+09:00");
  return isNaN(d.getTime()) ? null : Math.floor(d.getTime() / 1000);
}

const clamp = (v: number, lo: number, hi: number) => Math.min(hi, Math.max(lo, v));

// ---------- Influx ----------
async function writeLP(lines: string[]): Promise<void> {
  const url = `${need("INFLUX_URL")}/api/v2/write`
    + `?org=${encodeURIComponent(need("INFLUX_ORG"))}`
    + `&bucket=${encodeURIComponent(need("INFLUX_BUCKET"))}`
    + `&precision=s`;
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

// ---------- POP 수집 ----------
type PopRow = { ts: number; pop: number };

function looksLikeHeader(s: string): boolean {
  return /(POP|강수확률)/i.test(s) && /,/.test(s);
}

/** fct_afs_dl2: CSV(help=1, disp=1) 파싱 → 전량 {ts,pop} */
async function fetchPopSeries(reg: string): Promise<{ rows: PopRow[]; url: string; latency: number }> {
  const qs = new URLSearchParams({
    reg,
    tmfc: "0",     // 최신 발표 묶음
    disp: "1",     // CSV
    help: "1",     // 헤더 포함
    authKey: need("APIHUB_KEY"),
  });
  const url = `${need("APIHUB_BASE")}/api/typ01/url/fct_afs_dl2.php?${qs.toString()}`;

  const t0 = Date.now();
  const res = await fetch(url);
  const latency = Date.now() - t0;
  const text = await decodeKR(res);
  if (!res.ok) throw new Error(`KMAHub ${res.status}: ${text.slice(0, 200)}`);

  // BOM/공백/주석 제거
  const linesAll = text.replace(/\ufeff/g, "")
    .split(/\r?\n/)
    .map((l) => l.trim())
    .filter((l) => l.length > 0 && !l.startsWith("#"));

  if (linesAll.length === 0) throw new Error("No lines");

  // 헤더 라인 탐색(최대 5줄)
  let headerIdx = 0;
  for (let i = 0; i < Math.min(5, linesAll.length); i++) {
    if (looksLikeHeader(linesAll[i])) { headerIdx = i; break; }
  }
  const header = splitCSVLine(linesAll[headerIdx]);
  const dataRows = linesAll.slice(headerIdx + 1).map(splitCSVLine).filter(r => r.length >= header.length);

  if (DBG) {
    console.log("POP header:", header);
    console.log("POP first 3 rows:", dataRows.slice(0, 3));
  }

  // 컬럼 인덱스 탐색
  let idxPOP = header.findIndex(h => /^(ST|POP)$/i.test(h) || /강수확률/.test(h));
  let idxT   = header.findIndex(h => /(tmef|ftime|time|valid|fcst)/i.test(h));

  // 시간 컬럼 미탐색 시: 데이터 기반 추정 (12~14자리 시각이 3회 이상 파싱되는 열)
  if (idxT === -1 && dataRows.length) {
    const cols = dataRows[0].length;
    let best = -1, score = -1;
    for (let c = 0; c < cols; c++) {
      let ok = 0, tot = 0;
      for (const r of dataRows.slice(0, 24)) {
        const ts = parseTsKST(r[c] || "");
        if (ts) ok++;
        tot++;
      }
      if (ok >= 3 && ok > score) { score = ok; best = c; }
    }
    idxT = best;
  }

  if (idxPOP === -1) throw new Error("POP column not found");
  if (idxT   === -1) throw new Error("Time column not found (tmef/ftime/valid)");

  const rows: PopRow[] = [];
  for (const r of dataRows) {
    const pRaw = (r[idxPOP] ?? "").trim();
    if (!/^\d+$/.test(pRaw)) continue;
    const pop = clamp(parseInt(pRaw, 10), 0, 100);

    const ts = parseTsKST(r[idxT] || "");
    if (!ts) continue;

    rows.push({ ts, pop });
  }

  if (!rows.length) throw new Error("No numeric POP rows with valid time");

  // 정렬 & 중복(ts) 마지막 값 우선
  rows.sort((a, b) => a.ts - b.ts);
  const dedup = new Map<number, number>();
  for (const { ts, pop } of rows) dedup.set(ts, pop);
  const out: PopRow[] = Array.from(dedup.entries()).map(([ts, pop]) => ({ ts, pop }))
    .sort((a, b) => a.ts - b.ts);

  return { rows: out, url, latency };
}

// ---------- 메인 ----------
(async () => {
  const reg = (env.POP_REG || "").trim();
  if (!reg) throw new Error("POP_REG 미설정");
  const loc = env.LOC?.trim() || "seoul";

  try {
    const { rows, url, latency } = await fetchPopSeries(reg);

    // 시계열 가시화용 시간창 (과거 12h ~ +72h)
    const now = Math.floor(Date.now() / 1000);
    const minTs = now - 12 * 3600;
    const maxTs = now + 72 * 3600;

    const lines: string[] = [];
    for (const { ts, pop } of rows) {
      if (ts < minTs || ts > maxTs) continue;
      // DL2는 % 정수라고 가정(보정 없음). 0~100 범위는 이미 clamp.
      lines.push(`pop,source=kmahub-dl2,loc=${loc},reg=${reg} pop_pct=${pop}i ${ts}`);
    }

    if (DBG) console.log(`toWrite=${lines.length}, window=[${new Date(minTs*1000).toISOString()} ~ ${new Date(maxTs*1000).toISOString()}]`);
    if (!lines.length) throw new Error("No rows in time window (past 12h ~ +72h)");

    // 본 데이터 + 수집 프로브
    const probeTs = Math.floor(Date.now() / 1000);
    lines.push(`api_probe,service=pop_kmahub_dl2,env=prod,loc=${loc} success=1i,latency_ms=${latency}i ${probeTs}`);

    await writeLP(lines);
    console.log(`Wrote POP points: ${lines.length - 1}\nfrom: ${url}`);
  } catch (e: any) {
    // 실패도 프로브로 기록하고 Job 계속(스케줄 유지)
    const locTag = env.LOC?.trim() || "seoul";
    const now = Math.floor(Date.now() / 1000);
    const latency = Number.isFinite(e?.latency) ? e.latency : 0;
    try {
      await writeLP([`api_probe,service=pop_kmahub_dl2,env=prod,loc=${locTag} success=0i,latency_ms=${latency}i ${now}`]);
    } catch {}
    console.error(e);
    process.exit(0);
  }
})();

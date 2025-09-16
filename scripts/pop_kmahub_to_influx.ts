/**
 * POP(강수확률) 수집: KMA API허브 fct_afs_dl2 → InfluxDB Cloud
 *
 * 실행:
 *   npx ts-node scripts/pop_kmahub_to_influx.ts
 *
 * 필요 환경변수(.env 혹은 GitHub Actions env/secrets):
 *   INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET
 *   APIHUB_BASE=https://apihub.kma.go.kr
 *   APIHUB_KEY=<authKey>
 *   POP_REG=<예보구역코드, 예: 11B10101>
 *   LOC=seoul (선택)
 *
 * 스키마(권장/통일):
 *   _measurement = "pop"
 *   _field       = "pop_pct"      // float, 0~100 (%)
 *   tags         = source=kmahub-di2, reg=<...>, loc=<...>
 *   _time        = 예보 유효시각
 *
 * 헬스체크는 별도 측정치:
 *   _measurement = "api_probe"
 *   fields       = success(1i), latency_ms(i)
 *   tags         = service=pop_kmahub, env=prod, loc=<...>
 */

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

/** CSV 한 줄 안전 분해 (따옴표 이스케이프 대응) */
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

/** 다양한 포맷의 시간 문자열 → epoch(sec) (기본 KST) */
function parseToEpochSec(raw: string): number | null {
  const s = raw.trim();
  if (!s) return null;

  // 1) 20240916 12:00 or 2024-09-16 12:00
  const norm = s.replace(/(\d{4})[-/.]?(\d{2})[-/.]?(\d{2})[ T]?(\d{2}):?(\d{2})(?::?(\d{2}))?/, "$1-$2-$3T$4:$5:$6");
  if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(?::\d{2})?$/.test(norm)) {
    const withTz = norm.length === 16 ? norm + ":00+09:00" : norm + "+09:00";
    const d = new Date(withTz);
    return isNaN(d.getTime()) ? null : Math.floor(d.getTime() / 1000);
  }

  // 2) 202409161200 (yyyyMMddHHmm) or 20240916120000
  if (/^\d{12,14}$/.test(s)) {
    const yyyy = s.slice(0, 4);
    const MM = s.slice(4, 6);
    const dd = s.slice(6, 8);
    const HH = s.slice(8, 10);
    const mm = s.slice(10, 12);
    const ss = s.length >= 14 ? s.slice(12, 14) : "00";
    const iso = `${yyyy}-${MM}-${dd}T${HH}:${mm}:${ss}+09:00`;
    const d = new Date(iso);
    return isNaN(d.getTime()) ? null : Math.floor(d.getTime() / 1000);
  }

  // 3) 이미 타임존 포함 ISO
  const d = new Date(/\+|Z/.test(s) ? s : s + "+09:00");
  return isNaN(d.getTime()) ? null : Math.floor(d.getTime() / 1000);
}

type PopRow = { ts: number; popPct: number; raw: string[] };

/** KMAHub fct_afs_dl2: POP 전체 행 파싱 */
async function fetchPopRows(reg: string): Promise<{ rows: PopRow[]; latency: number; url: string }> {
  const qs = new URLSearchParams({
    reg,
    tmfc: "0", // 최신 발표 묶음
    disp: "1", // CSV
    help: "1", // 헤더/주석 포함
    authKey: need("APIHUB_KEY"),
  });
  const url = `${need("APIHUB_BASE")}/api/typ01/url/fct_afs_dl2.php?${qs.toString()}`;

  const t0 = Date.now();
  const res = await fetch(url);
  const latency = Date.now() - t0;
  const text = await res.text();
  if (!res.ok) throw new Error(`KMAHub ${res.status}: ${text.slice(0, 300)}`);

  const rawLines = text.replace(/\ufeff/g, "").split(/\r?\n/).filter((l) => l.trim().length > 0);
  const lines = rawLines.filter((l) => !l.startsWith("#"));

  // 헤더 찾기
  const looksHeader = (s: string) =>
    /(^|,)\s*(ST|POP|강수확률)\s*(,|$)/i.test(s) || /(tmef|ftime|tmEf|valid|time|fcst)/i.test(s);
  let headerIdx = lines.findIndex(looksHeader);
  if (headerIdx < 0) headerIdx = 0;

  const header = splitCSVLine(lines[headerIdx]);
  const data = lines.slice(headerIdx + 1).map(splitCSVLine).filter((r) => r.length >= header.length);

  // 컬럼 인덱스 추정
  let iPOP = header.findIndex((h) => /^(ST|POP)$/i.test(h) || /강수확률/.test(h));
  if (iPOP < 0) {
    // 숫자 0~100 패턴 열 탐색
    const cand: number[] = [];
    for (let c = 0; c < header.length; c++) {
      const ok = data.slice(0, 20).every((r) => /^\d+$/.test(r[c] ?? "") && +r[c] >= 0 && +r[c] <= 100);
      if (ok) cand.push(c);
    }
    if (cand.length) iPOP = cand[0];
  }
  const iT = header.findIndex((h) => /(tmef|ftime|tmEf|valid|time|fcst)/i.test(h));

  if (iPOP < 0) {
    console.log("Header:", header);
    console.log("Sample:", data.slice(0, 5));
    throw new Error("POP column not found");
  }

  const rows: PopRow[] = [];
  for (const r of data) {
    const rawPop = r[iPOP] ?? "";
    if (!/^\d+$/.test(rawPop)) continue;
    const popInt = parseInt(rawPop, 10);
    // 0~100이 기본. (혹시 0~1 스케일이면, 아래에서 100배)
    let popPct = popInt;
    if (popPct <= 1) popPct = popPct * 100;

    let ts = Math.floor(Date.now() / 1000);
    if (iT >= 0 && r[iT]) {
      const t = parseToEpochSec(r[iT]);
      if (t) ts = t;
    }

    rows.push({ ts, popPct, raw: r });
  }

  // 중복 시간 제거(가장 마지막 값 채택)
  rows.sort((a, b) => a.ts - b.ts);
  const uniq: PopRow[] = [];
  for (let i = 0; i < rows.length; i++) {
    if (i === rows.length - 1 || rows[i].ts !== rows[i + 1].ts) uniq.push(rows[i]);
  }

  return { rows: uniq, latency, url };
}

/** Influx v2 write (precision=s) */
async function writeLP(lines: string[]): Promise<void> {
  const url =
    `${need("INFLUX_URL")}/api/v2/write` +
    `?org=${encodeURIComponent(need("INFLUX_ORG"))}` +
    `&bucket=${encodeURIComponent(need("INFLUX_BUCKET"))}` +
    `&precision=s`;

  const res = await fetch(url, {
    method: "POST",
    headers: {
      Authorization: `Token ${need("INFLUX_TOKEN")}`,
      "Content-Type": "text/plain; charset=utf-8",
    },
    body: lines.join("\n"),
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Influx write ${res.status}: ${body}`);
  }
}

/** main */
(async () => {
  const reg = (env.POP_REG || "").trim();
  if (!reg) throw new Error("POP_REG 미설정: 예보구역 코드(POP_REG)를 넣어주세요.");
  const loc = env.LOC?.trim() || "seoul";

  const { rows, latency, url } = await fetchPopRows(reg);

  if (rows.length === 0) throw new Error("POP rows not found");

  // 라인프로토콜 생성: measurement=pop / field=pop_pct(float)
  const points = rows.map(
    ({ ts, popPct }) => `pop,source=kmahub-di2,loc=${loc},reg=${reg} pop_pct=${popPct.toFixed(1)} ${ts}`
  );

  // 헬스체크(현재시각)
  const now = Math.floor(Date.now() / 1000);
  points.push(`api_probe,service=pop_kmahub,env=prod,loc=${loc} success=1i,latency_ms=${latency}i ${now}`);

  await writeLP(points);

  console.log(`POP rows written: ${rows.length}`);
  console.log(`first=${rows[0].popPct}% @ ${rows[0].ts}, last=${rows[rows.length - 1].popPct}% @ ${rows[rows.length - 1].ts}`);
  console.log(`from: ${url}`);
  console.log("Influx write OK");
})().catch((e) => {
  console.error(e);
  process.exit(1);
});

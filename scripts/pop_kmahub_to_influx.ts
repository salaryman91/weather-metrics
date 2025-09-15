/**
 * POP(강수확률) 스모크: KMA API허브 fct_afs_dl2 → InfluxDB Cloud 적재
 * 실행: npm run dev:pop
 * 필요 .env:
 *   INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET
 *   APIHUB_BASE=https://apihub.kma.go.kr
 *   APIHUB_KEY=<authKey>
 *   POP_REG=<예보구역코드>
 *   LOC=seoul (선택)
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
const req = (k: keyof Env) => {
  const v = env[k];
  if (!v) throw new Error(`Missing env: ${k}`);
  return v;
};

/** CSV 한 줄 안전 분할 (따옴표/이중따옴표 이스케이프 처리) */
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

/** KMAHub 단기 육상예보(구역 단위)에서 POP 1건 가져오기 */
async function fetchPop(reg: string): Promise<{
  pop: number;
  ts: number; // seconds
  latency: number;
  url: string;
}> {
  // 헤더가 항상 나오도록 help=1로 호출, CSV(disp=1)
  const qs = new URLSearchParams({
    reg,
    tmfc: "0", // 최신 발표 묶음
    disp: "1",
    help: "1",
    authKey: req("APIHUB_KEY"),
  });

  const url = `${req("APIHUB_BASE")}/api/typ01/url/fct_afs_dl2.php?${qs.toString()}`;

  const t0 = Date.now();
  const res = await fetch(url);
  const latency = Date.now() - t0;
  const text = await res.text();

  if (!res.ok) {
    throw new Error(`KMAHub ${res.status}: ${text.slice(0, 200)}`);
  }

  // BOM 제거, 공백/빈줄 제거
  const rawLines = text.replace(/\ufeff/g, "").split(/\r?\n/).filter((l) => l.trim().length > 0);
  // '#START7777' 같은 메타/주석 라인 제거
  const lines = rawLines.filter((l) => !l.startsWith("#"));

  // 헤더 라인 찾기
  const looksLikeHeader = (s: string) =>
    /(^|,)\s*(ST|POP|강수확률)\s*(,|$)/i.test(s) || /(tmef|ftime|time|valid)/i.test(s);
  let headerIdx = lines.findIndex(looksLikeHeader);
  if (headerIdx < 0) headerIdx = 0; // 실패 시 첫 줄을 헤더로 가정

  const header = splitCSVLine(lines[headerIdx]);
  const dataRows = lines
    .slice(headerIdx + 1)
    .map(splitCSVLine)
    .filter((r) => r.length >= header.length);

  // POP 컬럼 찾기(이름 우선, 실패 시 0~100 정수 패턴)
  let idxPOP = header.findIndex((h) => /^(ST|POP)$/i.test(h) || /강수확률/.test(h));
  if (idxPOP === -1) {
    const cand: number[] = [];
    for (let c = 0; c < header.length; c++) {
      const ok = dataRows.slice(0, 12).every((r) => /^\d+$/.test(r[c] ?? "") && +r[c] >= 0 && +r[c] <= 100);
      if (ok) cand.push(c);
    }
    if (cand.length > 0) idxPOP = cand[0];
  }
  if (idxPOP === -1) {
    console.log("Header:", header);
    console.log("Sample rows:", dataRows.slice(0, 5));
    throw new Error("POP column not found (header·패턴 둘 다 실패)");
  }

  // 시간 컬럼 추정(없으면 현재 시각 사용)
  const idxT = header.findIndex((h) => /(tmef|ftime|time|valid|fcst)/i.test(h));

  // 첫 유효 숫자행 선택
  const row = dataRows.find((r) => /^\d+$/.test(r[idxPOP] ?? ""));
  if (!row) throw new Error("No numeric POP row");

  const pop = parseInt(row[idxPOP], 10);

  // 타임스탬프(초) 계산
  let ts = Math.floor(Date.now() / 1000);
  if (idxT !== -1) {
    const raw = (row[idxT] || "").replace(" ", "T");
    const d = new Date(/\+/.test(raw) ? raw : raw + "+09:00");
    if (!isNaN(d.getTime())) ts = Math.floor(d.getTime() / 1000);
  }

  return { pop, ts, latency, url };
}

/** InfluxDB v2 라인프로토콜 쓰기 */
async function writeLines(lines: string[]): Promise<void> {
  const url =
    `${req("INFLUX_URL")}/api/v2/write` +
    `?org=${encodeURIComponent(req("INFLUX_ORG"))}` +
    `&bucket=${encodeURIComponent(req("INFLUX_BUCKET"))}` +
    `&precision=s`;

  const res = await fetch(url, {
    method: "POST",
    headers: {
      Authorization: `Token ${req("INFLUX_TOKEN")}`,
      "Content-Type": "text/plain; charset=utf-8",
    },
    body: lines.join("\n"),
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Influx write ${res.status}: ${body}`);
  }
}

/** 메인 */
(async () => {
  // 환경변수 확인
  const reg = (env.POP_REG || "").trim();
  if (!reg) throw new Error("POP_REG 미설정: .env에 예보구역 코드(POP_REG)를 넣어주세요.");
  const loc = env.LOC?.trim() || "seoul";

  // POP 1건 수집
  const { pop, ts, latency, url } = await fetchPop(reg);
  console.log(`POP=${pop} ts=${ts} reg=${reg}\nfrom: ${url}`);

  // 라인프로토콜 작성
  const now = Math.floor(Date.now() / 1000);
  const lines = [
    // 예보
    `forecast,source=kmahub-dl2,loc=${loc},reg=${reg} pop_pct=${pop}i ${ts}`,
    // 프로브(가용성/지연)
    `api_probe,service=pop_kmahub,env=dev,loc=${loc} success=1i,latency_ms=${latency}i ${now}`,
  ];

  // Influx로 전송
  await writeLines(lines);
  console.log("Influx write OK");
})().catch((e) => {
  console.error(e);
  process.exit(1);
});

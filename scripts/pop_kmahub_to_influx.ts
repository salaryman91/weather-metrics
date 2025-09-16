/**
 * POP(강수확률) 수집: KMA API허브 fct_afs_dl2 → InfluxDB Cloud (타임시리즈 전체 적재)
 * 실행: npx ts-node scripts/pop_kmahub_to_influx.ts
 * 필요 .env:
 *   INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET
 *   APIHUB_BASE=https://apihub.kma.go.kr
 *   APIHUB_KEY=<authKey>
 *   POP_REG=<예보구역코드>   예: 11B10101
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
const need = (k: keyof Env) => {
  const v = env[k];
  if (!v) throw new Error(`Missing env: ${k}`);
  return v;
};

// ---------- 유틸 ----------
function splitCSVLine(line: string): string[] {
  const out: string[] = []; let cur = ""; let q = false;
  for (let i=0;i<line.length;i++){
    const c=line[i];
    if (c === '"'){ if(q && line[i+1]==='"'){cur+='"'; i++;} else q=!q; }
    else if (c === "," && !q){ out.push(cur); cur=""; }
    else cur += c;
  }
  out.push(cur);
  return out.map(s=>s.trim());
}
const clamp = (v: number, lo: number, hi: number) => Math.min(hi, Math.max(lo, v));
const toNum = (s?: string) => {
  if (s == null) return NaN;
  const n = parseFloat(String(s).replace(/[^\d.\-]/g,""));
  return Number.isFinite(n) ? n : NaN;
};
const toLines = (t: string) => t.replace(/\ufeff/g,"").split(/\r?\n/).filter(l=>l.trim().length>0);

// ---------- Influx ----------
async function writeLines(lines: string[]): Promise<void> {
  const url = `${need("INFLUX_URL")}/api/v2/write` +
              `?org=${encodeURIComponent(need("INFLUX_ORG"))}` +
              `&bucket=${encodeURIComponent(need("INFLUX_BUCKET"))}` +
              `&precision=s`;
  const res = await fetch(url, {
    method:"POST",
    headers:{ Authorization:`Token ${need("INFLUX_TOKEN")}`, "Content-Type":"text/plain; charset=utf-8" },
    body: lines.join("\n"),
  });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Influx write ${res.status}: ${body}`);
  }
}

// ---------- POP 파서 ----------
type PopRow = { ts: number; popPct: number; issueTs?: number };

function parseTimeToEpochKST(raw: string): number | null {
  // "YYYY-MM-DD HH:mm" or "YYYYMMDDHHmm" 등 처리
  const s = raw.trim();
  if (/^\d{12,14}$/.test(s)) {
    const yyyy=s.slice(0,4), MM=s.slice(4,6), dd=s.slice(6,8), HH=s.slice(8,10), mm=s.slice(10,12), ss=s.slice(12,14) || "00";
    const d = new Date(`${yyyy}-${MM}-${dd}T${HH}:${mm}:${ss}+09:00`);
    return isNaN(d.getTime()) ? null : Math.floor(d.getTime()/1000);
  }
  const d = new Date(s.replace(" ","T") + (/\+/.test(s) ? "" : "+09:00"));
  return isNaN(d.getTime()) ? null : Math.floor(d.getTime()/1000);
}

async function fetchPopSeries(reg: string): Promise<{ rows: PopRow[]; latency: number; url: string }> {
  // 최신 발표 묶음(tmfc=0), CSV(disp=1), 헤더(help=1)
  const qs = new URLSearchParams({
    reg,
    tmfc: "0",
    disp: "1",
    help: "1",
    authKey: need("APIHUB_KEY"),
  });
  const url = `${need("APIHUB_BASE")}/api/typ01/url/fct_afs_dl2.php?${qs.toString()}`;

  const t0 = Date.now();
  const res = await fetch(url);
  const latency = Date.now() - t0;
  const text = await res.text();
  if (!res.ok) throw new Error(`KMAHub ${res.status}: ${text.slice(0,200)}`);

  const raw = toLines(text);
  const data = raw.filter(l => !l.startsWith("#"));
  if (data.length === 0) throw new Error("POP: no data lines");

  // 헤더 추정
  const header = splitCSVLine(data[0]);
  const body   = data.slice(1).map(splitCSVLine).filter(r => r.length >= header.length);

  const findIdx = (re: RegExp) => header.findIndex(h => re.test(h));
  // POP 열: "POP" / "ST" / "강수확률"
  let iPOP = findIdx(/^(POP|ST)$/i);
  if (iPOP < 0) iPOP = header.findIndex(h => /강수확률/.test(h));
  if (iPOP < 0) {
    // 이름 실패 → 값 패턴(0~100 정수)로 추정
    const cand: number[] = [];
    for (let c=0;c<header.length;c++){
      const ok = body.slice(0,12).every(r => /^\d+$/.test(r[c] ?? "") && +r[c] >= 0 && +r[c] <= 100);
      if (ok) cand.push(c);
    }
    if (cand.length) iPOP = cand[0];
  }
  if (iPOP < 0) throw new Error("POP column not found");

  // 시간열: tmef/ftime/valid(예보 유효시간), 발표시각 tmfc(있으면 보조로 저장)
  const iValid = [findIdx(/^(tmef|ftime|valid|fcst)$/i)].find(i => i >= 0) ?? -1;
  const iIssue = findIdx(/^tmfc$/i);

  const out: PopRow[] = [];
  for (const r of body) {
    const popRaw = r[iPOP];
    if (!/^\d+(\.\d+)?$/.test(popRaw ?? "")) continue;

    let pop = toNum(popRaw);
    if (!isFinite(pop)) continue;
    // 0~1 스케일 들어오면 %로 환산
    if (pop <= 1) pop *= 100;
    pop = clamp(pop, 0, 100);

    // 시간 파싱(없으면 skip)
    let ts: number | null = null;
    if (iValid >= 0 && r[iValid]) ts = parseTimeToEpochKST(r[iValid]);
    if (ts == null) continue;

    // 발표시각(Optional)
    let issueTs: number | undefined;
    if (iIssue >= 0 && r[iIssue]) {
      const it = parseTimeToEpochKST(r[iIssue]);
      if (it != null) issueTs = it;
    }

    out.push({ ts, popPct: pop, issueTs });
  }

  // 중복 시간(_time) 정리: 동일 ts가 여러 줄이면 마지막 값만 남김
  const byTs = new Map<number, PopRow>();
  for (const row of out) byTs.set(row.ts, row);
  const rows = Array.from(byTs.values()).sort((a,b)=>a.ts-b.ts);

  if (!rows.length) throw new Error("POP: parsed 0 rows");

  return { rows, latency, url };
}

// ---------- 메인 ----------
(async () => {
  const reg = (env.POP_REG || "").trim();
  if (!reg) throw new Error("POP_REG 미설정");
  const loc = env.LOC?.trim() || "seoul";

  const { rows, latency, url } = await fetchPopSeries(reg);
  console.log(`POP rows=${rows.length} from: ${url}`);

  const now = Math.floor(Date.now()/1000);
  const lines: string[] = [];

  for (const r of rows) {
    const fields = [`pop_pct=${r.popPct}`];
    if (r.issueTs) fields.push(`issue_ts=${r.issueTs}i`); // 선택: 발표시각 추적
    lines.push(`pop,source=kmahub-di2,loc=${loc},reg=${reg} ${fields.join(",")} ${r.ts}`);
  }
  // 가용성/지연
  lines.push(`api_probe,service=pop_kmahub,env=prod,loc=${loc} success=1i,latency_ms=${latency}i ${now}`);

  await writeLines(lines);
  console.log(`Influx write OK (points=${rows.length})`);
})().catch(async (e) => {
  console.error(e);
  // 실패도 probe 남겨 스케줄 유지
  const now = Math.floor(Date.now()/1000);
  try {
    await writeLines([`api_probe,service=pop_kmahub,env=prod,loc=${env.LOC || "seoul"} success=0i,latency_ms=0i ${now}`]);
  } catch {}
  process.exit(0);
});

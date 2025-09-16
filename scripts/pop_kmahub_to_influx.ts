// scripts/pop_kmahub_to_influx.ts
/**
 * KMAHub 단기 육상예보 fct_afs_dl2 → POP(강수확률) 타임라인 전량 적재
 *   measurement=pop, source=kmahub-dl2, field=pop_pct(0~100, int)
 * 필요 .env / Actions:
 *   INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET
 *   APIHUB_BASE=https://apihub.kma.go.kr
 *   APIHUB_KEY=<authKey>
 *   POP_REG=<예보구역코드> (예: 11B10101)
 *   LOC=seoul (선택)
 */

type Env = {
  INFLUX_URL: string; INFLUX_TOKEN: string; INFLUX_ORG: string; INFLUX_BUCKET: string;
  APIHUB_BASE: string; APIHUB_KEY: string; POP_REG?: string; LOC?: string;
};
const env = process.env as unknown as Env;
const need = (k: keyof Env) => { const v = env[k]; if (!v) throw new Error(`Missing env: ${k}`); return v; };

function splitCSVLine(line: string): string[] {
  const out: string[] = []; let cur = ""; let q = false;
  for (let i=0;i<line.length;i++){
    const c=line[i];
    if (c === '"') { if (q && line[i+1] === '"') { cur+='"'; i++; } else { q=!q; } }
    else if (c === "," && !q) { out.push(cur); cur=""; }
    else cur += c;
  }
  out.push(cur);
  return out.map(s => s.trim());
}

function parseTsKST(raw: string): number | null {
  const s = raw.trim().replace(" ", "T");
  // YYYYMMDDHHmm or YYYYMMDDHH
  if (/^\d{10,12}$/.test(s)) {
    const yyyy = s.slice(0,4), MM=s.slice(4,6), dd=s.slice(6,8),
          HH=s.slice(8,10), mm=(s.slice(10,12) || "00");
    const iso = `${yyyy}-${MM}-${dd}T${HH}:${mm}:00+09:00`;
    const d = new Date(iso); return isNaN(d.getTime())? null : Math.floor(d.getTime()/1000);
  }
  // YYYY-MM-DDTHH:mm or "YYYY-MM-DD HH:mm"
  const iso = s.includes("T")? s : s.replace(" ", "T");
  const d = new Date(/\+/.test(iso)? iso : (iso + "+09:00"));
  return isNaN(d.getTime())? null : Math.floor(d.getTime()/1000);
}

/** fct_afs_dl2: CSV(help=1, disp=1) 기준으로 유연 파싱 */
async function fetchPopSeries(reg: string): Promise<{ rows: { ts:number; pop:number }[], url: string }> {
  const qs = new URLSearchParams({
    reg, tmfc: "0", disp: "1", help: "1", authKey: need("APIHUB_KEY"),
  });
  const url = `${need("APIHUB_BASE")}/api/typ01/url/fct_afs_dl2.php?${qs.toString()}`;
  const res = await fetch(url);
  const text = await res.text();
  if (!res.ok) throw new Error(`KMAHub ${res.status}: ${text.slice(0,200)}`);

  const lines = text.replace(/\ufeff/g,"").split(/\r?\n/).filter(l => l.trim().length>0 && !l.startsWith("#"));
  if (!lines.length) throw new Error("No lines");

  // 헤더 판별
  let headerIdx = 0;
  for (let i=0;i<Math.min(5, lines.length); i++) {
    if (/,/.test(lines[i]) && /(POP|강수확률)/i.test(lines[i])) { headerIdx = i; break; }
  }
  const header = splitCSVLine(lines[headerIdx]);
  const data = lines.slice(headerIdx+1).map(splitCSVLine).filter(r => r.length >= header.length);

  // 컬럼 탐색
  const idxPOP =
    header.findIndex(h => /^(ST|POP)$/i.test(h) || /강수확률/.test(h));
  const idxT =
    header.findIndex(h => /(tmef|ftime|time|valid|fcst)/i.test(h));

  if (idxPOP === -1) throw new Error("POP column not found");
  // 시간 컬럼이 없을 수도 있음 → 그 경우 현재시각 기준 offset이 전혀 없어 데이터가 쓸모없으니 건너뜀
  if (idxT === -1) throw new Error("Time column not found (tmef/ftime)");

  const rows: { ts:number; pop:number }[] = [];
  for (const r of data) {
    const p = r[idxPOP]?.trim() ?? "";
    if (!/^\d+$/.test(p)) continue;
    const pop = Math.max(0, Math.min(100, parseInt(p,10)));

    const ts = parseTsKST(r[idxT] || "");
    if (!ts) continue;

    rows.push({ ts, pop });
  }
  if (!rows.length) throw new Error("No numeric POP rows with valid time");
  return { rows, url };
}

async function writeLP(lines: string[]) {
  const url = `${need("INFLUX_URL")}/api/v2/write` +
              `?org=${encodeURIComponent(need("INFLUX_ORG"))}` +
              `&bucket=${encodeURIComponent(need("INFLUX_BUCKET"))}` +
              `&precision=s`;
  const res = await fetch(url, {
    method:"POST",
    headers:{ Authorization:`Token ${need("INFLUX_TOKEN")}`, "Content-Type":"text/plain; charset=utf-8" },
    body: lines.join("\n")
  });
  if (!res.ok) throw new Error(`Influx write ${res.status}: ${await res.text()}`);
}

(async () => {
  const reg = (env.POP_REG || "").trim();
  if (!reg) throw new Error("POP_REG 미설정");
  const loc = env.LOC?.trim() || "seoul";

  const { rows, url } = await fetchPopSeries(reg);

  // 과거 너무 먼 것/너무 먼 미래 잘라내기(가시화 품질 보정용)
  const now = Math.floor(Date.now()/1000);
  const minTs = now - 12*3600;
  const maxTs = now + 72*3600;

  const lines: string[] = [];
  for (const { ts, pop } of rows) {
    if (ts < minTs || ts > maxTs) continue;
    lines.push(`pop,source=kmahub-dl2,loc=${loc},reg=${reg} pop_pct=${pop}i ${ts}`);
  }

  if (!lines.length) throw new Error("No rows in time window (past 12h ~ +72h)");
  await writeLP(lines);
  console.log(`Wrote POP points: ${lines.length}\nfrom: ${url}`);
})().catch(e => { console.error(e); process.exit(1); });

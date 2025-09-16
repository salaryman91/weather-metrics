// scripts/pop_kmahub_to_influx.ts
/**
 * KMAHub fct_afs_dl2 → POP(강수확률) 타임라인 전량 적재
 * measurement=pop, source=kmahub-dl2, field=pop_pct(0~100 int), ts=예보 유효시각
 */

import * as iconv from "iconv-lite";

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
  const s = (raw || "").trim();
  if (/^\d{12,14}$/.test(s)) { // YYYYMMDDHHmm(ss)
    const yyyy=s.slice(0,4), MM=s.slice(4,6), dd=s.slice(6,8),
          HH=s.slice(8,10), mm=(s.slice(10,12)||"00"), ss=(s.slice(12,14)||"00");
    const iso = `${yyyy}-${MM}-${dd}T${HH}:${mm}:${ss}+09:00`;
    const d = new Date(iso); return isNaN(d.getTime())? null : Math.floor(d.getTime()/1000);
  }
  const iso = s.replace(" ", "T");
  const d = new Date(/\+/.test(iso)? iso : (iso + "+09:00"));
  return isNaN(d.getTime())? null : Math.floor(d.getTime()/1000);
}

async function decodeKR(res: Response): Promise<string> {
  const ab = await res.arrayBuffer();
  const buf = Buffer.from(ab);
  const ct = (res.headers.get("content-type") || "").toLowerCase();
  if (/euc-?kr|ks_c_5601|cp949/.test(ct)) return iconv.decode(buf, "euc-kr");
  const utf = buf.toString("utf8");
  if (utf.includes("\uFFFD")) return iconv.decode(buf, "euc-kr");
  return utf;
}

function pickTimeColumn(rows: string[][]): number {
  if (!rows.length) return -1;
  const cols = rows[0].length;
  // 1) 숫자 12~14자리(YYYYMMDDHHmm[ss]) 비율이 높은 열
  let best=-1, score=-1;
  for (let c=0;c<cols;c++){
    let ok=0, tot=0;
    for (const r of rows.slice(0,50)) {
      const v=r[c]||""; if (!v) continue;
      tot++; if (/^\d{12,14}$/.test(v.trim())) ok++;
      else if (/^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}/.test(v.trim())) ok++;
    }
    const fit = tot? ok/tot : 0;
    if (fit>score){score=fit;best=c;}
  }
  return best;
}

function pickPopColumn(rows: string[][]): number {
  if (!rows.length) return -1;
  const cols = rows[0].length;
  let best=-1, score=-1;
  for (let c=0;c<cols;c++){
    const vals = rows.slice(0,60).map(r => r[c] ?? "");
    const nums = vals.filter(s => /^\d+$/.test(s.trim())).map(s => parseInt(s,10));
    const ok = nums.filter(n => n>=0 && n<=100).length;
    const fit = vals.length ? ok/vals.length : 0;
    if (fit>score){score=fit;best=c;}
  }
  return best;
}

/** fct_afs_dl2: help=1, disp=1 */
async function fetchPopSeries(reg: string): Promise<{ rows: { ts:number; pop:number }[], url: string, latency: number }> {
  const qs = new URLSearchParams({ reg, tmfc:"0", disp:"1", help:"1", authKey: need("APIHUB_KEY") });
  const url = `${need("APIHUB_BASE")}/api/typ01/url/fct_afs_dl2.php?${qs}`;
  const t0 = Date.now();
  const res = await fetch(url);
  const text = await decodeKR(res);
  const latency = Date.now() - t0;

  if (!res.ok) throw new Error(`KMAHub ${res.status}: ${text.slice(0,200)}`);

  const all = text.replace(/\ufeff/g,"").split(/\r?\n/).filter(l => l.trim().length>0);
  // 헤더 후보: 주석/비주석 모두에서 탐색
  const headerIdx = (() => {
    for (let i=0;i<Math.min(10, all.length); i++) {
      const s = all[i].replace(/^#\s*/, "");
      if (s.includes(",") && /(POP|ST|강수확률)/i.test(s)) return i;
    }
    return -1;
  })();

  // CSV/공백 판별(헤더 있으면 헤더 기준, 없으면 데이터 첫줄 기준)
  const sample = (headerIdx>=0 ? all[headerIdx] : all.find(l => !l.startsWith("#")) ) || all[0];
  const csv = sample.includes(",");
  const split = (line: string) => {
    const s = line.replace(/^#\s*/, "").trim();
    return csv ? splitCSVLine(s) : s.split(/\s+/);
  };

  const header = headerIdx>=0 ? split(all[headerIdx]) : [];
  const dataLines = all.filter((l,idx) => idx>headerIdx && !/^#/.test(l));
  const rows = dataLines.map(split).filter(r => r.length >= Math.max(3, header.length || 3));
  if (!rows.length) throw new Error("No data rows");

  let iPOP = header.length ? header.findIndex(h => /^(ST|POP)$/i.test(h) || /강수확률/.test(h)) : -1;
  let iT   = header.length ? header.findIndex(h => /(tmef|ftime|time|valid|fcst)/i.test(h)) : -1;
  if (iPOP < 0) iPOP = pickPopColumn(rows);
  if (iT   < 0) iT   = pickTimeColumn(rows);
  if (iPOP < 0) throw new Error("POP column not found");
  if (iT   < 0) throw new Error("Time column not found");

  const out: { ts:number; pop:number }[] = [];
  for (const r of rows) {
    const p = (r[iPOP] ?? "").trim();
    if (!/^\d+$/.test(p)) continue;
    const pop = Math.max(0, Math.min(100, parseInt(p,10)));
    const ts = parseTsKST(r[iT] || "");
    if (!ts) continue;
    out.push({ ts, pop });
  }
  if (!out.length) throw new Error("No numeric POP rows with valid time");

  // 동일 ts 중복 → 마지막 값으로 덮어쓰기
  const dedup = new Map<number, number>();
  for (const {ts, pop} of out) dedup.set(ts, pop);
  const rowsUniq = Array.from(dedup.entries()).map(([ts,pop]) => ({ ts, pop }))
                        .sort((a,b)=>a.ts-b.ts);

  return { rows: rowsUniq, url, latency };
}

async function writeLP(lines: string[]) {
  const url = `${need("INFLUX_URL")}/api/v2/write`
    + `?org=${encodeURIComponent(need("INFLUX_ORG"))}`
    + `&bucket=${encodeURIComponent(need("INFLUX_BUCKET"))}`
    + `&precision=s`;
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

  try {
    const { rows, url, latency } = await fetchPopSeries(reg);

    const now = Math.floor(Date.now()/1000);
    const minTs = now - 12*3600;
    const maxTs = now + 72*3600;

    const lines: string[] = [];
    for (const { ts, pop } of rows) {
      if (ts < minTs || ts > maxTs) continue;
      lines.push(`pop,source=kmahub-dl2,loc=${loc},reg=${reg} pop_pct=${pop}i ${ts}`);
    }

    if (!lines.length) throw new Error("No rows in time window (past 12h ~ +72h)");

    // 가용성/지연 측정도 함께
    const probe = `api_probe,service=pop_kmahub,env=prod,loc=${loc} success=1i,latency_ms=${latency}i ${now}`;
    lines.push(probe);

    await writeLP(lines);
    console.log(`Wrote POP points: ${lines.length-1}\nfrom: ${url}`);
  } catch (e:any) {
    // 실패해도 잡 유지: 실패 프로브만 기록
    const now = Math.floor(Date.now()/1000);
    const latency = Number.isFinite(e?.latency) ? e.latency : 0;
    try {
      await writeLP([`api_probe,service=pop_kmahub,env=prod,loc=${env.LOC||"seoul"} success=0i,latency_ms=${latency}i ${now}`]);
    } catch {}
    console.error(e);
    process.exit(0);
  }
})();

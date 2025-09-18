좋습니다. 첨부해주신 스크립트/워크플로 내용을 실제로 확인해, **현재 레포 상태에 딱 맞게** 교열된 `README.md`를 완성했습니다. 그대로 붙여넣어 쓰시면 됩니다.

---

# weather-metrics

KMA(기상청) API Hub 데이터를 수집해 **InfluxDB 2.x**로 적재하고 **Grafana**로 시각화하는 경량 ETL 스크립트 모음입니다.
런타임은 Node.js 20 기준이며, 환경변수로 동작을 제어합니다.

## 포함 스크립트

* `scripts/asos_feels_to_influx.ts`
  초단기 실황(NOWCAST: `T1H`/`REH`/`WSD`)을 수집해 **열지수(Heat Index)**, **윈드칠(Wind Chill)**, **Apparent Temperature**를 계산 후 적재합니다.

  * 안정화 규칙: 기준시각이 `HH00`/`HH30`인 데이터를 사용하며, **가장 최근 유효 반시간 슬롯**을 우선 탐색(+ 실패 시 30분 간격 다중 폴백)
  * 적재: `life_index` (tags: `source=kma-ultra-ncst`, `loc`, `stn`, `method=hi|wc|at`)

* `scripts/pop_kmahub_to_influx.ts`
  단기예보(`getVilageFcst`)의 **POP(강수확률)/PCP(강수량)** 를 `forecast`로, 초단기 실황(`getUltraSrtNcst`)의 **RN1(1시간 강수)** 을 `nowcast`로 적재합니다.

  * 적재:

    * `forecast` (tags: `source=kma-vilage`, `loc`, `reg`, `nx`, `ny`)
    * `nowcast` (tags: `source=kma-ultra-ncst`, `loc`, `reg`, `nx`, `ny`)

* `scripts/uv_to_influx.ts`
  kma\_sfctm\_uv.php(관측, 10분 단위) 응답을 자동 인코딩 판별 후 파싱(EUC-KR/UTF-8)하여 UVI를 추출합니다.

  * 추출 우선순위: **UV-B(index) > EUV/25 > EUV\*40 > 휴리스틱**
  * 적재: `life_index` (tags: `source=kmahub-uv`, `loc`, `stn`, `method=uvb|euv25|euv40|heur`)

> 모든 스크립트는 공통적으로 InfluxDB/기상청 API Hub 설정을 환경변수로 읽어 동작합니다. **CLI 플래그는 사용하지 않습니다.**

---

## 데이터 스키마(Influx, precision=s)

### life\_index (feels / uv)

* **feels (ultra nowcast)**

  * tags: `source=kma-ultra-ncst`, `loc`, `stn`, `method=hi|wc|at`
  * fields: `temp_c`, `rh_pct`, `wind_ms`, `feels_c`, `heat_index_c`, `wind_chill_c`, `apparent_c`, `base_time_s`
* **uv (obs)**

  * tags: `source=kmahub-uv`, `loc`, `stn`, `method=uvb|euv25|euv40|heur`
  * fields: `uv_idx`, `base_time_s`

### forecast (vilage)

* tags: `source=kma-vilage`, `loc`, `reg`, `nx`, `ny`
* fields: `pop_pct`, `pcp_mm`, `base_time_s`

### nowcast (ultra nowcast)

* tags: `source=kma-ultra-ncst`, `loc`, `reg`, `nx`, `ny`
* fields: `rn1_mm`, `base_time_s`

### 공통 SLI(품질/가용성 프로브)

* measurement: `api_probe`
* 예) `service=feels_ultra | pop_vilage | rn1_ultra | uv_obs`, tags: `env=prod`, `loc`
* fields(스크립트별 일부 상이): `success`, `latency_ms`, `base_time_s`, `age_s`, `ver`, *(실패 시)* `note`

---

## 요구사항

* Node.js **20+**
* InfluxDB **2.x** (URL/ORG/BUCKET/TOKEN)
* KMA API Hub Key (공공데이터 포털)

---

## 설치

```bash
npm ci
```

> `package.json`에는 `typescript`, `ts-node`, `dotenv-cli`, `@types/node`, `iconv-lite`가 포함되어 있습니다.

---

## 환경변수(.env 예시)

레포 루트에 `.env` 파일을 생성하세요.

```ini
# --- InfluxDB ---
INFLUX_URL=http://localhost:8086
INFLUX_TOKEN=REPLACE_ME
INFLUX_ORG=your-org
INFLUX_BUCKET=weather-metrics

# --- KMA API Hub ---
APIHUB_BASE=https://apihub.kma.go.kr
APIHUB_KEY=REPLACE_ME

# --- Location / Grid (tags & 좌표) ---
LOC=seoul
NX=60
NY=127

# --- Station/Region codes ---
ASOS_STN=108     # asos_feels_to_influx.ts (tag 용)
POP_REG=11B10101 # pop_kmahub_to_influx.ts (예: 서울권 코드)
UV_STN=0         # uv_to_influx.ts (0=전체 또는 특정 지점 ID)

# --- Debug flags (선택) ---
DEBUG=0
DEBUG_FEELS=0
DEBUG_POP=0
DEBUG_UV=0
```

> 현재 스크립트들은 **환경변수 기반**으로만 동작합니다. (CLI 인자 없음)

---

## 실행 (로컬)

```bash
# 체감온도 + 현재 기온 (ASOS / Ultra Nowcast)
npx ts-node scripts/asos_feels_to_influx.ts

# 강수확률/강수량 (Vilage Forecast) + RN1(초단기) 적재
npx ts-node scripts/pop_kmahub_to_influx.ts

# 자외선(UVI) 관측 적재
npx ts-node scripts/uv_to_influx.ts
```

> ※ `package.json`에는 현재 `dev:pop`만 정의되어 있습니다. 원하시면 아래처럼 스크립트를 추가해 로컬 실행을 단축할 수 있습니다.
> `"dev:asos": "dotenv -e .env -- ts-node scripts/asos_feels_to_influx.ts"`
> `"dev:uv":   "dotenv -e .env -- ts-node scripts/uv_to_influx.ts"`

---

## GitHub Actions (예시 워크플로)

레포에는 다음 스케줄 워크플로 예시가 포함되어 있습니다.

* `asos.yml` : **매 30분** 체감온도 적재
* `pop.yml`  : 매시 **05, 35분**(슬롯 안정화 후) POP/PCP + RN1 적재
* `uv.yml`   : **10분마다** UVI 적재 *(스케줄 시 60–120초 지터)*

필요 **Secrets**

* `INFLUX_URL`, `INFLUX_TOKEN`, `INFLUX_ORG`, `INFLUX_BUCKET`
* `APIHUB_KEY`

필요 **Variables**

* `APIHUB_BASE` (예: `https://apihub.kma.go.kr`)
* `LOC`, `NX`, `NY`
* `ASOS_STN`, `POP_REG`, `UV_STN`

> 워크플로 파일 경로는 `.github/workflows/*.yml`를 권장합니다. (`uv.yml`에도 `APIHUB_BASE` 변수를 함께 지정하면 구성 일관성이 높아집니다.)

---

## 운영 메모

* **초단기 실황 안정화**: 기준시각이 `HH00`/`HH30`인 점을 고려해, 분(minute) 기준으로 최근 유효 슬롯을 선택하고 **30분 단위 폴백**을 적용합니다. (일반적으로 **기준시각+10분** 이후가 유효)
* **타임스탬프**: 저장 정밀도는 초(precision=s). Feels/POP/RN1/UV 모두 `base_time_s` 필드에 기준시각(슬롯)을 함께 기록합니다.
* **품질 지표**: `api_probe`로 success/latency/age 등을 함께 적재하므로 Grafana 알림 조건으로 활용 가능합니다.
* **로깅/디버그**: `DEBUG_*` 플래그로 샘플 포인트/슬롯 선택 로그를 확인할 수 있습니다.

---

## 라이선스

ISC

---

## 링크

* Repository: [https://github.com/salaryman91/weather-metrics](https://github.com/salaryman91/weather-metrics)

---

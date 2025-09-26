# DART XBRL Financial Data Crawler 📊

DART(전자공시시스템)에서 XBRL 재무제표 데이터를 자동 수집하고, 구조화된 Parquet 파일로 변환하여 S3에 저장하는 완전 자동화된 크롤링 시스템입니다.

## 🎯 시스템 개요

### 전체 아키텍처
```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Corp Map API  │ <- │   XBRL Crawler   │ -> │   S3 Parquet    │ -> │   AWS Athena    │
│     Lambda      │    │     Lambda       │    │     Files       │    │   Analytics     │
│                 │    │                  │    │                 │    │                 │
│ • Athena 쿼리   │    │ • DART API 호출  │    │ • 파티션 구조   │    │ • SQL 쿼리      │
│ • 회사 목록     │    │ • XBRL 파싱      │    │ • Parquet 포맷  │    │ • 데이터 분석   │
│ • 캐싱 시스템   │    │ • 데이터 변환    │    │ • 자동 업로드   │    │                 │
└─────────────────┘    └──────────────────┘    └─────────────────┘    └─────────────────┘
        │                        │                        │
        │                        │                        │
        ▼                        ▼                        ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│  Glue Catalog   │    │   DART OpenAPI   │    │   CloudWatch    │
│                 │    │                  │    │                 │
│ • table_corp_map│    │ • 공시목록 API   │    │ • 실행 로그     │
│ • 스키마 관리   │    │ • XBRL 다운로드  │    │ • 오류 모니터링 │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

## 🔄 데이터 처리 프로세스

### 1단계: 회사 목록 수집
```
Corp Map API Lambda → Athena Query → Glue Catalog
                   ↓
            회사코드-회사명 매핑 (JSON)
                   ↓
            DART_CORP_CODE 필터링
                   ↓
        실제 처리 대상 회사 목록 반환
```

### 2단계: DART API 크롤링
```
DART OpenAPI 호출 → 최근 6개월 공시목록 조회
        ↓
XBRL 첨부파일 다운로드 → ZIP 압축해제
        ↓
재무제표 XBRL 파일 추출 → 메타데이터 매핑
        ↓
    rcept_dt 매핑파일 생성
```

### 3단계: XBRL 데이터 처리
```
XBRL 파일 파싱 → 재무제표 추출 (BS, CIS)
        ↓
다차원 데이터 → 2차원 테이블 변환
        ↓
계층구조 개선 → "총계" 제거, 분류체계 정리
        ↓
    Parquet 포맷 변환
```

### 4단계: S3 파티션 업로드
```
S3://bucket/prefix/year=2025/mm=09/
        ↓
파일명: corp_code=00171636_report_type=BS_receipt_ymd=20250926.parquet
        ↓
    Athena 테이블 자동 파티션 인식
```

## 📁 프로젝트 구조

```
xbrl-analyzer/
├── 🚀 Main Lambda Components
│   ├── lambda_function.py          # AWS Lambda 진입점
│   ├── dart_api_manager.py         # DART API 통신 & 회사목록 관리
│   ├── xbrl_processor.py           # XBRL 파싱 & 데이터 변환
│   ├── xbrl_batch_processor.py     # 배치 처리 orchestration
│   └── s3_uploader.py             # S3 파티션 업로드
│
├── 🗄️ Corp Map API Lambda
│   └── fixed_corp_map_lambda.py    # 회사목록 조회 API (LPAD 적용)
│
├── ⚙️ Configuration
│   ├── .env                       # 환경변수 (Local 개발용)
│   ├── requirements.txt           # Python 의존성
│   ├── Dockerfile                 # Container 설정
│   └── .dockerignore             # Docker 빌드 제외
│
└── 📚 Documentation
    └── README.md                  # 이 파일
```

## 🔧 환경 설정

### 환경변수 우선순위
1. **Lambda 환경변수** (운영 환경)
2. **.env 파일** (로컬 개발 환경)

### 필수 환경변수

#### XBRL Crawler Lambda
```env
# DART API
DART_API_KEY=your_dart_api_key

# S3 Storage
S3_BUCKET_NAME=hds-dap-dev-an2-datalake-01
S3_PREFIX=l0/ver=1/sys=dart/loc=common/table=dart_report_from_xbrl/

# Corp Map API Integration
CORP_LIST_SOURCE=api
CORP_MAP_API_URL=https://YOUR_FUNCTION_URL.lambda-url.ap-northeast-2.on.aws/
```

#### Corp Map API Lambda
```env
# Athena Configuration
ATHENA_DATABASE=dev_fi_l0_database
ATHENA_TABLE=table_corp_map
ATHENA_OUTPUT_S3=s3://hds-dap-dev-an2-datalake-01/athena-results/
CORP_CACHE_TTL_HOURS=24
```

## 🚀 배포 가이드

### 1. Corp Map API Lambda 배포

```bash
# 1. Corp Map API Docker 빌드
cp fixed_corp_map_lambda.py lambda_function.py
docker build -t corp-map-api .

# 2. ECR 푸시 & Lambda 배포
aws ecr get-login-password --region ap-northeast-2 | docker login --username AWS --password-stdin <account>.dkr.ecr.ap-northeast-2.amazonaws.com
docker tag corp-map-api:latest <account>.dkr.ecr.ap-northeast-2.amazonaws.com/corp-map-api:latest
docker push <account>.dkr.ecr.ap-northeast-2.amazonaws.com/corp-map-api:latest

# 3. Lambda Function URL 생성 (AuthType: NONE)
aws lambda create-function-url-config \
  --function-name corp-map-api \
  --auth-type NONE \
  --cors '{"AllowOrigins":["*"],"AllowMethods":["GET"],"AllowHeaders":["content-type"]}' \
  --region ap-northeast-2
```

### 2. XBRL Crawler Lambda 배포

```bash
# 1. Main Lambda Docker 빌드
cp lambda_function.py lambda_function.py  # 이미 올바른 파일
docker build -t dart-xbrl-crawler .

# 2. ECR 푸시 & Lambda 배포
docker tag dart-xbrl-crawler:latest <account>.dkr.ecr.ap-northeast-2.amazonaws.com/dart-xbrl-crawler:latest
docker push <account>.dkr.ecr.ap-northeast-2.amazonaws.com/dart-xbrl-crawler:latest

# 3. Lambda 함수 업데이트
aws lambda update-function-code \
  --function-name xbrl-analyzer \
  --image-uri <account>.dkr.ecr.ap-northeast-2.amazonaws.com/dart-xbrl-crawler:latest \
  --region ap-northeast-2
```

## 🗃️ 출력 데이터 스키마

### Parquet 파일 구조
| 컬럼명 | 타입 | 설명 | 예시 |
|--------|------|------|------|
| `order_no` | int | 항목 순서 | 1, 2, 3... |
| `year` | string | 보고연도 | "2025" |
| `mm` | string | 보고월 | "06", "12" |
| `receipt_ymd` | string | 접수일자 | "20250926" |
| `corp_code` | string | 8자리 기업코드 | "00171636" |
| `corp_name` | string | 기업명 | "한솔홀딩스" |
| `report_type` | string | 보고서유형 | "BS", "CIS" |
| `account_id` | string | IFRS 개념ID | "ifrs-full_PropertyPlantAndEquipment" |
| `account_name` | string | 항목명(한글) | "유형자산" |
| `account_name_en` | string | 항목명(영문) | "Property, plant and equipment" |
| `class1` | string | 1차분류 | "자산", "부채", "자본" |
| `class2` | string | 2차분류 | "비유동자산" |
| `class3` | string | 3차분류 | "유형자산" |
| `class1_id` | string | 1차분류id | "자산", "부채", "자본" |
| `class2_id` | string | 2차분류id | "비유동자산" |
| `class3_id` | string | 3차분류id | "유형자산" |
| `fs_type` | string | 재무제표구분 | "연결", "별도" |
| `period` | string | 보고기간 | "2025-06-30" |
| `amount` | double | 금액(원) | 77370233000.0 |
| `crawl_time` | string | 처리시간 | "2025-09-26 15:30:45" |

### S3 파티션 구조
```
s3://hds-dap-dev-an2-datalake-01/l0/ver=1/sys=dart/loc=common/table=dart_report_from_xbrl/
├── year=2025/
│   ├── mm=06/
│   │   ├── corp_code=00171636_report_type=BS_receipt_ymd=20250926.parquet
│   │   ├── corp_code=00171636_report_type=CIS_receipt_ymd=20250926.parquet
│   │   └── ...
│   └── mm=09/
│       └── ...
└── year=2024/
    └── ...
```

## 📊 주요 특징

### ✨ 자동화 기능
- **완전 자동화**: 회사목록 조회부터 S3 업로드까지 전 과정 자동화
- **스마트 캐싱**: Corp Map API 24시간 캐싱으로 성능 최적화
- **오류 복구**: Corp Map API 실패 시 JSON 파일 Fallback
- **파티션 관리**: 년도/월별 자동 파티션 생성

### 🔧 데이터 품질 관리
- **접수일자 매핑**: DART API rcept_dt를 receipt_ymd로 정확 매핑
- **회사코드 표준화**: LPAD로 8자리 0-padding 처리 (171636 → 00171636)
- **"총계" 정리**: BS 데이터에서 "자산총계" → "자산" 변환
- **기간 필터링**: 보고서 기간과 무관한 과거 데이터 제거

### 🚀 성능 최적화
- **Parquet 포맷**: CSV 파싱 오류 방지 및 Athena 성능 향상
- **배치 처리**: 다중 회사 동시 처리
- **메모리 효율**: 대용량 XBRL 파일 스트리밍 처리
- **중복 제거**: 동일 파일 재처리 방지

## ⚠️ 주의사항

### 🔴 필수 확인사항

#### 1. Lambda Function URL 설정
```bash
# Corp Map API Lambda AuthType이 NONE인지 확인
aws lambda get-function-url-config --function-name corp-map-api --region ap-northeast-2

# AuthType이 AWS_IAM이면 403 Forbidden 발생
```

#### 2. IAM 권한 설정
```yaml
Corp Map API Lambda:
  - AmazonAthenaFullAccess
  - AmazonS3FullAccess (athena-results 경로)
  - AWSGlueConsoleFullAccess

XBRL Crawler Lambda:
  - 기본 Lambda 실행 권한
  - S3 업로드 권한 (target bucket)
```

#### 3. 환경변수 검증
```bash
# 실행 로그에서 환경변수 로드 상태 확인
aws logs tail /aws/lambda/xbrl-analyzer --region ap-northeast-2

# 다음과 같이 표시되어야 함:
# [ENV] ✅ CORP_MAP_API_URL: https://...
# [ENV] ✅ CORP_LIST_SOURCE: api
```

### ⚠️ 운영 시 주의사항

#### 1. DART API 제한
- **Rate Limit**: 분당 1000회 호출 제한
- **서비스 시간**: DART API 점검 시간 확인 필요
- **API Key 갱신**: 정기적인 API Key 업데이트 필요

#### 2. 데이터 품질
- **XBRL 파일 구조**: 회사별 XBRL 스키마 차이로 인한 파싱 오류 가능
- **재무제표 기간**: 분기/반기/연간 보고서 기간 혼재 주의
- **회사명 변경**: corp_map 테이블과 DART API 간 회사명 불일치 가능

#### 3. 리소스 관리
- **Lambda 타임아웃**: 15분 제한, 대량 처리 시 분할 실행 필요
- **메모리 사용량**: 대용량 XBRL 파일 처리 시 메모리 부족 가능
- **S3 용량**: 파티션 누적으로 인한 스토리지 비용 증가

## 📈 Athena 쿼리 예시

### 특정 기업 재무상태표 조회
```sql
SELECT
    receipt_ymd,
    period,
    fs_type,
    class1,
    class2,
    class3,
    label_ko,
    amount
FROM table_dart_report_from_xbrl
WHERE corp_name = '한솔홀딩스'
    AND report_type = 'BS'
    AND year = '2025'
    AND mm = '06'
    AND fs_type = '연결'
ORDER BY class1, class2, class3;
```

### 업종별 유형자산 비교
```sql
SELECT
    corp_name,
    amount / 1000000000 as amount_billions
FROM table_dart_report_from_xbrl
WHERE report_type = 'BS'
    AND label_ko = '유형자산'
    AND fs_type = '연결'
    AND year = '2025'
    AND mm = '06'
ORDER BY amount DESC
LIMIT 10;
```

## 🔄 변경 이력

### v3.0.0 (2025-09-26)
- **[FEATURE]** Corp Map API Lambda 분리 및 마이크로서비스 아키텍처 도입
- **[FEATURE]** DART_CORP_CODE LPAD 처리로 8자리 표준화
- **[FEATURE]** 환경변수 우선순위 시스템 (Lambda > .env)
- **[FEATURE]** receipt_ymd 매핑 시스템으로 접수일자 정확성 개선
- **[FIX]** Corp Map API 403 Forbidden 오류 해결 (AuthType NONE)
- **[ENHANCEMENT]** "총계" 제거 로직으로 BS 데이터 품질 향상

### v2.0.0 (2025-09-22)
- **[BREAKING]** CSV → Parquet 포맷 변경
- **[FIX]** 쉼표 포함 텍스트 파싱 오류 해결
- **[FEATURE]** S3 파티션 구조 도입

### v1.0.0 (초기 버전)
- 기본 XBRL 크롤링 및 처리 기능
- CSV 포맷 출력
- 단일 Lambda 아키텍처

## 📞 문의

시스템 관련 문의사항이나 오류 발생 시:
1. **CloudWatch 로그** 우선 확인
2. **환경변수 설정** 검증
3. **IAM 권한** 확인
4. **개발팀 문의**

---

**최종 업데이트**: 2025-09-26
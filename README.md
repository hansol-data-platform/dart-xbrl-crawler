# XBRL Financial Data Analyzer 📊

한국 DART(전자공시시스템)에서 다운로드한 XBRL 재무제표 파일을 분석하여 구조화된 데이터로 변환하는 도구입니다.

## 🎯 주요 기능

### 📋 데이터 처리 파이프라인
```
XBRL 파일 → 재무제표 추출 → 피벗 변환 → 기간 필터링 → 계층구조 개선 → Parquet 저장
```

### 🔧 핵심 기능
- **XBRL 파싱**: DART XBRL 파일에서 연결재무상태표, 연결손익계산서 추출
- **피벗 변환**: 다차원 매트릭스 구조를 분석 가능한 행-열 테이블로 변환
- **기간 필터링**: 보고서 기간과 무관한 과거 데이터 자동 제거
- **계층구조 개선**: 재무상태표 분류체계 정리 및 최적화
- **Parquet 출력**: CSV 파싱 오류 방지 및 Athena 성능 최적화

### 📈 지원 재무제표
- **연결재무상태표** (Consolidated Balance Sheet)
- **연결손익계산서** (Consolidated Income Statement)

## 🏗️ 아키텍처

### 시스템 구성도
```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   DART XBRL     │ -> │  XBRLProcessor   │ -> │   S3 Parquet    │
│   Files         │    │                  │    │   Files         │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                │
                                ▼
                       ┌──────────────────┐
                       │  AWS Glue        │
                       │  Crawler         │
                       └──────────────────┘
                                │
                                ▼
                       ┌──────────────────┐
                       │  Amazon Athena   │
                       │  Query Engine    │
                       └──────────────────┘
```

### 데이터 흐름
1. **입력**: DART에서 다운로드한 ZIP 파일 내 XBRL 파일
2. **처리**: XBRLProcessor를 통한 데이터 변환
3. **저장**: S3에 Parquet 포맷으로 저장
4. **인덱싱**: Glue Crawler를 통한 스키마 등록
5. **분석**: Athena를 통한 SQL 쿼리

## 📁 프로젝트 구조

```
xbrl-analyzer/
├── xbrl_processor.py           # 메인 처리 엔진
├── lambda_function.py          # AWS Lambda 핸들러
├── requirements_lambda_py313.txt # 의존성 목록
├── Dockerfile                  # 컨테이너 설정
├── .dockerignore              # Docker 빌드 제외 파일
├── corp_list.json             # 기업코드-기업명 매핑
└── README.md                  # 프로젝트 문서
```

## 🗃️ 출력 데이터 스키마

### Parquet 파일 구조
| 컬럼명 | 타입 | 설명 | 예시 |
|--------|------|------|------|
| `order_no` | int | 항목 순서 번호 | 1, 2, 3... |
| `yyyy` | string | 보고 연도 | "2025" |
| `month` | string | 보고 월 | "06", "12" |
| `corp_code` | string | 8자리 기업코드 | "00171636" |
| `corp_name` | string | 기업명 | "한솔홀딩스" |
| `report_type` | string | 보고서 유형 | "BS", "CIS" |
| `concept_id` | string | IFRS 개념 식별자 | "ifrs-full_PropertyPlantAndEquipment" |
| `label_ko` | string | 항목명 (한글) | "유형자산" |
| `label_en` | string | 항목명 (영문) | "Property, plant and equipment" |
| `class0` | string | 최상위 분류 | "ifrs-full:StatementOfFinancialPositionAbstract" |
| `class1` | string | 1차 분류 | "자산총계" |
| `class2` | string | 2차 분류 | "비유동자산" |
| `class3` | string | 3차 분류 | "유형자산" |
| `fs_type` | string | 재무제표 구분 | "연결", "별도" |
| `period` | string | 보고 기간 | "2025-06-30" |
| `amount` | double | 금액 (원) | 77370233000.0 |
| `crawl_time` | string | 처리 시간 | "2025-09-22 06:48:46" |

### 샘플 데이터
```
order_no: 18
corp_code: 00171636
corp_name: 한솔홀딩스
report_type: BS
concept_id: ifrs-full_PropertyPlantAndEquipment
label_ko: 유형자산
label_en: Property, plant and equipment
class1: 자산총계
class2: 비유동자산
class3: 유형자산
fs_type: 연결
period: 2025-06-30
amount: 77370233000.0
```

## 🚀 설치 및 실행

### 로컬 환경

#### 1. 의존성 설치
```bash
pip install -r requirements_lambda_py313.txt
```

#### 2. 실행
```bash
python xbrl_processor.py path/to/entity00171636_2025-06-30.xbrl
```

### AWS Lambda 배포

#### 1. Docker 이미지 빌드
```bash
docker build -t xbrl-analyzer .
```

#### 2. ECR에 푸시
```bash
# ECR 로그인
aws ecr get-login-password --region ap-northeast-2 | docker login --username AWS --password-stdin {account}.dkr.ecr.ap-northeast-2.amazonaws.com

# 이미지 태깅
docker tag xbrl-analyzer:latest {account}.dkr.ecr.ap-northeast-2.amazonaws.com/xbrl-analyzer:latest

# 푸시
docker push {account}.dkr.ecr.ap-northeast-2.amazonaws.com/xbrl-analyzer:latest
```

#### 3. Lambda 함수 업데이트
AWS Lambda 콘솔에서 컨테이너 이미지를 업데이트합니다.

## ⚙️ 주요 설정

### 기간 필터링 설정
```python
# xbrl_processor.py 내부
ENABLE_PERIOD_FILTERING = True  # 기간 필터링 활성화/비활성화
```

### 디버그 모드 설정
```python
# XBRLProcessor 클래스 내부
self.debug_mode = False  # 프로덕션: False, 개발: True
```

## 📊 사용 예시

### Athena 쿼리 예시

#### 1. 특정 기업의 유형자산 조회
```sql
SELECT
    yyyy, month, period, fs_type, amount
FROM table_dart_report_from_xbrl
WHERE corp_name = '한솔홀딩스'
    AND report_type = 'BS'
    AND label_ko = '유형자산'
    AND year = '2025'
    AND mm = '06'
ORDER BY period, fs_type;
```

#### 2. 재무상태표 주요 항목 비교
```sql
SELECT
    label_ko,
    fs_type,
    amount,
    period
FROM table_dart_report_from_xbrl
WHERE corp_code = '00171636'
    AND report_type = 'BS'
    AND class1 = '자산총계'
    AND label_ko IN ('유형자산', '무형자산', '투자자산')
    AND year = '2025'
    AND mm = '06'
ORDER BY label_ko, fs_type;
```

## 🐛 트러블슈팅

### 일반적인 문제들

#### 1. pyarrow 의존성 오류
```
Error: Missing optional dependency 'pyarrow'
```
**해결**: requirements.txt에 pyarrow 추가 확인
```bash
pip install pyarrow>=15.0.0
```

#### 2. CSV 파싱 오류 (이전 버전)
```
Error: 유형자산 데이터가 Athena에서 조회되지 않음
```
**해결**: Parquet 포맷 사용으로 해결됨 (현재 버전)

#### 3. XBRL 파일 경로 오류
```
Error: XBRL 파일을 찾을 수 없습니다
```
**해결**:
- 파일 경로가 올바른지 확인
- ZIP 파일인 경우 압축 해제 후 .xbrl 파일 사용

## 🔧 개발자 가이드

### 코드 구조

#### XBRLProcessor 클래스
```python
class XBRLProcessor:
    def __init__(self):
        # 기업명 매핑 로드 및 초기화

    def extract_financial_data(self, xbrl_path):
        # XBRL에서 재무제표 데이터 추출

    def convert_to_pivot_format(self, df, metadata):
        # 다차원 데이터를 2차원 테이블로 변환

    def improve_hierarchy_structure(self, df):
        # 재무상태표 계층구조 개선

    def save_to_parquet(self, df, output_path):
        # Parquet 포맷으로 저장
```

### 확장 가능성

#### 새로운 재무제표 유형 추가
1. `extract_financial_data()` 메서드에서 추가 재무제표 추출 로직 구현
2. `convert_to_pivot_format()` 메서드에서 새로운 report_type 처리 추가
3. 필요시 `improve_hierarchy_structure()` 메서드에서 특화 로직 추가

#### 추가 메타데이터 처리
1. `extract_metadata_from_xbrl()` 메서드에서 새로운 메타데이터 추출
2. 출력 스키마에 새로운 컬럼 추가
3. Glue Crawler 스키마 업데이트

## 📝 변경 이력

### v2.0.0 (2025-09-22)
- **[BREAKING]** CSV에서 Parquet 포맷으로 변경
- **[FIX]** 쉼표가 포함된 텍스트로 인한 파싱 오류 해결
- **[FEATURE]** 상세한 코드 주석 및 문서화 추가
- **[FEATURE]** pyarrow 의존성 추가

### v1.0.0 (이전 버전)
- 기본 XBRL 처리 기능
- CSV 포맷 출력
- 기간 필터링 기능

## 📄 라이선스

이 프로젝트는 내부 사용을 위한 도구입니다.

## 👥 기여자

- **개발**: XBRL 데이터 분석팀
- **문서화**: Claude AI Assistant

## 📞 문의

프로젝트 관련 문의사항이 있으시면 개발팀에 연락해주세요.
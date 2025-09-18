# XBRL Analyzer 시스템 개발 계획서

## 1. 프로젝트 개요

### 목적
- 한솔 그룹 계열사들의 DART XBRL 재무제표 데이터를 자동으로 수집, 분석, 변환하여 S3에 파티션 형태로 저장
- 현재 로컬 실행 환경에서 AWS Lambda 기반 서버리스 환경으로 확장

### 현재 상황 분석
- **회사 목록**: 17개 한솔 그룹 계열사 (corp_list.json)
- **기존 처리 파이프라인**: XBRL → CSV 추출 → 피벗 변환 → 메타데이터 추가 → Excel/CSV 출력
- **환경설정**: DART API 키, S3 버킷 정보 완료
- **S3 저장 경로**: `l0/ver=1/sys=dart/loc=common/table=financial_reports_detail/year={year}/mm={month}/`

## 2. 시스템 아키텍처

### 2.1 현재 아키텍처 (로컬 실행)
```
corp_list.json → DART API → XBRL 다운로드 →
순차 처리 (각 회사별) →
CSV 추출 → 피벗 변환 → 메타데이터 추가 →
최종 파일 생성 (CSV + Excel)
```

### 2.2 목표 아키텍처 (AWS Lambda + S3)
```
Lambda 트리거 → corp_list.json 로드 →
DART API 호출 (최근 180일) →
병렬 처리 (각 XBRL 파일) →
파티션별 S3 저장 (year/month 구조)
```

## 3. 구현 단계

### Phase 1: 현재 시스템 통합 및 최적화 (1주차)

#### 3.1 메인 오케스트레이터 개발
- **파일명**: `xbrl_batch_processor.py`
- **기능**:
  - corp_list.json 로드
  - 각 회사별 최근 180일 XBRL 파일 검색
  - 순차 또는 병렬 처리 옵션
  - 처리 진행상황 로깅

#### 3.2 DART API 통합 모듈
- **파일명**: `dart_api_manager.py`
- **기능**:
  - 회사별 최근 180일 공시 목록 조회
  - XBRL 파일 다운로드 및 압축 해제
  - API 호출 제한 관리 (Rate limiting)

#### 3.3 파일 매니저 모듈
- **파일명**: `file_manager.py`
- **기능**:
  - 임시 디렉터리 관리
  - 처리 완료 파일 정리
  - 중간 파일 삭제 로직

### Phase 2: AWS Lambda 대응 준비 (2주차)

#### 2.1 Lambda 호환 구조 변경
- **파일명**: `lambda_handler.py`
- **기능**:
  - Lambda 이벤트 처리
  - 환경변수 기반 설정 로드
  - 오류 처리 및 재시도 로직

#### 2.2 S3 통합 모듈
- **파일명**: `s3_manager.py`
- **기능**:
  - 파티션 경로 생성 (year={year}/mm={month})
  - 파일 업로드 및 메타데이터 설정
  - 중복 파일 처리 로직

#### 2.3 메모리 및 처리시간 최적화
- 메모리 사용량 모니터링
- Lambda 15분 제한 고려한 배치 사이즈 조정
- 필요시 Step Functions 활용 검토

### Phase 3: 고도화 및 운영 준비 (3주차)

#### 3.1 모니터링 및 알림
- CloudWatch 로그 통합
- 처리 실패 알림 (SNS)
- 처리 통계 대시보드

#### 3.2 데이터 품질 관리
- 처리 결과 검증 로직
- 누락 데이터 감지
- 데이터 무결성 체크

## 4. 핵심 모듈 설계

### 4.1 메인 프로세서 (xbrl_batch_processor.py)
```python
class XBRLBatchProcessor:
    def __init__(self, config):
        self.corp_list = self.load_corp_list()
        self.dart_api = DARTAPIManager(config.dart_api_key)
        self.s3_manager = S3Manager(config.s3_bucket, config.s3_prefix)

    def process_all_companies(self, days_back=180):
        # 모든 회사에 대해 최근 180일 XBRL 처리

    def process_single_company(self, corp_code, days_back=180):
        # 단일 회사 처리

    def cleanup_temp_files(self):
        # 임시 파일 정리
```

### 4.2 DART API 매니저 (dart_api_manager.py)
```python
class DARTAPIManager:
    def get_recent_disclosures(self, corp_code, days_back=180):
        # 최근 180일 공시 목록 조회

    def download_xbrl_file(self, rcept_no):
        # XBRL 파일 다운로드 및 압축 해제

    def handle_rate_limiting(self):
        # API 호출 제한 관리
```

### 4.3 S3 매니저 (s3_manager.py)
```python
class S3Manager:
    def upload_processed_file(self, file_path, corp_info, report_date):
        # 파티션 경로: year={year}/mm={month}/

    def generate_partition_path(self, report_date):
        # 파티션 경로 생성

    def check_file_exists(self, s3_key):
        # 중복 파일 체크
```

## 5. 파티션 전략

### 5.1 S3 파티션 구조
```
s3://hds-dap-dev-an2-datalake-01/
└── l0/ver=1/sys=dart/loc=common/table=financial_reports_detail/
    ├── year=2024/
    │   ├── mm=09/
    │   │   ├── corp_code={corp_code}/
    │   │   │   ├── {corp_name}_{report_date}_{report_type}_pivot_metadata.csv
    │   │   │   └── {corp_name}_{report_date}_{report_type}_pivot_metadata.xlsx
    │   │   └── ...
    │   └── mm=10/
    └── year=2025/
```

### 5.2 파일 네이밍 규칙
- **포맷**: `{corp_name}_{report_date}_{report_type}_pivot_metadata.{ext}`
- **예시**: `한솔홀딩스_2025-06-30_연결재무상태표_pivot_metadata.csv`

## 6. 환경별 설정

### 6.1 로컬 환경
```python
# config.py
LOCAL_CONFIG = {
    'dart_api_key': os.getenv('DART_API_KEY'),
    'temp_dir': './temp',
    'output_dir': './output',
    'parallel_workers': 2
}
```

### 6.2 Lambda 환경
```python
# lambda_config.py
LAMBDA_CONFIG = {
    'dart_api_key': os.getenv('DART_API_KEY'),
    's3_bucket': os.getenv('S3_BUCKET_NAME'),
    's3_prefix': os.getenv('S3_PREFIX'),
    'temp_dir': '/tmp',
    'max_memory_mb': 3008,
    'timeout_seconds': 900
}
```

## 7. 에러 처리 및 재시도 전략

### 7.1 에러 분류
- **일시적 오류**: 네트워크, API 제한 → 재시도
- **영구적 오류**: 잘못된 데이터, 권한 문제 → 스킵 후 로깅
- **시스템 오류**: 메모리 부족, 타임아웃 → 중단 후 알림

### 7.2 재시도 로직
```python
@retry(max_attempts=3, backoff_strategy='exponential')
def process_xbrl_file(self, xbrl_path):
    # XBRL 파일 처리 로직
```

## 8. 모니터링 및 로깅

### 8.1 로그 레벨
- **INFO**: 처리 시작/완료, 파일 개수
- **WARNING**: 일부 파일 처리 실패
- **ERROR**: 전체 처리 실패

### 8.2 메트릭스
- 처리된 회사 수
- 처리된 XBRL 파일 수
- 처리 시간
- 에러율

## 9. 테스트 전략

### 9.1 단위 테스트
- 각 모듈별 기능 테스트
- 가짜 데이터를 활용한 테스트

### 9.2 통합 테스트
- 실제 DART API 호출 테스트 (소량)
- S3 업로드 테스트

### 9.3 성능 테스트
- Lambda 메모리/시간 제한 테스트
- 대용량 파일 처리 테스트

## 10. 배포 계획

### 10.1 로컬 테스트
- 1-2개 회사 대상 테스트
- 전체 파이프라인 검증

### 10.2 스테이징 배포
- Lambda 함수 배포
- 소량 데이터 처리 테스트

### 10.3 프로덕션 배포
- 전체 회사 대상 배포
- 모니터링 활성화

## 11. 일정 및 마일스톤

### Week 1: 기반 구축
- [ ] 메인 오케스트레이터 개발
- [ ] DART API 통합
- [ ] 로컬 환경 전체 파이프라인 테스트

### Week 2: 클라우드 대응
- [ ] Lambda 호환 구조 변경
- [ ] S3 통합 모듈 개발
- [ ] 파티션 로직 구현

### Week 3: 운영 준비
- [ ] 모니터링 설정
- [ ] 에러 처리 고도화
- [ ] 성능 최적화

### Week 4: 배포 및 검증
- [ ] 스테이징 배포
- [ ] 프로덕션 배포
- [ ] 운영 안정성 검증

## 12. 리스크 및 대응방안

### 12.1 기술적 리스크
- **Lambda 타임아웃**: Step Functions으로 워크플로 분할
- **메모리 부족**: 배치 사이즈 조정, 메모리 최적화
- **API 제한**: Rate limiting, 재시도 로직

### 12.2 데이터 리스크
- **XBRL 포맷 변경**: 파싱 로직 유연성 확보
- **데이터 누락**: 검증 로직 강화
- **중복 처리**: 멱등성 보장

## 13. 성공 지표

### 13.1 기능적 지표
- [ ] 17개 회사 100% 처리 성공
- [ ] 최근 180일 데이터 완전 수집
- [ ] 파티션 구조 정확성 100%

### 13.2 운영적 지표
- [ ] 일일 자동 실행 성공률 > 95%
- [ ] 평균 처리 시간 < 30분 (전체 회사)
- [ ] 에러 발생시 5분 내 알림

이 계획서를 바탕으로 단계적으로 시스템을 구축해 나가겠습니다. 각 단계마다 검토와 피드백을 통해 최적의 솔루션을 만들어 보겠습니다.
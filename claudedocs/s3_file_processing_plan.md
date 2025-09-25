# S3 파일 처리 개발 계획서

## 1. 현재 구조 분석

### 1.1 기존 corp_list.json 의존성
- **파일 위치**: 로컬 파일 시스템의 `corp_list.json`
- **사용 위치**:
  - `dart_api_manager.py:56` - 회사 목록 로드
  - `xbrl_processor.py:146-151` - 회사명 매핑 로드
  - Lambda 환경에서도 컨테이너에 포함하여 사용 중

### 1.2 현재 처리 흐름
```
corp_list.json(로컬) → DART API 호출 → XBRL 다운로드 → 처리 → S3 업로드
```

## 2. S3 기반 처리 구조 설계

### 2.1 목표 아키텍처
```
S3 corp_list.json → Lambda 다운로드 → DART API 호출 → XBRL 처리 → S3 업로드
```

### 2.2 S3 파일 구조
```
s3://your-bucket/
├── config/
│   └── corp_list.json              # 회사 목록 설정 파일
├── raw/
│   └── xbrl/                       # 원본 XBRL 파일들
└── processed/
    └── l0/ver=1/sys=dart/          # 처리된 결과 파일들
        └── year=2025/mm=06/
```

## 3. 개발 계획

### 3.1 1단계: S3 Configuration Manager 개발
**파일**: `s3_config_manager.py`

**주요 기능**:
- S3에서 `corp_list.json` 다운로드
- Lambda 캐시 메커니즘 구현
- 설정 파일 검증 및 오류 처리

**구현 포인트**:
- Lambda `/tmp` 디렉토리 활용
- 동일 Lambda 인스턴스에서 재사용을 위한 캐싱
- 설정 파일 변경 감지 메커니즘

### 3.2 2단계: 기존 코드 리팩토링
**수정 대상 파일**:
1. `dart_api_manager.py`
   - `load_corp_list()` 메서드 수정
   - S3ConfigManager 통합

2. `xbrl_processor.py`
   - `_load_corp_name_mapping()` 메서드 수정
   - S3 기반 매핑 로드

3. `lambda_function.py`
   - 초기화 단계에서 S3 설정 로드
   - 환경 변수 추가: `S3_CONFIG_PATH`

### 3.3 3단계: 환경 변수 및 배포 설정
**추가 환경 변수**:
```
S3_CONFIG_BUCKET=your-config-bucket
S3_CONFIG_PATH=config/corp_list.json
S3_CONFIG_CACHE_TTL=3600  # 1시간 캐시
```

**Dockerfile 수정**:
- 로컬 `corp_list.json` 복사 제거
- 런타임에 S3에서 다운로드하도록 변경

### 3.4 4단계: 배포 및 테스트 전략
1. **로컬 테스트**:
   - `.env` 파일에 S3 설정 추가
   - 로컬에서 S3 연동 테스트

2. **Lambda 테스트**:
   - 테스트 모드로 1개 회사 처리
   - S3 설정 로드 검증

3. **프로덕션 배포**:
   - 기존 corp_list.json을 S3로 업로드
   - Lambda 환경 변수 설정
   - 전체 회사 배치 처리 테스트

## 4. 구현 세부사항

### 4.1 S3ConfigManager 클래스 설계
```python
class S3ConfigManager:
    def __init__(self, bucket_name, config_path, cache_ttl=3600)
    def download_config(self) -> dict
    def get_corp_list(self) -> list
    def is_cache_valid(self) -> bool
    def refresh_cache(self) -> bool
```

### 4.2 캐싱 전략
- Lambda `/tmp` 디렉토리에 파일 캐시
- TTL 기반 캐시 무효화
- ETag/LastModified 기반 변경 감지

### 4.3 오류 처리 전략
- S3 다운로드 실패 시 로컬 fallback (개발 환경)
- 재시도 로직 (exponential backoff)
- 상세한 오류 로깅 및 CloudWatch 연동

## 5. 마이그레이션 로드맵

### Phase 1: 준비 작업
- S3ConfigManager 개발
- 단위 테스트 작성
- 로컬 테스트 환경 구성

### Phase 2: 코드 통합
- 기존 코드 리팩토링
- 하위 호환성 유지 (로컬/S3 동시 지원)
- 통합 테스트

### Phase 3: 배포 및 검증
- S3에 설정 파일 업로드
- Lambda 환경 변수 설정
- 프로덕션 검증

### Phase 4: 완전 전환
- 로컬 파일 의존성 제거
- 성능 모니터링
- 문서 업데이트

## 6. 예상 이점

### 6.1 운영상 이점
- **중앙 집중 관리**: 설정 파일 S3 중앙화
- **실시간 업데이트**: Lambda 재배포 없이 설정 변경
- **다중 환경 지원**: dev/staging/prod 환경별 설정
- **버전 관리**: S3 버전 관리 활용

### 6.2 기술적 이점
- **컨테이너 최적화**: Lambda 패키지 크기 감소
- **확장성**: 여러 Lambda 함수 간 설정 공유
- **장애 복구**: S3 백업 및 복구 메커니즘
- **모니터링**: CloudWatch 통합 로깅

## 7. 고려사항

### 7.1 성능 영향
- 최초 Lambda cold start 시 S3 다운로드 지연
- 캐싱으로 후속 호출 성능 최적화
- 네트워크 의존성 추가

### 7.2 비용 영향
- S3 GET 요청 비용 (미미함)
- Lambda 실행 시간 소폭 증가
- 운영 효율성 향상으로 상쇄

### 7.3 보안 고려사항
- S3 버킷 접근 권한 최소화
- Lambda IAM 역할 권한 추가 필요
- 설정 파일 암호화 (필요시)
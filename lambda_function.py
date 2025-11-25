#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AWS Lambda Handler for XBRL DART API 처리 시스템

목적:
1. AWS Lambda 환경에서 DART API를 통한 XBRL 데이터 수집
2. 수집된 데이터를 처리하여 S3에 파티셔닝하여 저장
3. CloudWatch를 통한 실행 로그 및 오류 추적

Lambda 실행 환경:
- Python 3.13 런타임
- 메모리: 1024MB 이상 권장
- 타임아웃: 15분 (최대 실행 시간)
- 환경 변수: DART_API_KEY, S3_BUCKET_NAME, S3_PREFIX 필수
- 레이어: AWSSDKPandas-Python313

실행 방식:
1. 스케줄 실행 (EventBridge): 매월 정기적 데이터 수집
2. 수동 실행: 테스트 또는 특정 기간 데이터 재처리
"""

import json
import os
import sys
import traceback
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Optional

# Lambda 환경에서는 /tmp 디렉토리만 쓰기 가능
LAMBDA_TMP_DIR = "/tmp"

# 로깅 설정 (CloudWatch 연동)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# 핸들러 함수에서 사용할 전역 변수들
batch_processor = None


def setup_lambda_environment():
    """
    Lambda 환경 설정 및 검증

    Returns:
        bool: 환경 설정 성공 여부
    """
    try:
        # Lambda 환경에서 dart-fss 캐시 디렉토리를 /tmp로 설정
        if os.environ.get('AWS_LAMBDA_FUNCTION_NAME'):
            os.environ['DART_CACHE_DIR'] = '/tmp/.dart_cache'
            os.environ['HOME'] = '/tmp'

            # dart-fss가 사용할 수 있는 캐시 디렉토리 생성
            os.makedirs('/tmp/.dart_cache', exist_ok=True)
            os.makedirs('/tmp/.cache', exist_ok=True)

        # 필수 환경 변수 검증
        required_env_vars = [
            'DART_API_KEY',
            'S3_BUCKET_NAME'
        ]

        missing_vars = []
        for var in required_env_vars:
            if not os.getenv(var):
                missing_vars.append(var)

        if missing_vars:
            logger.error(f"필수 환경 변수가 설정되지 않음: {missing_vars}")
            return False

        # 선택적 환경 변수 기본값 설정
        if not os.getenv('S3_PREFIX'):
            os.environ['S3_PREFIX'] = 'l0/ver=1/sys=dart/loc=common/table=financial_reports_detail/'
            logger.info("S3_PREFIX 기본값 설정됨")

        # Lambda 임시 디렉토리 확인
        if not os.path.exists(LAMBDA_TMP_DIR):
            logger.error(f"Lambda 임시 디렉토리 접근 불가: {LAMBDA_TMP_DIR}")
            return False

        # 작업 디렉토리를 /tmp로 변경
        os.chdir(LAMBDA_TMP_DIR)
        logger.info(f"작업 디렉토리 변경: {os.getcwd()}")

        return True

    except Exception as e:
        logger.error(f"Lambda 환경 설정 오류: {e}")
        logger.error(traceback.format_exc())
        return False


def initialize_batch_processor():
    """
    배치 프로세서 초기화 (전역 변수로 재사용)
    """
    global batch_processor

    if batch_processor is None:
        try:
            # 지연 임포트 (Lambda cold start 최적화)
            from xbrl_batch_processor import XBRLBatchProcessor

            # S3 dry-run 모드는 환경 변수로 제어
            s3_dry_run = os.getenv('S3_DRY_RUN', 'false').lower() == 'true'
            batch_processor = XBRLBatchProcessor(s3_dry_run=s3_dry_run)

            logger.info("배치 프로세서 초기화 완료")
            logger.info(f"S3 DRY-RUN 모드: {s3_dry_run}")

        except Exception as e:
            logger.error(f"배치 프로세서 초기화 실패: {e}")
            logger.error(traceback.format_exc())
            raise

    return batch_processor


def parse_lambda_event(event: Dict) -> Dict:
    """
    Lambda 이벤트 파싱 및 기본값 설정

    Args:
        event (dict): Lambda 이벤트 객체

    Returns:
        dict: 파싱된 실행 파라미터
    """
    # 기본 파라미터 (환경변수 우선, 없으면 기본값)
    default_months_back = int(os.getenv('MONTHS_BACK', 6))
    default_start_ymd = os.getenv('START_YMD', '')  # 환경변수에서 시작일
    default_end_ymd = os.getenv('END_YMD', '')      # 환경변수에서 종료일

    params = {
        'months_back': default_months_back,  # 환경변수 우선, 기본 6개월
        'start_ymd': default_start_ymd,      # 조회 시작일 (YYYYMMDD)
        'end_ymd': default_end_ymd,          # 조회 종료일 (YYYYMMDD)
        'upload_s3': True,  # 기본적으로 S3 업로드 활성화
        'corp_codes': None,  # None이면 corp_list.json의 모든 회사
        'test_mode': False   # 테스트 모드 (1개 회사만 처리)
    }

    # 이벤트에서 파라미터 추출 (이벤트 값이 환경변수보다 우선)
    if 'months_back' in event:
        params['months_back'] = int(event['months_back'])

    if 'start_ymd' in event:
        params['start_ymd'] = str(event['start_ymd']).strip()

    if 'end_ymd' in event:
        params['end_ymd'] = str(event['end_ymd']).strip()

    if 'upload_s3' in event:
        params['upload_s3'] = bool(event['upload_s3'])

    if 'corp_codes' in event and event['corp_codes']:
        params['corp_codes'] = event['corp_codes']
        if isinstance(params['corp_codes'], str):
            params['corp_codes'] = [params['corp_codes']]

    if 'test_mode' in event:
        params['test_mode'] = bool(event['test_mode'])

    # 조회 기간 로그 출력
    if params['start_ymd'] and params['end_ymd']:
        logger.info(f"조회 기간: {params['start_ymd']} ~ {params['end_ymd']} (직접 지정)")
    else:
        logger.info(f"조회 기간: 최근 {params['months_back']}개월")

    logger.info(f"실행 파라미터: {params}")
    return params


def lambda_handler(event, context):
    """
    AWS Lambda 메인 핸들러 함수

    Args:
        event (dict): Lambda 이벤트 (EventBridge, API Gateway 등)
        context (object): Lambda 실행 컨텍스트

    Returns:
        dict: 실행 결과 응답
    """
    start_time = datetime.now()
    execution_id = context.aws_request_id if context else 'local-test'

    logger.info("=" * 60)
    logger.info(f"XBRL DART Lambda 실행 시작 - ID: {execution_id}")
    logger.info(f"시작 시간: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Lambda 메모리 제한: {context.memory_limit_in_mb if context else 'Unknown'}MB")
    logger.info(f"Lambda 남은 시간: {context.get_remaining_time_in_millis() if context else 'Unknown'}ms")
    logger.info("=" * 60)

    try:
        # 1. Lambda 환경 설정
        if not setup_lambda_environment():
            raise Exception("Lambda 환경 설정 실패")

        # 2. 실행 파라미터 파싱
        params = parse_lambda_event(event)

        # 3. 배치 프로세서 초기화
        processor = initialize_batch_processor()

        # 4. 남은 실행 시간 체크 (타임아웃 방지)
        if context:
            remaining_time = context.get_remaining_time_in_millis()
            if remaining_time < 60000:  # 1분 미만 남음
                logger.warning(f"실행 시간 부족: {remaining_time}ms 남음")
                return {
                    'statusCode': 408,
                    'body': json.dumps({
                        'success': False,
                        'error': 'Insufficient time remaining',
                        'remaining_time_ms': remaining_time
                    })
                }

        # 5. DART API에서 XBRL 파일 다운로드
        logger.info("DART API 다운로드 시작")
        download_stats = processor.download_xbrl_files(
            months_back=params['months_back'],
            corp_codes=params['corp_codes'],
            start_ymd=params['start_ymd'],
            end_ymd=params['end_ymd']
        )

        if download_stats['files_downloaded'] == 0:
            logger.warning("다운로드된 파일이 없습니다")
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'success': True,
                    'message': 'No files to process',
                    'stats': download_stats
                })
            }

        # 6. 테스트 모드인 경우 1개 파일만 처리
        if params['test_mode']:
            logger.info("테스트 모드: 1개 파일만 처리")
            # 첫 번째 회사의 첫 번째 파일만 처리하도록 제한
            # 실제 구현은 processor 내부에서 처리

        # 7. XBRL 파일 처리
        logger.info("XBRL 파일 처리 시작")
        process_stats = processor.process_all_xbrl_files()

        # 8. S3 업로드
        parquet_files = []
        if params['upload_s3'] and process_stats['files_processed'] > 0:
            logger.info("S3 업로드 시작")

            # 생성된 Parquet 파일 목록 수집
            parquet_files = processor.get_generated_parquet_files()

            if parquet_files:
                processor.upload_to_s3(parquet_files)
                logger.info(f"S3 업로드 완료: {len(parquet_files)}개 파일")
            else:
                logger.warning("업로드할 Parquet 파일이 없습니다")

        # 9. 실행 결과 요약
        end_time = datetime.now()
        execution_duration = (end_time - start_time).total_seconds()

        # 최종 통계 수집
        final_stats = processor.get_execution_stats()

        result = {
            'statusCode': 200,
            'body': json.dumps({
                'success': True,
                'execution_id': execution_id,
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'duration_seconds': execution_duration,
                'parameters': params,
                'statistics': final_stats,
                'parquet_files_generated': len(parquet_files),
                'message': f"처리 완료: {final_stats.get('files_processed', 0)}개 파일 처리됨"
            }, ensure_ascii=False, indent=2)
        }

        logger.info("=" * 60)
        logger.info(f"XBRL DART Lambda 실행 완료 - 소요시간: {execution_duration:.2f}초")
        logger.info(f"처리된 파일: {final_stats.get('files_processed', 0)}개")
        logger.info(f"생성된 Parquet: {len(parquet_files)}개")
        logger.info("=" * 60)

        return result

    except Exception as e:
        # 오류 발생 시 상세 로그 및 응답
        end_time = datetime.now()
        execution_duration = (end_time - start_time).total_seconds()

        error_details = {
            'error_type': type(e).__name__,
            'error_message': str(e),
            'traceback': traceback.format_exc(),
            'execution_id': execution_id,
            'duration_seconds': execution_duration
        }

        logger.error("=" * 60)
        logger.error(f"XBRL DART Lambda 실행 실패 - ID: {execution_id}")
        logger.error(f"오류 유형: {error_details['error_type']}")
        logger.error(f"오류 메시지: {error_details['error_message']}")
        logger.error(f"소요 시간: {execution_duration:.2f}초")
        logger.error("상세 오류:")
        logger.error(error_details['traceback'])
        logger.error("=" * 60)

        return {
            'statusCode': 500,
            'body': json.dumps({
                'success': False,
                'error_details': error_details
            }, ensure_ascii=False, indent=2)
        }


def local_test():
    """
    로컬 테스트용 함수
    """
    print("로컬 테스트 실행 중...")

    # 테스트 이벤트
    test_event = {
        'months_back': 1,
        'test_mode': True,
        'upload_s3': False  # 로컬 테스트에서는 S3 업로드 비활성화
    }

    # 테스트 컨텍스트 모의 객체
    class MockContext:
        def __init__(self):
            self.aws_request_id = 'local-test-' + datetime.now().strftime('%Y%m%d_%H%M%S')
            self.memory_limit_in_mb = 1024

        def get_remaining_time_in_millis(self):
            return 900000  # 15분

    context = MockContext()

    # 핸들러 실행
    result = lambda_handler(test_event, context)

    print("\n" + "=" * 60)
    print("로컬 테스트 결과:")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    print("=" * 60)

    return result


if __name__ == "__main__":
    # 로컬 실행 시 테스트 모드
    local_test()
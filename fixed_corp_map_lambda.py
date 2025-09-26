#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
실제 테이블 스키마에 맞게 수정된 Corp Map API Lambda
"""

import json
import os
import time
import boto3
import logging
from datetime import datetime

# 로깅 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class FixedCorpMapAPIHandler:
    """실제 테이블 스키마에 맞게 수정된 Corp Map 테이블 조회"""

    def __init__(self):
        # 환경변수에서 설정 로드
        self.database = os.getenv('ATHENA_DATABASE', 'dev_fi_l0_database')
        self.table = os.getenv('ATHENA_TABLE', 'table_corp_map')
        self.output_s3 = os.getenv('ATHENA_OUTPUT_S3', 's3://hds-dap-dev-an2-datalake-01/athena-results/')
        self.cache_ttl_hours = int(os.getenv('CORP_CACHE_TTL_HOURS', '24'))

        # AWS 클라이언트 초기화
        self.athena_client = boto3.client('athena')
        self.s3_client = boto3.client('s3')

        # 캐시 설정
        self.cache_file = "/tmp/corp_map_full_cache.json"
        self.memory_cache = None
        self.cache_timestamp = None

        logger.info(f"FixedCorpMapAPIHandler 초기화 완료")
        logger.info(f"Database: {self.database}, Table: {self.table}")

    def get_full_corp_map(self):
        """전체 corp_map 테이블 조회 - 실제 스키마 사용"""

        logger.info("실제 스키마로 전체 corp_map 테이블 조회 시작")

        # 1. 캐시 확인
        cached_data = self._load_from_cache()
        if cached_data:
            logger.info(f"캐시에서 로드: {len(cached_data)}개 회사")
            return cached_data

        # 2. Athena 쿼리 실행
        try:
            logger.info("Athena 쿼리 실행 중...")
            corp_list = self._query_athena()

            if corp_list:
                # 3. 캐시 저장
                self._save_to_cache(corp_list)
                logger.info(f"Athena에서 로드 성공: {len(corp_list)}개 회사")
                return corp_list
            else:
                logger.warning("Athena 쿼리 결과가 비어있음")
                return []

        except Exception as e:
            logger.error(f"Athena 쿼리 실패: {e}")

            # 4. Fallback: 캐시 파일이라도 사용 (만료되어도)
            cached_data = self._load_from_cache(ignore_ttl=True)
            if cached_data:
                logger.info(f"Fallback - 만료된 캐시 사용: {len(cached_data)}개 회사")
                return cached_data

            return []

    def _query_athena(self):
        """실제 스키마에 맞는 Athena 쿼리"""

        # 실제 컬럼들을 사용한 쿼리 (DART_CORP_CODE를 8자리로 LPAD 처리)
        query = f"""
        SELECT
            dart_corp,
            LPAD(CAST(dart_corp_code AS VARCHAR), 8, '0') as dart_corp_code,
            stock_nm,
            LPAD(CAST(stock_code AS VARCHAR), 6, '0') as stock_code
        FROM {self.database}.{self.table}
        GROUP BY dart_corp, dart_corp_code, stock_nm, stock_code
        ORDER BY dart_corp
        """

        try:
            logger.info(f"실제 스키마 쿼리 실행: {query}")

            # 쿼리 실행
            response = self.athena_client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={'Database': self.database},
                ResultConfiguration={'OutputLocation': self.output_s3}
            )

            query_execution_id = response['QueryExecutionId']
            logger.info(f"쿼리 실행 ID: {query_execution_id}")

            # 쿼리 완료 대기
            if self._wait_for_query_completion(query_execution_id):
                return self._get_query_results(query_execution_id)
            else:
                raise Exception("쿼리 실행 시간 초과")

        except Exception as e:
            logger.error(f"Athena 쿼리 실행 오류: {e}")
            raise

    def _wait_for_query_completion(self, query_execution_id: str, max_wait: int = 60) -> bool:
        """쿼리 완료 대기"""
        start_time = time.time()

        while time.time() - start_time < max_wait:
            response = self.athena_client.get_query_execution(
                QueryExecutionId=query_execution_id
            )

            status = response['QueryExecution']['Status']['State']

            if status == 'SUCCEEDED':
                logger.info(f"쿼리 성공 (소요시간: {time.time() - start_time:.1f}초)")
                return True
            elif status in ['FAILED', 'CANCELLED']:
                error = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                logger.error(f"쿼리 실패: {error}")
                return False

            time.sleep(2)  # 2초 대기

        logger.error(f"쿼리 시간 초과 ({max_wait}초)")
        return False

    def _get_query_results(self, query_execution_id: str):
        """쿼리 결과 가져오기 - 실제 스키마 매핑"""
        corp_data = []
        next_token = None

        # 실제 컬럼 순서 (쿼리 순서와 동일)
        column_names = [
            'dart_corp',
            'dart_corp_code',
            'stock_nm',
            'stock_code'
        ]

        while True:
            # 페이징 처리
            if next_token:
                response = self.athena_client.get_query_results(
                    QueryExecutionId=query_execution_id,
                    NextToken=next_token
                )
            else:
                response = self.athena_client.get_query_results(
                    QueryExecutionId=query_execution_id
                )

            rows = response['ResultSet']['Rows']

            # 첫 페이지인 경우 헤더 행 제외
            if not next_token:
                rows = rows[1:]  # 헤더 제외

            # 데이터 파싱
            for row in rows:
                data = row['Data']

                corp_info = {}
                for i, column_name in enumerate(column_names):
                    if i < len(data):
                        value = data[i].get('VarCharValue', '')
                        corp_info[column_name] = value if value else None
                    else:
                        corp_info[column_name] = None

                # 모든 데이터 포함 (필터링은 클라이언트에서)
                corp_data.append(corp_info)

            # 다음 페이지 확인
            next_token = response.get('NextToken')
            if not next_token:
                break

        logger.info(f"파싱 완료: {len(corp_data)}개 회사")
        return corp_data

    def _load_from_cache(self, ignore_ttl: bool = False):
        """캐시에서 데이터 로드"""
        # 1. 메모리 캐시 확인
        if self.memory_cache and self.cache_timestamp:
            if ignore_ttl or self._is_cache_valid(self.cache_timestamp):
                logger.info("메모리 캐시 사용")
                return self.memory_cache

        # 2. 파일 캐시 확인
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    cache_data = json.load(f)

                cache_timestamp = datetime.fromisoformat(cache_data.get('timestamp', '2000-01-01'))

                if ignore_ttl or self._is_cache_valid(cache_timestamp):
                    logger.info("파일 캐시 사용")
                    corp_list = cache_data.get('data', [])

                    # 메모리 캐시 업데이트
                    self.memory_cache = corp_list
                    self.cache_timestamp = cache_timestamp

                    return corp_list
        except Exception as e:
            logger.error(f"캐시 파일 로드 오류: {e}")

        return None

    def _save_to_cache(self, corp_list):
        """캐시에 데이터 저장"""
        try:
            # 1. 메모리 캐시 저장
            self.memory_cache = corp_list
            self.cache_timestamp = datetime.now()

            # 2. 파일 캐시 저장
            cache_data = {
                'timestamp': self.cache_timestamp.isoformat(),
                'data': corp_list
            }

            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, ensure_ascii=False, indent=2)

            logger.info(f"캐시 저장 완료: {len(corp_list)}개 회사")

        except Exception as e:
            logger.error(f"캐시 저장 오류: {e}")

    def _is_cache_valid(self, cache_timestamp: datetime) -> bool:
        """캐시 유효성 검사"""
        from datetime import timedelta
        ttl_delta = timedelta(hours=self.cache_ttl_hours)
        return datetime.now() - cache_timestamp < ttl_delta


def lambda_handler(event, context):
    """Fixed Lambda 핸들러"""

    try:
        # HTTP 메서드 확인 (Lambda Function URL)
        http_method = event.get('requestContext', {}).get('http', {}).get('method', 'GET')

        if http_method != 'GET':
            return {
                'statusCode': 405,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'success': False,
                    'error': 'Method Not Allowed. Use GET method.'
                }, ensure_ascii=False)
            }

        logger.info(f"Fixed Corp Map API 요청 시작 - Method: {http_method}")

        # Corp Map 데이터 조회
        handler = FixedCorpMapAPIHandler()
        corp_data = handler.get_full_corp_map()

        logger.info(f"Fixed Corp Map API 응답 준비 - {len(corp_data)}개 회사")

        # JSON 응답 반환
        response_data = {
            'success': True,
            'count': len(corp_data),
            'timestamp': datetime.now().isoformat(),
            'data': corp_data
        }

        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json; charset=utf-8',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'GET',
                'Access-Control-Allow-Headers': 'Content-Type',
                'Cache-Control': 'public, max-age=3600'  # 1시간 캐시
            },
            'body': json.dumps(response_data, ensure_ascii=False, separators=(',', ':'))
        }

    except Exception as e:
        logger.error(f"Fixed Lambda 실행 오류: {e}")

        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json; charset=utf-8',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }, ensure_ascii=False)
        }


if __name__ == "__main__":
    # 로컬 테스트
    handler = FixedCorpMapAPIHandler()
    corp_data = handler.get_full_corp_map()
    print(f"테스트 결과: {len(corp_data)}개 회사")
    if corp_data:
        print("샘플:")
        for corp in corp_data[:3]:
            print(f"  - {corp.get('dart_corp')}: {corp.get('dart_corp_code')} (주식코드: {corp.get('stock_code', 'N/A')})")
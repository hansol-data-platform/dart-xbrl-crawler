#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
S3 파티셔닝 업로드 관리자

기능:
1. 생성된 CSV 파일을 S3에 파티션 구조로 업로드
2. year={year}/mm={month} 파티션 구조 생성
3. 파일명에서 년도/월 정보 추출하여 자동 파티셔닝
4. 업로드 진행상황 추적 및 오류 관리

파티션 구조:
s3://bucket/prefix/year=2025/mm=06/FS_회사코드_202506.csv
"""

import os
import boto3
from pathlib import Path
import pandas as pd
from datetime import datetime
import re
from typing import List, Dict, Optional
from dotenv import load_dotenv

# 환경 변수 로드
load_dotenv()


class S3Uploader:
    """S3 파티셔닝 업로드 관리자"""

    def __init__(self, dry_run=False):
        """
        초기화

        Args:
            dry_run (bool): True이면 실제 업로드 없이 시뮬레이션만 수행
        """
        self.dry_run = dry_run
        # S3 설정 로드
        self.bucket_name = os.getenv('S3_BUCKET_NAME')
        self.s3_prefix = os.getenv('S3_PREFIX', '').rstrip('/')

        if not self.bucket_name:
            raise ValueError("S3_BUCKET_NAME이 .env 파일에 설정되어 있지 않습니다.")

        # S3 클라이언트 초기화
        if self.dry_run:
            print(f"[DRY-RUN MODE] S3 클라이언트 시뮬레이션")
            print(f"  - 버킷: {self.bucket_name}")
            print(f"  - 프리픽스: {self.s3_prefix}")
            self.s3_client = None
        else:
            try:
                self.s3_client = boto3.client('s3')
                print(f"S3 클라이언트 초기화 완료")
                print(f"  - 버킷: {self.bucket_name}")
                print(f"  - 프리픽스: {self.s3_prefix}")
            except Exception as e:
                print(f"S3 클라이언트 초기화 실패: {e}")
                self.s3_client = None

        # 업로드 통계
        self.stats = {
            "files_uploaded": 0,
            "files_failed": 0,
            "total_size": 0,
            "errors": []
        }

    def extract_partition_info(self, filename: str) -> Optional[Dict[str, str]]:
        """
        파일명에서 파티션 정보 추출

        Args:
            filename (str): 파일명 (예: "FS_00171636_202506.csv")

        Returns:
            dict: {"year": "2025", "month": "06"} 또는 None
        """
        # FS_회사코드_YYYYMM.parquet 패턴에서 YYYYMM 추출
        pattern = r'FS_\d{8}_(\d{4})(\d{2})\.parquet'
        match = re.search(pattern, filename)

        if match:
            year = match.group(1)
            month = match.group(2)
            return {
                "year": year,
                "month": month
            }

        print(f"파일명에서 파티션 정보를 추출할 수 없습니다: {filename}")
        return None

    def generate_s3_key(self, filename: str, year: str, month: str) -> str:
        """
        S3 키 생성 (파티션 경로 포함)

        Args:
            filename (str): 파일명
            year (str): 년도 (4자리)
            month (str): 월 (2자리)

        Returns:
            str: S3 키 경로
        """
        # 파티션 경로 생성: year=YYYY/mm=MM/filename
        partition_path = f"year={year}/mm={month}"

        if self.s3_prefix:
            s3_key = f"{self.s3_prefix}/{partition_path}/{filename}"
        else:
            s3_key = f"{partition_path}/{filename}"

        return s3_key

    def prepare_parquet_for_upload(self, parquet_file_path: str) -> Optional[str]:
        """
        =========================================================================
        🗂️ 중요: Parquet 파일에서 파티션 컬럼 제거 🗂️
        =========================================================================

        목적: S3 파티션 구조로 저장 시 yyyy, month 컬럼은 불필요

        제거 대상:
        - yyyy 컬럼: 파티션 경로의 year=YYYY로 대체
        - month 컬럼: 파티션 경로의 mm=MM으로 대체

        유지 대상:
        - 나머지 모든 컬럼 (order_no, corp_code, corp_name, report_type 등)

        수정방법:
        - 다시 yyyy, month를 포함하려면 drop_columns 리스트에서 제거
        =========================================================================

        Args:
            parquet_file_path (str): 원본 Parquet 파일 경로

        Returns:
            str: 수정된 Parquet 파일 경로 (임시 파일) 또는 None
        """
        try:
            # Parquet 파일 읽기
            df = pd.read_parquet(parquet_file_path)

            # 파티션 컬럼 제거 (yyyy, month)
            drop_columns = ['yyyy', 'month']
            columns_to_drop = [col for col in drop_columns if col in df.columns]

            if columns_to_drop:
                df_cleaned = df.drop(columns=columns_to_drop)
                print(f"파티션 컬럼 제거: {columns_to_drop}")
                print(f"  - 제거 전: {len(df.columns)}개 컬럼")
                print(f"  - 제거 후: {len(df_cleaned.columns)}개 컬럼")
            else:
                df_cleaned = df
                print("제거할 파티션 컬럼이 없습니다.")

            # 임시 파일로 저장
            temp_file_path = parquet_file_path.replace('.parquet', '_temp_for_s3.parquet')
            df_cleaned.to_parquet(temp_file_path, index=False)

            return temp_file_path

        except Exception as e:
            print(f"Parquet 파일 전처리 오류 ({parquet_file_path}): {e}")
            return None

    def upload_file_to_s3(self, local_file_path: str, s3_key: str) -> bool:
        """
        파일을 S3에 업로드

        Args:
            local_file_path (str): 로컬 파일 경로
            s3_key (str): S3 키 경로

        Returns:
            bool: 업로드 성공 여부
        """
        # 파일 크기 확인
        file_size = os.path.getsize(local_file_path)

        if self.dry_run:
            print(f"[DRY-RUN] S3 업로드 시뮬레이션: s3://{self.bucket_name}/{s3_key}")
            print(f"  - 파일 크기: {file_size:,} bytes")

            # 통계 업데이트 (시뮬레이션)
            self.stats["files_uploaded"] += 1
            self.stats["total_size"] += file_size
            return True

        if not self.s3_client:
            print("S3 클라이언트가 초기화되지 않았습니다.")
            return False

        try:
            # S3 업로드
            self.s3_client.upload_file(
                local_file_path,
                self.bucket_name,
                s3_key
            )

            print(f"S3 업로드 성공: s3://{self.bucket_name}/{s3_key}")
            print(f"  - 파일 크기: {file_size:,} bytes")

            # 통계 업데이트
            self.stats["files_uploaded"] += 1
            self.stats["total_size"] += file_size

            return True

        except Exception as e:
            error_msg = f"S3 업로드 실패 ({s3_key}): {e}"
            print(error_msg)
            self.stats["errors"].append(error_msg)
            self.stats["files_failed"] += 1
            return False

    def upload_parquet_files(self, parquet_files: List[str]) -> Dict:
        """
        여러 Parquet 파일을 S3에 파티셔닝하여 업로드

        Args:
            parquet_files (list): Parquet 파일 경로 목록

        Returns:
            dict: 업로드 결과 통계
        """
        print(f"\n=== S3 파티셔닝 업로드 시작 ===")
        print(f"업로드할 파일 수: {len(parquet_files)}")
        if self.dry_run:
            print(f"[DRY-RUN MODE] 실제 업로드 없이 시뮬레이션만 수행")

        if not self.dry_run and not self.s3_client:
            print("S3 클라이언트가 초기화되지 않아 업로드를 건너뜁니다.")
            return self.stats

        uploaded_files = []
        temp_files_to_cleanup = []

        for i, parquet_file in enumerate(parquet_files, 1):
            print(f"\n[{i}/{len(parquet_files)}] 처리 중: {Path(parquet_file).name}")

            # 1. 파티션 정보 추출
            filename = Path(parquet_file).name
            partition_info = self.extract_partition_info(filename)

            if not partition_info:
                print(f"파티션 정보 추출 실패, 건너뜀: {filename}")
                continue

            year = partition_info["year"]
            month = partition_info["month"]

            print(f"  파티션: year={year}/mm={month}")

            # 2. Parquet 파일 전처리 (파티션 컬럼 제거)
            temp_parquet_path = self.prepare_parquet_for_upload(parquet_file)
            if not temp_parquet_path:
                print(f"Parquet 전처리 실패, 건너뜀: {filename}")
                continue

            temp_files_to_cleanup.append(temp_parquet_path)

            # 3. S3 키 생성
            s3_key = self.generate_s3_key(filename, year, month)
            print(f"  S3 경로: s3://{self.bucket_name}/{s3_key}")

            # 4. S3 업로드
            if self.upload_file_to_s3(temp_parquet_path, s3_key):
                uploaded_files.append({
                    "local_file": parquet_file,
                    "s3_key": s3_key,
                    "partition": f"year={year}/mm={month}"
                })

        # 5. 임시 파일 정리
        self.cleanup_temp_files(temp_files_to_cleanup)

        # 6. 결과 보고서 생성
        self.generate_upload_report(uploaded_files)

        return self.stats

    def cleanup_temp_files(self, temp_files: List[str]):
        """임시 파일 정리"""
        for temp_file in temp_files:
            try:
                if os.path.exists(temp_file):
                    os.remove(temp_file)
                    print(f"임시 파일 삭제: {Path(temp_file).name}")
            except Exception as e:
                print(f"임시 파일 삭제 실패 ({temp_file}): {e}")

    def generate_upload_report(self, uploaded_files: List[Dict]):
        """업로드 결과 보고서 생성"""
        print(f"\n=== S3 업로드 결과 보고서 ===")
        print(f"업로드 성공: {self.stats['files_uploaded']}개")
        print(f"업로드 실패: {self.stats['files_failed']}개")
        print(f"총 업로드 크기: {self.stats['total_size']:,} bytes")

        if uploaded_files:
            print(f"\n업로드된 파일 목록:")
            for file_info in uploaded_files:
                print(f"  - {Path(file_info['local_file']).name}")
                print(f"    └─ s3://{self.bucket_name}/{file_info['s3_key']}")

        if self.stats["errors"]:
            print(f"\n발생한 오류 ({len(self.stats['errors'])}개):")
            for error in self.stats["errors"][:5]:  # 최대 5개만 표시
                print(f"  - {error}")
            if len(self.stats["errors"]) > 5:
                print(f"  ... 총 {len(self.stats['errors'])}개 오류")

    def test_s3_connection(self) -> bool:
        """S3 연결 테스트"""
        if not self.s3_client:
            return False

        try:
            # 버킷 존재 확인
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            print(f"S3 연결 테스트 성공: {self.bucket_name}")
            return True
        except Exception as e:
            print(f"S3 연결 테스트 실패: {e}")
            return False


def main():
    """테스트용 메인 함수"""
    uploader = S3Uploader()

    # S3 연결 테스트
    if uploader.test_s3_connection():
        print("S3 연결이 정상적으로 설정되었습니다.")
    else:
        print("S3 연결 설정을 확인해주세요.")


if __name__ == "__main__":
    main()
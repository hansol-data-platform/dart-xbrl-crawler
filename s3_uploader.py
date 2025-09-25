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
s3://bucket/prefix/year=2025/mm=06/corp_code=00171636/report_type=BS/FS_00171636_202506.parquet
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

    def extract_partition_info(self, filename: str, parquet_data: Optional[pd.DataFrame] = None) -> Optional[Dict[str, str]]:
        """
        파일명과 데이터에서 파티션 정보 추출

        Args:
            filename (str): 파일명 (예: "FS_00171636_202506.parquet")
            parquet_data (pd.DataFrame): Parquet 데이터 (corp_code, report_type 추출용)

        Returns:
            dict: {"year": "2025", "month": "06", "corp_code": "00171636", "report_type": "BS"} 또는 None
        """
        # FS_회사코드_YYYYMM.parquet 패턴에서 기본 정보 추출
        pattern = r'FS_(\d{8})_(\d{4})(\d{2})\.parquet'
        match = re.search(pattern, filename)

        if not match:
            print(f"파일명 패턴이 맞지 않습니다: {filename}")
            return None

        corp_code = match.group(1)
        year = match.group(2)
        month = match.group(3)

        # 기본 파티션 정보
        partition_info = {
            "year": year,
            "month": month,
            "corp_code": corp_code
        }

        # Parquet 데이터에서 report_type 추출 (우선순위: BS > CIS)
        if parquet_data is not None and not parquet_data.empty:
            if 'report_type' in parquet_data.columns:
                unique_report_types = parquet_data['report_type'].unique()
                # BS (재무상태표)가 있으면 우선, 없으면 첫 번째 타입 사용
                if 'BS' in unique_report_types:
                    partition_info['report_type'] = 'BS'
                elif len(unique_report_types) > 0:
                    partition_info['report_type'] = str(unique_report_types[0])
                else:
                    partition_info['report_type'] = 'UNKNOWN'
            else:
                print(f"데이터에 report_type 컬럼이 없습니다: {filename}")
                partition_info['report_type'] = 'UNKNOWN'
        else:
            # 데이터가 없으면 파일명으로 추정
            partition_info['report_type'] = 'MIXED'

        return partition_info

    def generate_s3_key(self, filename: str, partition_info: Dict[str, str]) -> str:
        """
        S3 키 생성 (확장된 파티션 경로 포함)

        Args:
            filename (str): 파일명
            partition_info (dict): 파티션 정보 {"year": "2025", "month": "06", "corp_code": "00171636", "report_type": "BS"}

        Returns:
            str: S3 키 경로
        """
        year = partition_info.get('year', 'unknown')
        month = partition_info.get('month', 'unknown')
        corp_code = partition_info.get('corp_code', 'unknown')
        report_type = partition_info.get('report_type', 'unknown')

        # 파티션 경로 생성: year=YYYY/mm=MM/corp_code=XXXXXXXX/report_type=XX/filename
        partition_path = f"year={year}/mm={month}/corp_code={corp_code}/report_type={report_type}"

        if self.s3_prefix:
            s3_key = f"{self.s3_prefix}/{partition_path}/{filename}"
        else:
            s3_key = f"{partition_path}/{filename}"

        return s3_key

    def prepare_parquet_for_upload(self, parquet_file_path: str) -> Optional[Dict[str, any]]:
        """
        =========================================================================
        🗂️ 중요: Parquet 파일에서 파티션 컬럼 제거 🗂️
        =========================================================================

        목적: S3 파티션 구조로 저장 시 yyyy, month 컬럼은 불필요

        제거 대상:
        - yyyy 컬럼: 파티션 경로의 year=YYYY로 대체
        - month 컬럼: 파티션 경로의 mm=MM으로 대체

        유지 대상:
        - 나머지 모든 컬럼 (order_no, corp_name, concept_id, label_ko, value 등)
        - corp_code, report_type은 파티션으로 활용되므로 파일에서는 제거 가능

        수정방법:
        - 다시 yyyy, month를 포함하려면 drop_columns 리스트에서 제거
        - corp_code, report_type을 파일에 유지하려면 drop_columns에서 제거
        =========================================================================

        Args:
            parquet_file_path (str): 원본 Parquet 파일 경로

        Returns:
            dict: {"temp_file_path": str, "original_data": pd.DataFrame} 또는 None
        """
        try:
            # Parquet 파일 읽기
            df = pd.read_parquet(parquet_file_path)

            # 파티션 컬럼 제거 (yyyy, month, corp_code, report_type)
            # QuickSight에서 파티션으로 필터링할 수 있으므로 데이터에서는 제거
            drop_columns = ['yyyy', 'month', 'corp_code', 'report_type']
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

            return {
                "temp_file_path": temp_file_path,
                "original_data": df  # 파티션 정보 추출용
            }

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

    def filter_and_upload_by_partitions(self, parquet_files: List[str]) -> Dict:
        """
        Parquet 파일들을 파티션별로 필터링하여 S3에 업로드

        각 파일을 corp_code 및 report_type별로 분리하여 업로드합니다.
        동일한 corp_code/report_type 조합이 여러 파일에 있을 경우,
        별도의 파일로 분리하여 저장합니다.

        Args:
            parquet_files (list): Parquet 파일 경로 목록

        Returns:
            dict: 업로드 결과 통계
        """
        print(f"\n=== S3 파티션별 필터링 업로드 시작 ===")
        print(f"업로드할 파일 수: {len(parquet_files)}")
        if self.dry_run:
            print(f"[DRY-RUN MODE] 실제 업로드 없이 시뮬레이션만 수행")

        if not self.dry_run and not self.s3_client:
            print("S3 클라이언트가 초기화되지 않아 업로드를 건너뜁니다.")
            return self.stats

        uploaded_files = []
        temp_files_to_cleanup = []
        partition_file_groups = {}  # corp_code + report_type별로 그룹화

        # 1단계: 모든 파일의 데이터를 파티션별로 그룹화
        for i, parquet_file in enumerate(parquet_files, 1):
            print(f"\n[{i}/{len(parquet_files)}] 분석 중: {Path(parquet_file).name}")

            try:
                # 원본 데이터 로드
                df = pd.read_parquet(parquet_file)

                if df.empty:
                    print(f"  빈 파일, 건너뜀")
                    continue

                # 파티션 정보 추출
                filename = Path(parquet_file).name
                partition_info = self.extract_partition_info(filename, df)

                if not partition_info:
                    print(f"  파티션 정보 추출 실패, 건너뜀")
                    continue

                year = partition_info["year"]
                month = partition_info["month"]
                base_corp_code = partition_info["corp_code"]

                # corp_code 및 report_type별로 데이터 분리
                unique_partitions = []

                if 'corp_code' in df.columns and 'report_type' in df.columns:
                    # 실제 데이터의 corp_code와 report_type 조합 확인
                    partition_combinations = df[['corp_code', 'report_type']].drop_duplicates()

                    for _, row in partition_combinations.iterrows():
                        corp_code = str(row['corp_code']).zfill(8)
                        report_type = str(row['report_type'])

                        # 해당 파티션의 데이터만 필터링
                        partition_data = df[(df['corp_code'] == row['corp_code']) &
                                          (df['report_type'] == row['report_type'])].copy()

                        if not partition_data.empty:
                            partition_key = f"{year}_{month}_{corp_code}_{report_type}"

                            if partition_key not in partition_file_groups:
                                partition_file_groups[partition_key] = {
                                    'year': year,
                                    'month': month,
                                    'corp_code': corp_code,
                                    'report_type': report_type,
                                    'data_frames': [],
                                    'source_files': []
                                }

                            partition_file_groups[partition_key]['data_frames'].append(partition_data)
                            partition_file_groups[partition_key]['source_files'].append(parquet_file)

                            print(f"  파티션 {partition_key}: {len(partition_data)}개 행")

                else:
                    # corp_code, report_type 컬럼이 없는 경우 파일명 기반으로 처리
                    report_type = partition_info.get('report_type', 'MIXED')
                    partition_key = f"{year}_{month}_{base_corp_code}_{report_type}"

                    if partition_key not in partition_file_groups:
                        partition_file_groups[partition_key] = {
                            'year': year,
                            'month': month,
                            'corp_code': base_corp_code,
                            'report_type': report_type,
                            'data_frames': [],
                            'source_files': []
                        }

                    partition_file_groups[partition_key]['data_frames'].append(df)
                    partition_file_groups[partition_key]['source_files'].append(parquet_file)

                    print(f"  파티션 {partition_key}: {len(df)}개 행")

            except Exception as e:
                print(f"  파일 처리 오류: {e}")
                continue

        # 2단계: 파티션별로 데이터 병합 및 업로드
        print(f"\n=== 총 {len(partition_file_groups)}개 파티션 업로드 시작 ===")

        for partition_key, partition_data in partition_file_groups.items():
            year = partition_data['year']
            month = partition_data['month']
            corp_code = partition_data['corp_code']
            report_type = partition_data['report_type']

            print(f"\n파티션 처리: {partition_key}")
            print(f"  소스 파일: {len(partition_data['source_files'])}개")

            try:
                # 데이터 병합
                if len(partition_data['data_frames']) == 1:
                    merged_df = partition_data['data_frames'][0]
                else:
                    merged_df = pd.concat(partition_data['data_frames'], ignore_index=True)

                print(f"  병합된 데이터: {len(merged_df)}개 행, {len(merged_df.columns)}개 컬럼")

                # 파티션 컬럼 제거
                drop_columns = ['yyyy', 'month', 'corp_code', 'report_type']
                columns_to_drop = [col for col in drop_columns if col in merged_df.columns]

                if columns_to_drop:
                    merged_df_cleaned = merged_df.drop(columns=columns_to_drop)
                    print(f"  파티션 컬럼 제거: {columns_to_drop}")
                else:
                    merged_df_cleaned = merged_df

                # 임시 파일 생성
                temp_filename = f"FS_{corp_code}_{year}{month}_{report_type}_partitioned.parquet"
                temp_file_path = os.path.join(os.path.dirname(partition_data['source_files'][0]), temp_filename)

                merged_df_cleaned.to_parquet(temp_file_path, index=False)
                temp_files_to_cleanup.append(temp_file_path)

                # S3 키 생성
                partition_info_dict = {
                    'year': year,
                    'month': month,
                    'corp_code': corp_code,
                    'report_type': report_type
                }

                s3_key = self.generate_s3_key(temp_filename, partition_info_dict)
                print(f"  S3 경로: s3://{self.bucket_name}/{s3_key}")

                # S3 업로드
                if self.upload_file_to_s3(temp_file_path, s3_key):
                    uploaded_files.append({
                        "local_files": partition_data['source_files'],
                        "s3_key": s3_key,
                        "partition": f"year={year}/mm={month}/corp_code={corp_code}/report_type={report_type}",
                        "rows_count": len(merged_df)
                    })
                    print(f"  ✓ 업로드 성공")
                else:
                    print(f"  ✗ 업로드 실패")

            except Exception as e:
                print(f"  파티션 처리 오류: {e}")
                continue

        # 3단계: 임시 파일 정리
        self.cleanup_temp_files(temp_files_to_cleanup)

        # 4단계: 결과 보고서 생성
        self.generate_partition_upload_report(uploaded_files)

        return self.stats

    def upload_parquet_files(self, parquet_files: List[str]) -> Dict:
        """
        여러 Parquet 파일을 S3에 파티셔닝하여 업로드 (기존 방식)

        Args:
            parquet_files (list): Parquet 파일 경로 목록

        Returns:
            dict: 업로드 결과 통계
        """
        print(f"\n=== S3 기본 파티셔닝 업로드 시작 ===")
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

            # 1. Parquet 파일 전처리 (원본 데이터 로드 및 파티션 컬럼 제거)
            filename = Path(parquet_file).name
            prepare_result = self.prepare_parquet_for_upload(parquet_file)

            if not prepare_result:
                print(f"Parquet 전처리 실패, 건너뜀: {filename}")
                continue

            temp_parquet_path = prepare_result["temp_file_path"]
            original_data = prepare_result["original_data"]
            temp_files_to_cleanup.append(temp_parquet_path)

            # 2. 파티션 정보 추출 (데이터 포함)
            partition_info = self.extract_partition_info(filename, original_data)
            if not partition_info:
                print(f"파티션 정보 추출 실패, 건너뜀: {filename}")
                continue

            year = partition_info["year"]
            month = partition_info["month"]
            corp_code = partition_info["corp_code"]
            report_type = partition_info["report_type"]

            print(f"  파티션: year={year}/mm={month}/corp_code={corp_code}/report_type={report_type}")

            # 3. S3 키 생성
            s3_key = self.generate_s3_key(filename, partition_info)
            print(f"  S3 경로: s3://{self.bucket_name}/{s3_key}")

            # 4. S3 업로드
            if self.upload_file_to_s3(temp_parquet_path, s3_key):
                uploaded_files.append({
                    "local_file": parquet_file,
                    "s3_key": s3_key,
                    "partition": f"year={year}/mm={month}/corp_code={corp_code}/report_type={report_type}"
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
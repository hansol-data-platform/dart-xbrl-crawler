#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
XBRL 배치 처리 메인 오케스트레이터

기능:
1. DART API를 통해 모든 회사의 XBRL 파일 다운로드
2. 다운로드된 XBRL 파일들을 순차적으로 처리
3. 최종 CSV 파일 생성 및 정리
4. 처리 진행상황 로깅 및 오류 관리

사용법:
    python xbrl_batch_processor.py [--download-only] [--process-only] [--months=6]
"""

import os
import sys
import json
import argparse
from pathlib import Path
import time
from datetime import datetime
import shutil

from dart_api_manager import DARTAPIManager
from xbrl_processor import XBRLProcessor
from s3_uploader import S3Uploader


class XBRLBatchProcessor:
    """XBRL 배치 처리 메인 클래스"""

    def __init__(self, s3_dry_run=False):
        """
        초기화

        Args:
            s3_dry_run (bool): S3 업로드를 시뮬레이션 모드로 실행
        """
        self.dart_manager = DARTAPIManager()
        self.xbrl_processor = XBRLProcessor()
        self.s3_uploader = S3Uploader(dry_run=s3_dry_run)
        self.results_dir = Path("batch_results")
        self.results_dir.mkdir(exist_ok=True)

        # 처리 통계
        self.stats = {
            "companies_processed": 0,
            "xbrl_files_processed": 0,
            "csv_files_generated": 0,
            "errors": [],
            "start_time": None,
            "end_time": None
        }

    def download_all_xbrl_files(self, months_back=6):
        """
        모든 회사의 XBRL 파일 다운로드

        Args:
            months_back (int): 조회 기간 (개월)

        Returns:
            dict: 회사별 다운로드된 XBRL 파일 목록
        """
        print(f"=== XBRL 파일 다운로드 단계 ===")
        print(f"최근 {months_back}개월간 데이터 다운로드 시작...")

        try:
            all_xbrl_files = self.dart_manager.download_all_companies_xbrl(months_back)

            # 다운로드 결과 저장
            download_summary_path = self.results_dir / "download_summary.json"
            with open(download_summary_path, 'w', encoding='utf-8') as f:
                json.dump(all_xbrl_files, f, ensure_ascii=False, indent=2)

            print(f"다운로드 요약 저장: {download_summary_path}")
            return all_xbrl_files

        except Exception as e:
            error_msg = f"XBRL 다운로드 중 오류 발생: {e}"
            print(error_msg)
            self.stats["errors"].append(error_msg)
            return {}

    def get_downloaded_xbrl_files(self):
        """
        이미 다운로드된 XBRL 파일 목록 조회

        Returns:
            dict: 회사별 XBRL 파일 목록
        """
        # Lambda 환경에서는 /tmp 디렉토리 사용
        if os.environ.get('AWS_LAMBDA_FUNCTION_NAME'):
            download_dir = Path("/tmp/downloaded_xbrl")
        else:
            download_dir = Path("downloaded_xbrl")
        if not download_dir.exists():
            return {}

        all_xbrl_files = {}
        corp_list = self.dart_manager.load_corp_list()

        # 회사명별 매핑을 위한 dict 생성
        corp_name_mapping = {corp['corp_code']: corp['name'] for corp in corp_list}

        for xbrl_file in download_dir.glob("**/*.xbrl"):
            # 파일 경로에서 회사 정보 추출 시도
            parts = xbrl_file.parts

            # 일반적으로 XBRL 파일명에서 entity 코드 추출
            file_stem = xbrl_file.stem
            if "entity" in file_stem:
                # entity00171636_2025-06-30 형태에서 corp_code 추출
                import re
                match = re.search(r'entity(\d{8})', file_stem)
                if match:
                    corp_code = match.group(1)
                    corp_name = corp_name_mapping.get(corp_code, f"Unknown_{corp_code}")

                    if corp_name not in all_xbrl_files:
                        all_xbrl_files[corp_name] = []
                    all_xbrl_files[corp_name].append(str(xbrl_file))

        print(f"기존 다운로드된 XBRL 파일 발견: {len(all_xbrl_files)}개 회사")
        for corp_name, files in all_xbrl_files.items():
            print(f"  {corp_name}: {len(files)}개 파일")

        return all_xbrl_files

    def process_all_xbrl_files(self, all_xbrl_files):
        """
        다운로드된 모든 XBRL 파일 처리

        Args:
            all_xbrl_files (dict): 회사별 XBRL 파일 목록

        Returns:
            list: 생성된 CSV 파일 목록
        """
        print(f"\n=== XBRL 파일 처리 단계 ===")

        all_csv_files = []
        total_companies = len(all_xbrl_files)

        for i, (corp_name, xbrl_files) in enumerate(all_xbrl_files.items(), 1):
            print(f"\n[{i}/{total_companies}] {corp_name} 처리 중...")

            if not xbrl_files:
                print(f"  {corp_name}: 처리할 XBRL 파일 없음")
                continue

            self.stats["companies_processed"] += 1

            for j, xbrl_info in enumerate(xbrl_files, 1):
                # xbrl_info가 dict인지 string인지 확인 (하위호환성)
                if isinstance(xbrl_info, dict):
                    xbrl_file_path = xbrl_info['file_path']
                    report_nm = xbrl_info.get('report_nm', '')
                    print(f"  [{j}/{len(xbrl_files)}] 파일 처리 중: {Path(xbrl_file_path).name} ({report_nm})")
                else:
                    xbrl_file_path = xbrl_info
                    report_nm = ''
                    print(f"  [{j}/{len(xbrl_files)}] 파일 처리 중: {Path(xbrl_file_path).name}")

                try:
                    # XBRL 파일 처리 (보고서 정보 포함)
                    # 보고서 정보가 있으면 새 메서드 사용, 없으면 기존 메서드 사용
                    if report_nm and hasattr(self.xbrl_processor, 'process_xbrl_file_with_report_info'):
                        csv_files = self.xbrl_processor.process_xbrl_file_with_report_info(xbrl_file_path, report_nm)
                    else:
                        csv_files = self.xbrl_processor.process_xbrl_file(xbrl_file_path)

                    if csv_files:
                        self.stats["xbrl_files_processed"] += 1
                        self.stats["csv_files_generated"] += len(csv_files)

                        # 생성된 CSV 파일을 결과 디렉터리로 이동
                        for csv_file in csv_files:
                            if os.path.exists(csv_file):
                                dest_path = self.results_dir / Path(csv_file).name
                                shutil.move(csv_file, dest_path)
                                all_csv_files.append(str(dest_path))
                                print(f"    생성됨: {dest_path.name}")
                    else:
                        error_msg = f"CSV 파일 생성 실패: {xbrl_file_path}"
                        print(f"    오류: {error_msg}")
                        self.stats["errors"].append(error_msg)

                except Exception as e:
                    error_msg = f"파일 처리 오류 ({xbrl_file_path}): {e}"
                    print(f"    오류: {error_msg}")
                    self.stats["errors"].append(error_msg)
                    continue

        return all_csv_files

    def generate_processing_report(self, csv_files):
        """
        처리 결과 보고서 생성

        Args:
            csv_files (list): 생성된 CSV 파일 목록
        """
        # 처리 시간 계산
        if self.stats["start_time"] and self.stats["end_time"]:
            processing_time = self.stats["end_time"] - self.stats["start_time"]
            processing_minutes = processing_time.total_seconds() / 60
        else:
            processing_minutes = 0

        # 보고서 생성
        report = {
            "processing_summary": {
                "start_time": self.stats["start_time"].isoformat() if self.stats["start_time"] else None,
                "end_time": self.stats["end_time"].isoformat() if self.stats["end_time"] else None,
                "processing_time_minutes": round(processing_minutes, 2),
                "companies_processed": self.stats["companies_processed"],
                "xbrl_files_processed": self.stats["xbrl_files_processed"],
                "csv_files_generated": self.stats["csv_files_generated"],
                "error_count": len(self.stats["errors"])
            },
            "generated_csv_files": csv_files,
            "errors": self.stats["errors"]
        }

        # 보고서 저장
        report_path = self.results_dir / "processing_report.json"
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)

        print(f"\n=== 처리 결과 보고서 ===")
        print(f"처리 시간: {processing_minutes:.1f}분")
        print(f"처리된 회사 수: {self.stats['companies_processed']}")
        print(f"처리된 XBRL 파일 수: {self.stats['xbrl_files_processed']}")
        print(f"생성된 CSV 파일 수: {self.stats['csv_files_generated']}")
        print(f"오류 발생 수: {len(self.stats['errors'])}")
        print(f"보고서 저장: {report_path}")

        if self.stats["errors"]:
            print("\n=== 발생한 오류들 ===")
            for i, error in enumerate(self.stats["errors"][:10], 1):  # 최대 10개만 표시
                print(f"{i}. {error}")
            if len(self.stats["errors"]) > 10:
                print(f"... 총 {len(self.stats['errors'])}개 오류 (보고서 참조)")

    def run_full_pipeline(self, months_back=6, download_new=True):
        """
        전체 파이프라인 실행

        Args:
            months_back (int): 다운로드할 기간 (개월)
            download_new (bool): 새로 다운로드할지 여부
        """
        self.stats["start_time"] = datetime.now()

        try:
            print("=== XBRL 배치 처리 시작 ===")
            print(f"시작 시간: {self.stats['start_time'].strftime('%Y-%m-%d %H:%M:%S')}")

            # 1단계: XBRL 파일 다운로드 또는 기존 파일 조회
            if download_new:
                all_xbrl_files = self.download_all_xbrl_files(months_back)
            else:
                all_xbrl_files = self.get_downloaded_xbrl_files()

            if not all_xbrl_files:
                print("처리할 XBRL 파일이 없습니다.")
                return

            # 2단계: XBRL 파일 처리
            csv_files = self.process_all_xbrl_files(all_xbrl_files)

            self.stats["end_time"] = datetime.now()

            # 3단계: 결과 보고서 생성
            self.generate_processing_report(csv_files)

            # 4단계: S3 업로드 (옵션)
            if csv_files:
                self.upload_to_s3(csv_files)

            print(f"\n=== 배치 처리 완료 ===")
            print(f"결과 디렉터리: {self.results_dir}")
            print(f"총 {len(csv_files)}개 CSV 파일 생성 완료")

        except Exception as e:
            self.stats["end_time"] = datetime.now()
            error_msg = f"배치 처리 중 치명적 오류: {e}"
            print(error_msg)
            self.stats["errors"].append(error_msg)
            import traceback
            traceback.print_exc()

    def upload_to_s3(self, csv_files: list):
        """
        =========================================================================
        ☁️ 중요: S3 파티셔닝 업로드 기능 ☁️
        =========================================================================

        목적: 생성된 CSV 파일을 S3에 파티션 구조로 업로드

        파티션 구조:
        s3://bucket/prefix/year=YYYY/mm=MM/FS_회사코드_YYYYMM.csv

        예시:
        - FS_00171636_202506.csv → year=2025/mm=06/FS_00171636_202506.csv
        - FS_01060744_202503.csv → year=2025/mm=03/FS_01060744_202503.csv

        파티션 컬럼 처리:
        - yyyy, month 컬럼은 CSV에서 제거됨 (파티션 경로로 대체)
        - 나머지 컬럼은 모두 유지

        비활성화:
        - S3 업로드를 비활성화하려면 이 메서드 호출을 주석처리
        =========================================================================
        """
        try:
            print(f"\n=== S3 파티셔닝 업로드 단계 ===")

            # S3 연결 테스트
            if not self.s3_uploader.test_s3_connection():
                print("S3 연결 실패로 업로드를 건너뜁니다.")
                print("AWS 자격 증명과 .env 파일의 S3 설정을 확인해주세요.")
                return

            # CSV 파일들을 S3에 업로드
            upload_stats = self.s3_uploader.upload_csv_files(csv_files)

            # 업로드 통계를 메인 통계에 추가
            self.stats["s3_upload"] = upload_stats

            print(f"S3 업로드 완료:")
            print(f"  - 성공: {upload_stats['files_uploaded']}개")
            print(f"  - 실패: {upload_stats['files_failed']}개")

        except Exception as e:
            error_msg = f"S3 업로드 중 오류: {e}"
            print(error_msg)
            self.stats["errors"].append(error_msg)

    def cleanup_temp_files(self):
        """임시 파일 정리"""
        try:
            # 오래된 다운로드 파일 정리
            self.dart_manager.cleanup_old_downloads(days_old=7)

            # 현재 디렉터리의 임시 파일 정리
            temp_patterns = ["*.tmp", "temp_*", "*_temp.*", "*_temp_for_s3.*"]
            for pattern in temp_patterns:
                for temp_file in Path(".").glob(pattern):
                    if temp_file.is_file():
                        temp_file.unlink()
                        print(f"임시 파일 삭제: {temp_file}")

            print("임시 파일 정리 완료")

        except Exception as e:
            print(f"임시 파일 정리 중 오류: {e}")

    # =========================================================================
    # Lambda 핸들러 전용 메서드들
    # =========================================================================

    def download_xbrl_files(self, months_back=6, corp_codes=None):
        """
        Lambda 핸들러용 XBRL 파일 다운로드 메서드

        Args:
            months_back (int): 다운로드할 기간 (개월)
            corp_codes (list): 특정 회사 코드 목록 (None이면 전체)

        Returns:
            dict: 다운로드 통계
        """
        print(f"DART API 다운로드 시작 - 최근 {months_back}개월")

        try:
            if corp_codes:
                # 특정 회사만 다운로드
                all_xbrl_files = {}
                corp_list = self.dart_manager.load_corp_list()
                target_corps = [corp for corp in corp_list if corp['corp_code'] in corp_codes]

                for corp in target_corps:
                    corp_name = corp['name']
                    xbrl_files = self.dart_manager.download_single_company_xbrl(
                        corp['corp_code'], corp_name, months_back
                    )
                    if xbrl_files:
                        all_xbrl_files[corp_name] = xbrl_files
            else:
                # 전체 회사 다운로드
                all_xbrl_files = self.dart_manager.download_all_companies_xbrl(months_back)

            # 다운로드된 파일 수 계산
            total_files = sum(len(files) for files in all_xbrl_files.values())

            return {
                'companies_processed': len(all_xbrl_files),
                'files_downloaded': total_files,
                'company_details': all_xbrl_files
            }

        except Exception as e:
            error_msg = f"XBRL 다운로드 오류: {e}"
            print(error_msg)
            self.stats["errors"].append(error_msg)
            return {
                'companies_processed': 0,
                'files_downloaded': 0,
                'error': error_msg
            }

    def process_all_xbrl_files(self):
        """
        Lambda 핸들러용 XBRL 파일 처리 메서드 (기존 다운로드된 파일 사용)

        Returns:
            dict: 처리 통계
        """
        print("XBRL 파일 처리 시작")

        try:
            # 기존 다운로드된 파일 조회
            all_xbrl_files = self.get_downloaded_xbrl_files()

            if not all_xbrl_files:
                print("처리할 XBRL 파일이 없습니다")
                return {
                    'files_processed': 0,
                    'csv_files_generated': 0
                }

            # 파일 처리
            csv_files = self.process_all_xbrl_files_internal(all_xbrl_files)

            return {
                'files_processed': self.stats['xbrl_files_processed'],
                'csv_files_generated': len(csv_files),
                'csv_files': csv_files
            }

        except Exception as e:
            error_msg = f"XBRL 처리 오류: {e}"
            print(error_msg)
            self.stats["errors"].append(error_msg)
            return {
                'files_processed': 0,
                'csv_files_generated': 0,
                'error': error_msg
            }

    def process_all_xbrl_files_internal(self, all_xbrl_files):
        """
        내부용 XBRL 파일 처리 메서드 (기존 메서드명 충돌 방지)
        """
        all_csv_files = []
        total_companies = len(all_xbrl_files)

        for i, (corp_name, xbrl_files) in enumerate(all_xbrl_files.items(), 1):
            print(f"[{i}/{total_companies}] {corp_name} 처리 중...")

            if not xbrl_files:
                continue

            self.stats["companies_processed"] += 1

            for j, xbrl_info in enumerate(xbrl_files, 1):
                # xbrl_info가 dict인지 string인지 확인
                if isinstance(xbrl_info, dict):
                    xbrl_file_path = xbrl_info['file_path']
                    report_nm = xbrl_info.get('report_nm', '')
                else:
                    xbrl_file_path = xbrl_info
                    report_nm = ''

                try:
                    # XBRL 파일 처리
                    if report_nm and hasattr(self.xbrl_processor, 'process_xbrl_file_with_report_info'):
                        csv_files = self.xbrl_processor.process_xbrl_file_with_report_info(xbrl_file_path, report_nm)
                    else:
                        csv_files = self.xbrl_processor.process_xbrl_file(xbrl_file_path)

                    if csv_files:
                        self.stats["xbrl_files_processed"] += 1
                        self.stats["csv_files_generated"] += len(csv_files)

                        # 생성된 CSV 파일을 결과 디렉터리로 이동
                        for csv_file in csv_files:
                            if os.path.exists(csv_file):
                                dest_path = self.results_dir / Path(csv_file).name
                                shutil.move(csv_file, dest_path)
                                all_csv_files.append(str(dest_path))

                except Exception as e:
                    error_msg = f"파일 처리 오류 ({xbrl_file_path}): {e}"
                    print(error_msg)
                    self.stats["errors"].append(error_msg)
                    continue

        return all_csv_files

    def get_generated_csv_files(self):
        """
        생성된 CSV 파일 목록 반환

        Returns:
            list: CSV 파일 경로 목록
        """
        csv_files = []
        if self.results_dir.exists():
            csv_files = [str(f) for f in self.results_dir.glob("*.csv")]

        print(f"생성된 CSV 파일: {len(csv_files)}개")
        return csv_files

    def get_execution_stats(self):
        """
        실행 통계 반환

        Returns:
            dict: 실행 통계
        """
        return {
            'companies_processed': self.stats['companies_processed'],
            'xbrl_files_processed': self.stats['xbrl_files_processed'],
            'csv_files_generated': self.stats['csv_files_generated'],
            'error_count': len(self.stats['errors']),
            'errors': self.stats['errors'][-10:] if self.stats['errors'] else []  # 최근 10개 오류만
        }


def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(description="XBRL 배치 처리 도구")
    parser.add_argument("--download-only", action="store_true",
                       help="다운로드만 수행 (처리 안함)")
    parser.add_argument("--process-only", action="store_true",
                       help="기존 다운로드 파일만 처리 (새 다운로드 안함)")
    parser.add_argument("--months", type=int, default=6,
                       help="다운로드할 기간 (개월, 기본값: 6)")
    parser.add_argument("--cleanup", action="store_true",
                       help="처리 후 임시 파일 정리")
    parser.add_argument("--s3-dry-run", action="store_true",
                       help="S3 업로드를 시뮬레이션 모드로 실행")

    args = parser.parse_args()

    try:
        # 배치 프로세서 초기화
        batch_processor = XBRLBatchProcessor(s3_dry_run=args.s3_dry_run)

        if args.download_only:
            # 다운로드만 수행
            print("=== 다운로드 전용 모드 ===")
            batch_processor.download_all_xbrl_files(args.months)

        elif args.process_only:
            # 기존 파일만 처리
            print("=== 처리 전용 모드 ===")
            batch_processor.stats["start_time"] = datetime.now()
            all_xbrl_files = batch_processor.get_downloaded_xbrl_files()
            csv_files = batch_processor.process_all_xbrl_files(all_xbrl_files)
            batch_processor.stats["end_time"] = datetime.now()
            batch_processor.generate_processing_report(csv_files)

        else:
            # 전체 파이프라인 실행
            batch_processor.run_full_pipeline(args.months, download_new=True)

        # 임시 파일 정리
        if args.cleanup:
            batch_processor.cleanup_temp_files()

        print("\n모든 작업이 완료되었습니다!")

    except KeyboardInterrupt:
        print("\n사용자에 의해 중단되었습니다.")
        sys.exit(1)
    except Exception as e:
        print(f"오류 발생: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
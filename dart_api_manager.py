#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DART API를 통해 공시정보를 조회하고 XBRL 파일을 다운로드하는 모듈

기능:
1. 회사별 최근 6개월간 공시 목록 조회
2. XBRL 파일 다운로드 및 압축 해제
3. API 호출 제한 관리 (Rate limiting)
4. 다운로드된 파일 정리 및 관리

사용법:
    python dart_api_manager.py
"""

import os
import json
import requests
import zipfile
import tempfile
import shutil
from pathlib import Path
from datetime import datetime, timedelta
from dotenv import load_dotenv
import time
import glob


class DARTAPIManager:
    """DART API 관리 클래스"""

    def __init__(self):
        """초기화"""
        self.load_environment()
        self.base_url = "https://opendart.fss.or.kr/api"
        self.session = requests.Session()
        # Lambda 환경에서는 /tmp 디렉토리 사용
        if os.environ.get('AWS_LAMBDA_FUNCTION_NAME'):
            self.download_dir = Path("/tmp/downloaded_xbrl")
        else:
            self.download_dir = Path("downloaded_xbrl")
        self.download_dir.mkdir(exist_ok=True)

        # API 호출 제한 관리 (분당 1000회)
        self.last_api_call = 0
        self.min_interval = 0.06  # 60초 / 1000회 = 0.06초

    def load_environment(self):
        """
        환경변수 로드 (Lambda 환경변수 우선, .env 파일 fallback)
        """
        # Lambda 환경에서는 .env 파일이 없을 수 있으므로 try-catch 사용
        try:
            load_dotenv()
            print("[ENV] .env 파일 로드 성공")
        except Exception as e:
            print(f"[ENV] .env 파일 로드 실패 (Lambda 환경에서는 정상): {e}")

        # DART API 키 로드 (Lambda 환경변수 > .env 파일)
        self.dart_api_key = os.getenv('DART_API_KEY')
        if not self.dart_api_key:
            raise ValueError("DART_API_KEY가 Lambda 환경변수 또는 .env 파일에 설정되지 않았습니다.")

        # 환경변수 소스 확인 (디버깅용)
        env_source = "Lambda 환경변수" if not os.path.exists('.env') else "Lambda 환경변수 또는 .env 파일"
        print(f"[ENV] DART API 키 로드 완료: {self.dart_api_key[:10]}... (소스: {env_source})")

        # 주요 환경변수들 확인 및 출력
        self._print_environment_variables()

    def load_corp_list(self, filename='corp_list.json'):
        """
        회사 목록 로드

        환경변수 CORP_LIST_SOURCE에 따라:
        - 'api': Corp Map API Lambda 호출
        - 'json': 기존 JSON 파일 사용 (기본값)
        """
        # 환경변수로 소스 선택
        source = os.getenv('CORP_LIST_SOURCE', 'json').lower()

        if source == 'api':
            try:
                print("[DARTAPIManager] Corp Map API에서 회사 목록 로드 시도...")
                corp_list = self._load_from_corp_map_api()

                if corp_list:
                    print(f"[DARTAPIManager] Corp Map API에서 회사 목록 로드 성공: {len(corp_list)}개 회사")
                    return corp_list
                else:
                    print("[DARTAPIManager] Corp Map API 로드 실패, JSON 파일로 fallback")
                    source = 'json'  # fallback

            except Exception as e:
                print(f"[DARTAPIManager] Corp Map API 로드 중 오류: {e}")
                source = 'json'  # fallback

        # JSON 파일에서 로드 (기본값 또는 fallback)
        if source == 'json':
            try:
                # Lambda 환경에서는 절대 경로 사용
                if not os.path.isabs(filename):
                    # 현재 스크립트 디렉토리 기준으로 파일 경로 설정
                    current_dir = os.path.dirname(os.path.abspath(__file__))
                    filename = os.path.join(current_dir, filename)

                with open(filename, 'r', encoding='utf-8') as f:
                    corp_list = json.load(f)
                print(f"[DARTAPIManager] JSON 파일에서 회사 목록 로드 성공: {len(corp_list)}개 회사")
                return corp_list
            except FileNotFoundError:
                print(f"[DARTAPIManager] {filename} 파일을 찾을 수 없습니다.")
                return []  # 빈 리스트 반환으로 변경 (graceful failure)
            except json.JSONDecodeError:
                print(f"[DARTAPIManager] {filename} 파일 형식이 올바르지 않습니다.")
                return []  # 빈 리스트 반환으로 변경 (graceful failure)

    def _load_from_corp_map_api(self):
        """
        Corp Map API Lambda에서 전체 데이터 조회 후 DART_CORP_CODE 필터링

        Returns:
            List[Dict]: DART_CORP_CODE가 있는 회사 목록 (기존 형식과 호환)
        """
        api_url = os.getenv('CORP_MAP_API_URL')
        if not api_url:
            raise ValueError("CORP_MAP_API_URL 환경변수가 설정되지 않았습니다.")

        try:
            # Corp Map API 호출
            print(f"[DARTAPIManager] Corp Map API 호출: {api_url}")
            response = self.session.get(api_url, timeout=30)
            response.raise_for_status()

            api_data = response.json()

            if not api_data.get('success'):
                raise Exception(f"Corp Map API 호출 실패: {api_data.get('error', 'Unknown error')}")

            full_corp_data = api_data.get('data', [])
            print(f"[DARTAPIManager] Corp Map API에서 {len(full_corp_data)}개 회사 데이터 수신")

            # 이 프로젝트에서는 DART_CORP_CODE가 있는 항목만 필터링
            filtered_corps = []
            for corp in full_corp_data:
                dart_corp_code = corp.get('dart_corp_code')
                dart_corp = corp.get('dart_corp')

                # DART 관련 필드가 있는 경우만 (이 프로젝트용)
                if dart_corp_code and dart_corp:
                    # 기존 형식과 호환 (name, corp_code)
                    filtered_corps.append({
                        'name': dart_corp,
                        'corp_code': dart_corp_code,
                        # 추가 정보도 포함 (필요시 사용)
                        'stock_code': corp.get('stock_code', ''),
                        'stock_nm': corp.get('stock_nm', ''),
                        'listed_yn': corp.get('listed_yn', 'N')
                    })

            print(f"[DARTAPIManager] 전체 {len(full_corp_data)}개 중 DART_CORP_CODE 있는 회사: {len(filtered_corps)}개")
            return filtered_corps

        except requests.exceptions.RequestException as e:
            print(f"[DARTAPIManager] Corp Map API 네트워크 오류: {e}")
            raise
        except Exception as e:
            print(f"[DARTAPIManager] Corp Map API 처리 중 오류: {e}")
            raise

    def _print_environment_variables(self):
        """환경변수 로드 상태 확인 및 출력"""
        env_vars = [
            'DART_API_KEY',
            'S3_BUCKET_NAME',
            'S3_PREFIX',
            'CORP_LIST_SOURCE',
            'CORP_MAP_API_URL',
            'ATHENA_DATABASE',
            'ATHENA_TABLE',
            'CORP_CACHE_TTL_HOURS'
        ]

        print("[ENV] 환경변수 로드 상태:")
        for var_name in env_vars:
            value = os.getenv(var_name)
            if value:
                # 민감한 정보는 마스킹
                if 'API_KEY' in var_name:
                    display_value = f"{value[:10]}..." if len(value) > 10 else value
                elif 'URL' in var_name and len(value) > 30:
                    display_value = f"{value[:30]}..."
                else:
                    display_value = value
                print(f"[ENV]   ✅ {var_name}: {display_value}")
            else:
                print(f"[ENV]   ❌ {var_name}: 설정되지 않음")

    def wait_for_rate_limit(self):
        """API 호출 제한 준수"""
        now = time.time()
        elapsed = now - self.last_api_call
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self.last_api_call = time.time()

    def get_recent_disclosures(self, corp_code, months_back=6, start_ymd=None, end_ymd=None):
        """
        특정 회사의 공시 목록 조회

        Args:
            corp_code (str): 회사 고유번호
            months_back (int): 조회 기간 (개월) - start_ymd/end_ymd 없을 때 사용
            start_ymd (str): 조회 시작일 (YYYYMMDD 형식, 예: '20240101')
            end_ymd (str): 조회 종료일 (YYYYMMDD 형식, 예: '20241231')

        Returns:
            list: 공시 목록
        """
        # 날짜 범위 계산: start_ymd/end_ymd 둘 다 있으면 사용, 아니면 months_back 사용
        if start_ymd and end_ymd and str(start_ymd).strip() and str(end_ymd).strip():
            # 직접 지정된 기간 사용 (둘 다 유효한 값일 때만)
            bgn_de = str(start_ymd).strip()
            end_de = str(end_ymd).strip()
            print(f"조회 기간: {bgn_de} ~ {end_de} (직접 지정)")
        else:
            # 기존 방식: 오늘 기준 months_back 개월 (start_ymd/end_ymd 중 하나라도 없으면)
            end_date = datetime.now()
            start_date = end_date - timedelta(days=months_back * 30)
            bgn_de = start_date.strftime('%Y%m%d')
            end_de = end_date.strftime('%Y%m%d')
            print(f"조회 기간: {bgn_de} ~ {end_de} (최근 {months_back}개월)")

        # API 호출 파라미터
        params = {
            'crtfc_key': self.dart_api_key,
            'corp_code': corp_code,
            'bgn_de': bgn_de,
            'end_de': end_de,
            'page_no': 1,
            'page_count': 100,
            'sort': 'date',
            'sort_mth': 'desc'
        }

        try:
            self.wait_for_rate_limit()
            response = self.session.get(f"{self.base_url}/list.json", params=params)
            response.raise_for_status()

            data = response.json()

            if data.get('status') == '000':
                disclosures = data.get('list', [])
                print(f"회사코드 {corp_code}: {len(disclosures)}개 공시 발견")
                return disclosures
            else:
                print(f"API 오류 (회사코드 {corp_code}): {data.get('message', 'Unknown error')}")
                return []

        except requests.RequestException as e:
            print(f"API 호출 오류 (회사코드 {corp_code}): {e}")
            return []
        except json.JSONDecodeError as e:
            print(f"JSON 파싱 오류 (회사코드 {corp_code}): {e}")
            return []

    def filter_xbrl_disclosures(self, disclosures):
        """
        XBRL이 포함된 공시만 필터링

        Args:
            disclosures (list): 공시 목록

        Returns:
            list: XBRL 공시 목록
        """
        xbrl_disclosures = []

        # =========================================================================
        # 🎯 중요: 특정 보고서 종류만 필터링 🎯
        # =========================================================================
        #
        # 목적: 분석에 필요한 정기 보고서만 다운로드하여 데이터 품질 향상
        #
        # 포함할 보고서:
        # - 반기보고서: 6월, 12월 반기 재무제표 (예: "반기보고서 (2025.06)")
        # - 분기보고서: 3월, 9월 분기 재무제표 (예: "1분기보고서 (2025.03)")
        # - 사업보고서: 연간 재무제표 (예: "사업보고서 (2024.12)")
        #
        # 제외할 보고서:
        # - 임시보고서, 정정신고서, 첨부보고서 등 비정기 보고서
        # - 단순 재무제표 첨부 문서들
        #
        # 수정방법: 아래 키워드 리스트를 변경하여 필터링 범위 조정 가능
        # =========================================================================

        # 정기 보고서만 선별적으로 다운로드
        target_report_types = [
            '반기보고서',    # 반기 재무제표 (6월, 12월)
            '분기보고서',    # 분기 재무제표 (3월, 9월)
            '사업보고서'     # 연간 재무제표 (12월)
        ]

        for disclosure in disclosures:
            report_nm = disclosure.get('report_nm', '')

            # 정기 보고서인지 확인 (괄호 안에 년월 정보가 있는 보고서만)
            # 예: "반기보고서 (2025.06)", "1분기보고서 (2025.03)" 등
            is_target_report = False
            for report_type in target_report_types:
                if report_type in report_nm and '(' in report_nm and ')' in report_nm:
                    # 년월 패턴이 있는지 추가 확인
                    import re
                    if re.search(r'\(\d{4}\.\d{2}\)', report_nm):
                        is_target_report = True
                        break

            if is_target_report:
                xbrl_disclosures.append(disclosure)
                print(f"  [SELECTED] {report_nm}")
            else:
                print(f"  [SKIPPED] {report_nm}")

        print(f"XBRL 관련 공시 필터링: {len(xbrl_disclosures)}개")
        return xbrl_disclosures

    def download_xbrl_file(self, rcept_no, corp_name=""):
        """
        특정 공시의 XBRL 파일 다운로드

        Args:
            rcept_no (str): 접수번호
            corp_name (str): 회사명 (로그용)

        Returns:
            list: 다운로드된 XBRL 파일 경로 목록 (성공시), [] (실패시)
        """
        # DART XBRL 다운로드는 바로 ZIP 파일 형태로 제공
        download_url = f"{self.base_url}/fnlttXbrl.xml"
        params = {
            'crtfc_key': self.dart_api_key,
            'rcept_no': rcept_no
        }

        try:
            self.wait_for_rate_limit()
            response = self.session.get(download_url, params=params)
            response.raise_for_status()

            # 응답이 ZIP 파일인지 확인
            content_type = response.headers.get('content-type', '')
            if 'application/zip' in content_type or response.content.startswith(b'PK'):
                # 바로 ZIP 파일로 처리
                return self.extract_zip_content(response.content, rcept_no, corp_name)
            else:
                # JSON 응답인 경우 (오류 응답)
                try:
                    data = response.json()
                    print(f"XBRL 다운로드 오류 ({corp_name}, 접수번호: {rcept_no}): {data.get('message', 'Unknown error')}")
                except:
                    print(f"XBRL 다운로드 응답 오류 ({corp_name}, 접수번호: {rcept_no}): 예상치 못한 응답 형식")
                return []

        except requests.RequestException as e:
            print(f"XBRL 다운로드 요청 오류 ({corp_name}, 접수번호: {rcept_no}): {e}")
            return []

    def extract_zip_content(self, zip_content, rcept_no, corp_name=""):
        """
        ZIP 바이트 내용을 압축 해제

        Args:
            zip_content (bytes): ZIP 파일 바이트 내용
            rcept_no (str): 접수번호
            corp_name (str): 회사명

        Returns:
            list: 추출된 XBRL 파일 경로 목록
        """
        try:
            # 임시 파일에 ZIP 내용 저장
            with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as temp_zip:
                temp_zip.write(zip_content)
                temp_zip_path = temp_zip.name

            # 압축 해제 디렉터리 생성
            extract_dir = self.download_dir / f"{rcept_no}"
            extract_dir.mkdir(exist_ok=True)

            # ZIP 파일 압축 해제
            with zipfile.ZipFile(temp_zip_path, 'r') as zip_ref:
                zip_ref.extractall(extract_dir)

            # 임시 ZIP 파일 삭제
            os.unlink(temp_zip_path)

            # XBRL 파일 찾기
            xbrl_files = list(extract_dir.glob("**/*.xbrl"))

            if xbrl_files:
                print(f"XBRL 다운로드 성공 ({corp_name}, 접수번호: {rcept_no}): {len(xbrl_files)}개 파일")
                return [str(f) for f in xbrl_files]
            else:
                print(f"XBRL 파일이 ZIP에 포함되지 않음 ({corp_name}, 접수번호: {rcept_no})")
                # 빈 디렉터리 정리
                shutil.rmtree(extract_dir)
                return []

        except Exception as e:
            print(f"ZIP 압축 해제 오류 ({corp_name}, 접수번호: {rcept_no}): {e}")
            # 임시 파일 정리
            if 'temp_zip_path' in locals() and os.path.exists(temp_zip_path):
                os.unlink(temp_zip_path)
            return []

    def download_and_extract_zip(self, zip_url, rcept_no, corp_name=""):
        """
        ZIP 파일 다운로드 및 압축 해제

        Args:
            zip_url (str): ZIP 파일 다운로드 URL
            rcept_no (str): 접수번호
            corp_name (str): 회사명

        Returns:
            list: 추출된 XBRL 파일 경로 목록
        """
        try:
            # ZIP 파일 다운로드
            self.wait_for_rate_limit()
            response = self.session.get(zip_url)
            response.raise_for_status()

            # 임시 디렉터리에 저장
            with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as temp_zip:
                temp_zip.write(response.content)
                temp_zip_path = temp_zip.name

            # 압축 해제 디렉터리 생성
            extract_dir = self.download_dir / f"{rcept_no}"
            extract_dir.mkdir(exist_ok=True)

            # ZIP 파일 압축 해제
            with zipfile.ZipFile(temp_zip_path, 'r') as zip_ref:
                zip_ref.extractall(extract_dir)

            # 임시 ZIP 파일 삭제
            os.unlink(temp_zip_path)

            # XBRL 파일 찾기
            xbrl_files = list(extract_dir.glob("**/*.xbrl"))

            if xbrl_files:
                print(f"XBRL 다운로드 성공 ({corp_name}, 접수번호: {rcept_no}): {len(xbrl_files)}개 파일")
                return [str(f) for f in xbrl_files]
            else:
                print(f"XBRL 파일이 ZIP에 포함되지 않음 ({corp_name}, 접수번호: {rcept_no})")
                # 빈 디렉터리 정리
                shutil.rmtree(extract_dir)
                return []

        except Exception as e:
            print(f"ZIP 다운로드/해제 오류 ({corp_name}, 접수번호: {rcept_no}): {e}")
            # 임시 파일 정리
            if 'temp_zip_path' in locals() and os.path.exists(temp_zip_path):
                os.unlink(temp_zip_path)
            return []

    def download_all_companies_xbrl(self, months_back=6, corp_list_file='corp_list.json', start_ymd=None, end_ymd=None):
        """
        모든 회사의 XBRL 파일 다운로드

        Args:
            months_back (int): 조회 기간 (개월) - start_ymd/end_ymd 없을 때 사용
            corp_list_file (str): 회사 목록 파일명
            start_ymd (str): 조회 시작일 (YYYYMMDD 형식)
            end_ymd (str): 조회 종료일 (YYYYMMDD 형식)

        Returns:
            dict: 회사별 다운로드된 XBRL 파일 목록
        """
        if start_ymd and end_ymd:
            print(f"=== 모든 회사의 {start_ymd} ~ {end_ymd} 기간 XBRL 다운로드 시작 ===")
        else:
            print(f"=== 모든 회사의 최근 {months_back}개월간 XBRL 다운로드 시작 ===")

        corp_list = self.load_corp_list(corp_list_file)
        all_xbrl_files = {}

        for i, corp_info in enumerate(corp_list, 1):
            corp_name = corp_info['name']
            corp_code = corp_info['corp_code']

            print(f"\n[{i}/{len(corp_list)}] {corp_name} (코드: {corp_code}) 처리 중...")

            # 공시 목록 조회
            disclosures = self.get_recent_disclosures(corp_code, months_back, start_ymd, end_ymd)

            if not disclosures:
                print(f"{corp_name}: 공시 정보 없음")
                continue

            # XBRL 공시 필터링
            xbrl_disclosures = self.filter_xbrl_disclosures(disclosures)

            if not xbrl_disclosures:
                print(f"{corp_name}: XBRL 공시 없음")
                continue

            # XBRL 파일 다운로드
            corp_xbrl_files = []
            for i, disclosure in enumerate(xbrl_disclosures[:5]):  # 최대 5개까지만 다운로드
                rcept_no = disclosure.get('rcept_no')
                report_nm = disclosure.get('report_nm', '')

                print(f"  다운로드 중: {report_nm} (접수번호: {rcept_no})")
                print(f"    [DEBUG] disclosure 전체: {disclosure}")

                xbrl_files = self.download_xbrl_file(rcept_no, corp_name)
                if xbrl_files:
                    # 각 XBRL 파일에 보고서 정보 추가
                    for xbrl_file in xbrl_files:
                        # 접수일자 추출 및 디버깅
                        rcept_dt = disclosure.get('rcept_dt', '')
                        print(f"    [DEBUG] disclosure.keys(): {list(disclosure.keys())}")
                        print(f"    [DEBUG] rcept_dt 원시값: '{rcept_dt}' (타입: {type(rcept_dt)})")
                        print(f"    [DEBUG] 접수일자: {rcept_dt} (report_nm: {report_nm})")

                        corp_xbrl_files.append({
                            'file_path': xbrl_file,
                            'report_nm': report_nm,
                            'rcept_dt': rcept_dt,
                            'rcept_no': rcept_no
                        })

                # API 호출 간격 조절
                time.sleep(0.1)

            all_xbrl_files[corp_name] = corp_xbrl_files
            print(f"{corp_name}: 총 {len(corp_xbrl_files)}개 XBRL 파일 다운로드 완료")

        # rcept_dt 매핑 정보를 파일로 저장 (Lambda 환경 고려)
        try:
            mapping_file = self.download_dir / "rcept_dt_mapping.json"
            rcept_mapping = {}

            print(f"[DEBUG PATH] 매핑 파일 저장 경로: {mapping_file}")
            print(f"[DEBUG PATH] download_dir: {self.download_dir}")
            print(f"[DEBUG PATH] download_dir 존재 여부: {self.download_dir.exists()}")

            for corp_name, xbrl_files in all_xbrl_files.items():
                for xbrl_info in xbrl_files:
                    if isinstance(xbrl_info, dict):
                        file_path = xbrl_info['file_path']
                        rcept_dt = xbrl_info.get('rcept_dt', '')
                        if rcept_dt:
                            filename = Path(file_path).name
                            rcept_mapping[filename] = rcept_dt
                            print(f"[DEBUG MAPPING] {filename} → {rcept_dt}")

            print(f"[DEBUG MAPPING] 저장할 매핑 총 개수: {len(rcept_mapping)}")

            with open(mapping_file, 'w', encoding='utf-8') as f:
                json.dump(rcept_mapping, f, ensure_ascii=False, indent=2)

            print(f"[MAPPING] rcept_dt 매핑 정보 저장 완료: {len(rcept_mapping)}개 → {mapping_file}")

            # 저장 후 검증
            if mapping_file.exists():
                print(f"[DEBUG PATH] 매핑 파일 저장 검증 성공: {mapping_file}")
            else:
                print(f"[ERROR PATH] 매핑 파일 저장 실패: {mapping_file}")

        except Exception as e:
            print(f"[WARNING] rcept_dt 매핑 파일 저장 실패: {e}")
            import traceback
            print(f"[ERROR TRACE] {traceback.format_exc()}")

        return all_xbrl_files

    def cleanup_old_downloads(self, days_old=7):
        """
        오래된 다운로드 파일 정리

        Args:
            days_old (int): 삭제할 파일의 기준 일수
        """
        if not self.download_dir.exists():
            return

        cutoff_time = time.time() - (days_old * 24 * 60 * 60)
        deleted_count = 0

        for item in self.download_dir.iterdir():
            if item.is_dir():
                # 디렉터리의 수정 시간 확인
                if item.stat().st_mtime < cutoff_time:
                    shutil.rmtree(item)
                    deleted_count += 1

        print(f"오래된 다운로드 파일 정리 완료: {deleted_count}개 디렉터리 삭제")

    def get_download_summary(self):
        """다운로드 현황 요약"""
        if not self.download_dir.exists():
            return {"total_directories": 0, "total_xbrl_files": 0}

        total_dirs = len([d for d in self.download_dir.iterdir() if d.is_dir()])
        total_xbrl = len(list(self.download_dir.glob("**/*.xbrl")))

        return {
            "total_directories": total_dirs,
            "total_xbrl_files": total_xbrl,
            "download_path": str(self.download_dir)
        }


def main():
    """메인 함수"""
    try:
        # DART API 매니저 초기화
        dart_manager = DARTAPIManager()

        # 오래된 파일 정리
        dart_manager.cleanup_old_downloads()

        # 모든 회사의 XBRL 다운로드
        all_xbrl_files = dart_manager.download_all_companies_xbrl(months_back=6)

        # 결과 요약
        print(f"\n=== 다운로드 완료 요약 ===")
        summary = dart_manager.get_download_summary()
        print(f"총 다운로드 디렉터리: {summary['total_directories']}개")
        print(f"총 XBRL 파일: {summary['total_xbrl_files']}개")
        print(f"다운로드 경로: {summary['download_path']}")

        # 회사별 요약
        print(f"\n=== 회사별 다운로드 현황 ===")
        for corp_name, xbrl_files in all_xbrl_files.items():
            print(f"{corp_name}: {len(xbrl_files)}개 파일")

        print("\n모든 XBRL 다운로드가 완료되었습니다!")

    except Exception as e:
        print(f"오류 발생: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
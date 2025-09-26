#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
XBRL 재무제표 데이터 처리 엔진

이 모듈은 한국 DART(Data Analysis, Retrieval and Transfer system)에서
다운로드한 XBRL 파일을 분석하여 구조화된 재무제표 데이터를 생성합니다.

주요 기능:
1. XBRL 파일 파싱 및 재무제표 추출 (연결재무상태표, 연결손익계산서)
2. 다차원 데이터를 행-열 구조로 피벗 변환
3. 보고서 기간 기반 데이터 필터링 (불필요한 과거 데이터 제거)
4. 재무상태표 계층구조 개선 (자산/부채/자본 총계 항목 정리)
5. Parquet 포맷으로 저장 (CSV 파싱 오류 방지 및 성능 최적화)

처리 흐름:
XBRL 파일 → 재무제표 추출 → 피벗 변환 → 기간 필터링 → 계층구조 개선 → Parquet 저장

출력 데이터 구조:
- order_no: 항목 순서 번호
- yyyy, month: 보고 연도, 월
- corp_code, corp_name: 기업 코드, 기업명
- report_type: 보고서 유형 (BS=재무상태표, CIS=손익계산서)
- concept_id: IFRS 개념 식별자
- label_ko, label_en: 항목명 (한글, 영문)
- class0~class3: 계층 구조 분류
- fs_type: 재무제표 유형 (연결, 별도)
- period: 보고 기간
- amount: 금액
- crawl_time: 데이터 처리 시간

사용법:
    python xbrl_processor.py <xbrl_file_path>

예시:
    python xbrl_processor.py entity00171636_2025-06-30.xbrl
"""

import os
import sys
import json
import pandas as pd
from pathlib import Path
import re
from datetime import datetime

# Lambda 환경에서 dart-fss 캐시 디렉토리 설정
if os.environ.get('AWS_LAMBDA_FUNCTION_NAME'):
    os.environ['DART_CACHE_DIR'] = '/tmp/.dart_cache'
    os.environ['HOME'] = '/tmp'
    os.makedirs('/tmp/.dart_cache', exist_ok=True)
    os.makedirs('/tmp/.cache', exist_ok=True)

from dart_fss.xbrl import get_xbrl_from_file


class XBRLProcessor:
    """
    XBRL 재무제표 데이터 처리를 위한 메인 클래스

    이 클래스는 XBRL 파일을 읽어서 구조화된 재무제표 데이터를 생성하는
    모든 기능을 담당합니다. DART에서 다운로드한 XBRL 파일의 복잡한
    다차원 구조를 분석하여 Athena에서 쿼리 가능한 형태로 변환합니다.

    주요 처리 단계:
    1. XBRL 파일 로드 및 메타데이터 추출
    2. 연결재무상태표, 연결손익계산서 데이터 추출
    3. 다차원 데이터를 2차원 테이블로 피벗 변환
    4. 보고서 기간에 맞는 데이터만 필터링
    5. 재무상태표 계층구조 개선 및 정리
    6. Parquet 포맷으로 최종 저장

    Attributes:
        corp_name_mapping (dict): 기업코드-기업명 매핑 딕셔너리
        debug_mode (bool): 디버그 로그 출력 여부
    """

    def __init__(self):
        """
        XBRLProcessor 인스턴스 초기화

        corp_list.json 파일에서 기업코드-기업명 매핑을 로드하고
        디버그 모드를 설정합니다.
        """
        self.corp_name_mapping = self._load_corp_name_mapping()
        self.debug_mode = False  # 프로덕션 환경에서는 디버그 로그 비활성화

        # XBRL 파일명 → rcept_dt 매핑 저장소
        self.xbrl_rcept_dt_mapping = {}  # {"entity00171636_2025-06-30.xbrl": "20250813"}

    def register_xbrl_rcept_dt(self, xbrl_file_path, rcept_dt):
        """
        XBRL 파일과 rcept_dt 매핑 등록

        Args:
            xbrl_file_path (str): XBRL 파일 경로
            rcept_dt (str): 접수일자 (YYYYMMDD 형식)
        """
        from pathlib import Path
        xbrl_filename = Path(xbrl_file_path).name
        if rcept_dt:
            self.xbrl_rcept_dt_mapping[xbrl_filename] = rcept_dt
            print(f"[MAPPING] XBRL-rcept_dt 매핑 등록: {xbrl_filename} → {rcept_dt}")
        else:
            print(f"[MAPPING] rcept_dt가 비어있어 등록하지 않음: {xbrl_filename}")

    def get_rcept_dt_by_xbrl_path(self, xbrl_file_path):
        """
        XBRL 파일 경로로 rcept_dt 조회

        Args:
            xbrl_file_path (str): XBRL 파일 경로

        Returns:
            str: 접수일자 (YYYYMMDD 형식) 또는 빈 문자열
        """
        from pathlib import Path

        print(f"[DEBUG KEY] 전체 경로: '{xbrl_file_path}'")
        xbrl_filename = Path(xbrl_file_path).name
        print(f"[DEBUG KEY] 추출된 파일명: '{xbrl_filename}'")

        rcept_dt = self.xbrl_rcept_dt_mapping.get(xbrl_filename, '')

        if rcept_dt:
            print(f"[MAPPING] XBRL-rcept_dt 매핑 조회 성공: {xbrl_filename} → {rcept_dt}")
        else:
            print(f"[MAPPING] XBRL-rcept_dt 매핑 없음: {xbrl_filename}")

            # 디버깅: 매핑 딕셔너리 내용 확인
            if len(self.xbrl_rcept_dt_mapping) > 0:
                print(f"[DEBUG MAPPING] 현재 매핑 딕셔너리에 있는 키들 ({len(self.xbrl_rcept_dt_mapping)}개):")
                for i, (key, value) in enumerate(list(self.xbrl_rcept_dt_mapping.items())[:5]):
                    print(f"  [{i+1}] '{key}' → '{value}'")
                print(f"[DEBUG KEY] 조회 시도한 키: '{xbrl_filename}'")

                # 키 길이 및 문자 비교
                if self.xbrl_rcept_dt_mapping:
                    first_key = list(self.xbrl_rcept_dt_mapping.keys())[0]
                    print(f"[DEBUG KEY] 첫 번째 키 길이: {len(first_key)}")
                    print(f"[DEBUG KEY] 조회 키 길이: {len(xbrl_filename)}")

                # 부분 매칭 시도
                matching_keys = [k for k in self.xbrl_rcept_dt_mapping.keys() if xbrl_filename in k or k in xbrl_filename]
                if matching_keys:
                    print(f"[DEBUG KEY] 부분 매칭되는 키들: {matching_keys}")
                else:
                    print(f"[DEBUG KEY] 부분 매칭되는 키 없음")
            else:
                print(f"[DEBUG MAPPING] 매핑 딕셔너리가 비어있음")

        return rcept_dt

    def _check_ppe_existence(self, df, step_name):
        """
        유형자산 항목의 존재 여부를 체크하는 디버그 함수

        Args:
            df: 체크할 DataFrame
            step_name: 체크하는 단계명

        Returns:
            bool: 유형자산 항목이 있으면 True, 없으면 False
        """
        if not self.debug_mode:
            return False

        if df.empty:
            print(f"  [- {step_name}] DataFrame 비어있음")
            return False

        # label_ko 컬럼 찾기 (일반 컬럼 또는 튜플 컬럼)
        label_col = None
        if 'label_ko' in df.columns:
            label_col = 'label_ko'
        else:
            # 튜플 형태의 컬럼에서 label_ko 찾기
            for col in df.columns:
                if isinstance(col, tuple) and len(col) >= 2 and col[1] == 'label_ko':
                    label_col = col
                    break

        if label_col is None:
            print(f"  [X {step_name}] label_ko 컬럼 없음")
            return False

        # 유형자산 검색
        ppe_items = df[df[label_col].str.contains('유형자산', na=False)]

        if len(ppe_items) > 0:
            print(f"  [O {step_name}] 유형자산 있음: {len(ppe_items)}개")
            return True
        else:
            print(f"  [X {step_name}] 유형자산 없음!")
            return False
    
    def _load_corp_name_mapping(self):
        """
        기업 코드와 기업명 매핑 정보를 로드합니다.

        corp_list.json 파일에서 DART 등록 기업들의 코드-명칭 매핑을
        읽어와서 XBRL 파일 처리 시 정확한 기업명을 설정할 수 있도록 합니다.
        파일이 없거나 로드에 실패하면 빈 딕셔너리를 반환합니다.

        Returns:
            dict: {기업코드(str): 기업명(str)} 형태의 매핑 딕셔너리

        Note:
            corp_list.json 파일 형식:
            [{"corp_code": "00000000", "name": "기업명"}, ...]
        """
        # 여러 경로에서 corp_list.json 찾기 (Lambda 환경 대응)
        possible_paths = [
            'corp_list.json',
            '/tmp/corp_list.json',
            '/var/task/corp_list.json',  # Lambda 환경
            os.path.join(os.path.dirname(__file__), 'corp_list.json')
        ]

        for corp_list_path in possible_paths:
            try:
                if os.path.exists(corp_list_path):
                    with open(corp_list_path, 'r', encoding='utf-8') as f:
                        corp_list = json.load(f)
                    # corp_code를 키로, name을 값으로 하는 딕셔너리 생성
                    # corp_code는 문자열로 강제 변환
                    mapping = {str(corp['corp_code']): corp['name'] for corp in corp_list}
                    print(f"✓ 회사명 매핑 로드 성공: {corp_list_path}에서 {len(mapping)}개 회사")
                    return mapping
            except Exception as e:
                print(f"경고: {corp_list_path} 로드 실패: {e}")
                continue

        print(f"경고: 모든 경로에서 corp_list.json을 찾을 수 없습니다.")
        print(f"  시도한 경로: {possible_paths}")
        return {}

    def extract_metadata_from_xbrl(self, xbrl):
        """
        XBRL 객체에서 기업 및 보고서 메타데이터를 추출합니다.

        XBRL 파일명과 내부 정보를 분석하여 기업코드, 기업명, 보고연도, 보고월 등의
        메타데이터를 추출합니다. 이 정보는 최종 데이터의 식별자로 사용됩니다.

        Args:
            xbrl: dart-fss 라이브러리의 XBRL 객체

        Returns:
            dict: 추출된 메타데이터
                - corp_code (str): 8자리 기업코드 (예: "00171636")
                - corp_name (str): 기업명 (corp_list.json에서만 가져옴)
                - yyyy (str): 보고연도 4자리 (예: "2025")
                - month (str): 보고월 2자리 (예: "06")

        Note:
            - 기업코드는 파일명의 'entity{8자리숫자}' 패턴에서 추출
            - 보고기간은 파일명의 YYYY-MM-DD 패턴에서 추출
            - 기업명은 corp_list.json 매핑에서만 가져옴 (없으면 Unknown_{기업코드} 사용)
        """
        metadata = {}

        # 법인코드 추출 (파일명에서) - 반드시 문자열로 처리
        try:
            filename = xbrl.filename
            if 'entity' in filename:
                match = re.search(r'entity(\d{8})', filename)
                metadata['corp_code'] = str(match.group(1)) if match else '00000000'
            else:
                metadata['corp_code'] = '00000000'
        except:
            metadata['corp_code'] = '00000000'

        # corp_code가 문자열인지 확인하고, 8자리 유지
        metadata['corp_code'] = str(metadata['corp_code']).zfill(8)

        # 법인명 설정: 무조건 corp_list.json 매핑 사용
        # 디버깅 로그 추가
        print(f"[DEBUG] corp_code: '{metadata['corp_code']}' (type: {type(metadata['corp_code'])})")
        print(f"[DEBUG] 매핑 딕셔너리 크기: {len(self.corp_name_mapping)}")

        if metadata['corp_code'] in self.corp_name_mapping:
            metadata['corp_name'] = self.corp_name_mapping[metadata['corp_code']]
            print(f"✓ corp_list.json에서 회사명 매핑 성공: {metadata['corp_code']} → {metadata['corp_name']}")
        else:
            # 매핑에 없으면 다시 시도 (매핑이 비어있을 수 있음)
            print(f"⚠ 경고: 매핑에서 {metadata['corp_code']} 찾을 수 없음")
            print(f"[DEBUG] 매핑 키 샘플: {list(self.corp_name_mapping.keys())[:5] if self.corp_name_mapping else 'Empty'}")

            # corp_list.json 재로드
            self.corp_name_mapping = self._load_corp_name_mapping()

            if metadata['corp_code'] in self.corp_name_mapping:
                metadata['corp_name'] = self.corp_name_mapping[metadata['corp_code']]
                print(f"✓ 재로드 후 회사명 찾음: {metadata['corp_code']} → {metadata['corp_name']}")
            else:
                # 숫자형으로 변환된 경우도 체크 (앞의 0이 제거된 경우)
                corp_code_without_zeros = metadata['corp_code'].lstrip('0')
                for key, value in self.corp_name_mapping.items():
                    if key.lstrip('0') == corp_code_without_zeros:
                        metadata['corp_name'] = value
                        print(f"✓ 0 제거 후 매칭 성공: {metadata['corp_code']} → {metadata['corp_name']}")
                        break
                else:
                    # 정말 없는 경우 기업코드 사용
                    metadata['corp_name'] = f"Corp_{metadata['corp_code']}"
                    print(f"✗ 최종 실패: {metadata['corp_code']}를 찾을 수 없음. Corp_{metadata['corp_code']} 사용")

        try:
            # 기간 정보 추출
            period_info = xbrl.get_period_information()
            date_columns = [col for col in period_info.columns if isinstance(col, (str, tuple))]

            date_range = None
            for col in date_columns:
                col_str = str(col[0]) if isinstance(col, tuple) else str(col)
                if re.match(r'\d{8}-\d{8}', col_str):
                    date_range = col_str
                    break

            if date_range:
                end_date = date_range.split('-')[1]
                if len(end_date) == 8:
                    metadata['yyyy'] = end_date[:4]
                    metadata['month'] = end_date[4:6]
                else:
                    metadata['yyyy'] = ''
                    metadata['month'] = ''
            else:
                metadata['yyyy'] = ''
                metadata['month'] = ''

        except Exception as e:
            print(f"기간 정보 추출 중 오류: {e}")
            metadata['yyyy'] = ''
            metadata['month'] = ''

        return metadata

    def extract_financial_data(self, xbrl_path):
        """
        XBRL 파일에서 재무제표 데이터 추출

        Args:
            xbrl_path (str): XBRL 파일 경로

        Returns:
            tuple: (balance_sheet_df, income_statement_df, metadata)
        """
        try:
            print(f"XBRL 파일 분석 중: {xbrl_path}")

            # XBRL 파일 로드
            xbrl = get_xbrl_from_file(xbrl_path)

            # 메타데이터 추출
            metadata = self.extract_metadata_from_xbrl(xbrl)
            print(f"추출된 메타데이터: {metadata}")

            # 연결재무상태표 추출
            balance_sheet_df = pd.DataFrame()
            try:
                financial_statements = xbrl.get_financial_statement(separate=False)
                if financial_statements:
                    balance_sheet = financial_statements[0]
                    balance_sheet_df = balance_sheet.to_DataFrame()
                    if not balance_sheet_df.empty:
                        balance_sheet_df = self.add_metadata_to_dataframe(
                            balance_sheet_df, metadata, 'BS'
                        )
                        print(f"연결재무상태표: {len(balance_sheet_df)}행 추출")
            except Exception as e:
                print(f"연결재무상태표 추출 중 오류: {e}")

            # 연결손익계산서 추출
            income_statement_df = pd.DataFrame()
            try:
                income_statements = xbrl.get_income_statement(separate=False)
                if income_statements:
                    income_statement = income_statements[0]
                    income_statement_df = income_statement.to_DataFrame()
                    if not income_statement_df.empty:
                        income_statement_df = self.add_metadata_to_dataframe(
                            income_statement_df, metadata, 'CIS'
                        )
                        print(f"연결손익계산서: {len(income_statement_df)}행 추출")
            except Exception as e:
                print(f"연결손익계산서 추출 중 오류: {e}")

            return balance_sheet_df, income_statement_df, metadata

        except Exception as e:
            print(f"XBRL 데이터 추출 중 오류: {e}")
            return pd.DataFrame(), pd.DataFrame(), {}

    def add_metadata_to_dataframe(self, df, metadata, report_type):
        """DataFrame에 메타데이터 컬럼 추가"""
        if df.empty:
            return df

        df_copy = df.copy()

        # 원본 데이터 순서를 보존하기 위한 order_no 컬럼 추가 (1부터 시작)
        df_copy.insert(0, 'order_no', range(1, len(df_copy) + 1))

        # 메타데이터 컬럼들을 order_no 다음에 추가
        df_copy.insert(1, 'yyyy', metadata.get('yyyy', ''))
        df_copy.insert(2, 'month', metadata.get('month', ''))
        df_copy.insert(3, 'corp_code', metadata.get('corp_code', '00000000'))
        df_copy.insert(4, 'corp_name', metadata.get('corp_name', ''))
        df_copy.insert(5, 'report_type', report_type)

        return df_copy

    def parse_period_info(self, col):
        """기간 정보를 파싱하여 readable 형태로 변환"""
        try:
            col_str = str(col)

            # 튜플 형태인 경우 처리: ('20240630', ('연결재무제표',))
            if col_str.startswith("(") and col_str.endswith(")"):
                # 날짜와 재무제표 유형을 분리
                date_match = re.search(r"'(\d{8})'", col_str)
                fs_type_match = re.search(r"'(연결재무제표|별도재무제표)'", col_str)

                if date_match:
                    date_str = date_match.group(1)
                    formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"

                    fs_type = '연결' if fs_type_match and '연결' in fs_type_match.group(1) else '별도'

                    return formatted_date, fs_type

            # 일반적인 날짜 형식 처리
            elif re.match(r'\d{8}', col_str):  # YYYYMMDD 형태
                formatted_date = f"{col_str[:4]}-{col_str[4:6]}-{col_str[6:8]}"
                return formatted_date, '연결'
            elif '-' in col_str and len(col_str.replace('-', '')) == 16:  # YYYYMMDD-YYYYMMDD 형태
                parts = col_str.split('-')
                if len(parts) == 2 and len(parts[0]) == 8 and len(parts[1]) == 8:
                    start_date = parts[0]
                    end_date = parts[1]
                    period = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:8]} ~ {end_date[:4]}-{end_date[4:6]}-{end_date[6:8]}"
                    return period, '연결'

            # 재무제표 유형 추출
            fs_type = '연결'
            if '연결' in col_str:
                fs_type = '연결'
            elif '별도' in col_str:
                fs_type = '별도'

            return col_str, fs_type

        except Exception as e:
            print(f"기간 정보 파싱 중 오류: {e}")
            return str(col), '연결'

    def analyze_dataframe_structure(self, df):
        """DataFrame의 컬럼 구조를 분석하여 모든 메타데이터 컬럼을 찾음"""
        columns_info = {
            'order_no': None,
            'concept_id': None,
            'label_ko': None,
            'label_en': None,
            'class0': None,
            'class1': None,
            'class2': None,
            'class3': None
        }
        data_columns = []

        print(f"DataFrame 컬럼 분석:")
        for i, col in enumerate(df.columns):
            print(f"  [{i}] {col} ({type(col)})")

            if isinstance(col, tuple) and len(col) == 2:
                # 튜플 형태의 컬럼: (statement_info, column_type)
                statement_info, column_type = col
                if column_type in columns_info:
                    columns_info[column_type] = col
                    print(f"    -> {column_type} 컬럼으로 인식")
                elif isinstance(column_type, tuple):
                    # 날짜 컬럼: ('20250630', ('연결재무제표',))
                    data_columns.append(col)
                    print(f"    -> 데이터 컬럼으로 인식")
                else:
                    data_columns.append(col)
                    print(f"    -> 기타 데이터 컬럼으로 인식")
            elif isinstance(col, str) and col not in ['yyyy', 'month', 'corp_code', 'corp_name', 'report_type']:
                # 단순 문자열 컬럼
                if col in columns_info:
                    columns_info[col] = col
                    print(f"    -> {col} 컬럼으로 인식")
                else:
                    data_columns.append(col)
                    print(f"    -> 데이터 컬럼으로 인식")
            else:
                print(f"    -> 메타데이터 컬럼으로 스킵")

        print(f"인식된 메타데이터 컬럼: {columns_info}")
        print(f"데이터 컬럼 수: {len(data_columns)}")

        return columns_info, data_columns

    def convert_to_pivot_format(self, df, metadata):
        """
        XBRL의 다차원 데이터구조를 2차원 테이블로 피벗 변환합니다.

        XBRL에서 추출한 재무제표 데이터는 행(concept)과 열(기간/구분)의 매트릭스 형태입니다.
        이를 분석 가능한 행 단위 레코드로 변환하여 각 재무항목-기간 조합이 하나의 행이 되도록 합니다.

        주요 처리 과정:
        1. DataFrame 컬럼 구조 분석 (메타데이터 vs 데이터 컬럼 구분)
        2. 각 concept(재무항목)에 대해 모든 기간 데이터를 개별 행으로 변환
        3. 보고서 기간 기반 데이터 필터링 (현재 보고서와 무관한 과거 데이터 제거)
        4. 정렬 및 정리

        Args:
            df (pd.DataFrame): XBRL에서 추출한 원본 재무제표 DataFrame
            metadata (dict): 기업코드, 보고연월 등의 메타데이터

        Returns:
            pd.DataFrame: 피벗 변환된 재무제표 데이터
                각 행은 하나의 재무항목-기간-구분 조합을 나타냄

        Note:
            - 기간 필터링은 ENABLE_PERIOD_FILTERING 플래그로 제어 가능
            - 숫자가 아닌 값이나 0인 값은 제외됨
            - 연결/별도 구분은 컬럼명에서 자동 파싱됨
        """
        if df.empty:
            return df

        try:
            print("피벗 포맷으로 변환 중...")

            # DataFrame 구조 분석
            columns_info, data_columns = self.analyze_dataframe_structure(df)

            # 변환된 데이터 저장할 리스트
            converted_data = []

            # 각 행(concept)에 대해 처리
            for index, row in df.iterrows():
                # 메타데이터 정보 추출
                order_no = row[columns_info['order_no']] if columns_info['order_no'] else index + 1
                concept_id = row[columns_info['concept_id']] if columns_info['concept_id'] else ''
                label_ko = row[columns_info['label_ko']] if columns_info['label_ko'] else ''
                label_en = row[columns_info['label_en']] if columns_info['label_en'] else ''
                class0 = row[columns_info['class0']] if columns_info['class0'] else ''
                class1 = row[columns_info['class1']] if columns_info['class1'] else ''
                class2 = row[columns_info['class2']] if columns_info['class2'] else ''
                class3 = row[columns_info['class3']] if columns_info['class3'] else ''

                # 기본 행 정보 생성 (모든 원본 메타데이터 포함)
                base_row = {
                    'order_no': order_no,
                    'yyyy': metadata.get('yyyy', ''),
                    'month': metadata.get('month', ''),
                    'corp_code': metadata.get('corp_code', '00000000'),
                    'corp_name': metadata.get('corp_name', ''),
                    'report_type': metadata.get('report_type', 'UNKNOWN'),
                    'concept_id': concept_id,
                    'label_ko': label_ko,
                    'label_en': label_en,
                    'class0': class0,
                    'class1': class1,
                    'class2': class2,
                    'class3': class3,
                    'fs_type': '연결',
                    'period': '',
                    'amount': 0
                }

                # 각 데이터 컬럼에 대해 처리
                for col in data_columns:
                    value = row[col]

                    # 값이 유효한지 확인 (숫자인 경우만)
                    try:
                        if pd.notna(value):
                            # 숫자인지 확인
                            numeric_value = float(value)
                            if numeric_value != 0:
                                # 튜플 형태의 컬럼인지 확인하여 실제 날짜 데이터인지 검증
                                if isinstance(col, tuple) and len(col) == 2:
                                    date_str, fs_info = col

                                    # 날짜 형식인지 확인 (YYYYMMDD 또는 YYYYMMDD-YYYYMMDD)
                                    if (isinstance(date_str, str) and
                                        (re.match(r'^\d{8}$', date_str) or re.match(r'^\d{8}-\d{8}$', date_str))):

                                        new_row = base_row.copy()

                                        # 재무제표 유형 파싱
                                        if isinstance(fs_info, tuple) and len(fs_info) > 0:
                                            fs_type_str = fs_info[0]
                                            fs_type = '연결' if '연결' in fs_type_str else '별도'
                                        else:
                                            fs_type = '연결'

                                        # 날짜 포맷팅
                                        if '-' in date_str and len(date_str.replace('-', '')) == 16:
                                            # YYYYMMDD-YYYYMMDD 형태
                                            start_date, end_date = date_str.split('-')
                                            period = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:8]} ~ {end_date[:4]}-{end_date[4:6]}-{end_date[6:8]}"
                                        elif len(date_str) == 8:
                                            # YYYYMMDD 형태
                                            period = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                                        else:
                                            period = date_str

                                        new_row['period'] = period
                                        new_row['fs_type'] = fs_type

                                        # 금액 설정
                                        new_row['amount'] = numeric_value

                                        converted_data.append(new_row)
                                elif isinstance(col, str):
                                    # 단순 문자열 컬럼 - 메타데이터 컬럼이 아닌 경우만
                                    if col not in ['yyyy', 'month', 'corp_code', 'corp_name', 'report_type']:
                                        new_row = base_row.copy()
                                        period, fs_type = self.parse_period_info(col)
                                        new_row['period'] = period
                                        new_row['fs_type'] = fs_type
                                        new_row['amount'] = numeric_value
                                        converted_data.append(new_row)

                    except (ValueError, TypeError):
                        # 숫자가 아닌 값은 스킵
                        continue
                    except Exception as col_error:
                        print(f"컬럼 {col} 처리 중 오류: {col_error}")
                        continue

            # DataFrame 생성
            result_df = pd.DataFrame(converted_data)

            # =========================================================================
            # 🔥 중요: 보고서 기간 기반 데이터 필터링 로직 🔥
            # =========================================================================
            #
            # 문제상황:
            # - 2025.06 반기보고서를 다운로드해도 실제 데이터에는 2025-06-30, 2024-06-30, 2024-12-31 등
            #   여러 기간의 데이터가 모두 포함되어 있음
            # - 하지만 우리는 해당 보고서 기간(2025.06)에 맞는 데이터만 필요함
            #
            # 해결방법:
            # - 보고서명에서 추출한 기간 정보(예: "202506")를 기준으로
            # - DataFrame의 period 컬럼에서 해당 년월에 맞는 데이터만 필터링
            #
            # 필터링 기준:
            # - 보고서 기간이 "202506"이면 period 컬럼에서 "2025-06"이 포함된 행만 유지
            # - 예: "2025-06-30", "2025-06-01 ~ 2025-06-30" 등은 유지
            # - 예: "2024-06-30", "2024-12-31" 등은 제외
            #
            # 주의사항:
            # - 이 필터링을 비활성화하려면 아래 if문을 False로 변경하거나 주석처리
            # - 필터링 로직을 수정하려면 filter_condition 부분을 조정
            # =========================================================================

            # 보고서 기간 기반 필터링 활성화/비활성화 스위치 (True: 활성화, False: 비활성화)
            ENABLE_PERIOD_FILTERING = True

            if ENABLE_PERIOD_FILTERING and not result_df.empty:
                # 기간 정보 추출: 우선순위 1) report_nm에서 추출, 2) 메타데이터에서 추출
                report_period_yyyymm = None

                # 1) 보고서명에서 기간 정보 추출 (예: "반기보고서 (2025.06)" -> "202506")
                if 'report_nm' in metadata:
                    report_period_yyyymm = self.extract_period_from_report_name(metadata.get('report_nm', ''))

                # 2) 보고서명이 없거나 추출 실패시 메타데이터의 yyyy, month에서 추출
                if not report_period_yyyymm:
                    yyyy = metadata.get('yyyy', '')
                    month = metadata.get('month', '')
                    if yyyy and month and len(yyyy) == 4 and len(month) == 2:
                        report_period_yyyymm = f"{yyyy}{month}"
                        print(f"[FILTER] 메타데이터에서 기간 정보 추출: {yyyy}-{month} -> {report_period_yyyymm}")

                if report_period_yyyymm and len(report_period_yyyymm) == 6:
                    # YYYYMM을 YYYY-MM 형태로 변환 (예: "202506" -> "2025-06")
                    target_year = report_period_yyyymm[:4]
                    target_month = report_period_yyyymm[4:6]
                    target_period_pattern = f"{target_year}-{target_month}"

                    print(f"[FILTER] 보고서 기간 필터링 적용: {metadata.get('report_nm', '')} -> {target_period_pattern}")
                    print(f"   필터링 전 데이터 수: {len(result_df)}행")

                    # period 컬럼에서 해당 년월에 해당하는 행만 필터링
                    # 보고서 기간(YYYY-MM)과 정확히 일치하는 기간 데이터만 유지
                    if 'period' in result_df.columns:
                        original_count = len(result_df)

                        # 필터링 조건: period 컬럼에서 target_period_pattern(YYYY-MM)이 포함된 행만 유지
                        # 예: 2025년 3월(202503) -> "2025-03"이 포함된 기간만 유지
                        filter_condition = result_df['period'].astype(str).str.contains(target_period_pattern, na=False)
                        result_df = result_df[filter_condition].reset_index(drop=True)

                        filtered_count = len(result_df)
                        print(f"   필터링 후 데이터 수: {filtered_count}행 (제거됨: {original_count - filtered_count}행)")

                        # 디버깅을 위한 기간별 데이터 분포 출력
                        if original_count > 0:
                            print("   [DATA] 필터링 전 기간별 데이터 분포:")
                            # 임시로 원본 데이터의 기간 분포 확인
                            temp_df = pd.DataFrame(converted_data)
                            if not temp_df.empty and 'period' in temp_df.columns:
                                period_counts = temp_df['period'].value_counts().head(10)
                                for period, count in period_counts.items():
                                    status = "[KEEP]" if target_period_pattern in str(period) else "[SKIP]"
                                    print(f"      {period}: {count}행 {status}")
                    else:
                        print("   [WARNING] period 컬럼이 없어 필터링을 수행할 수 없습니다.")
                else:
                    print(f"   [WARNING] 기간 정보를 추출할 수 없어 필터링을 건너뜁니다.")
            else:
                if not ENABLE_PERIOD_FILTERING:
                    print("   [INFO] 보고서 기간 필터링이 비활성화되어 있습니다.")
                elif result_df.empty:
                    print("   [INFO] 데이터가 없어 필터링을 건너뜁니다.")

            # =========================================================================
            # 필터링 완료 후 데이터 정렬
            # =========================================================================

            # 정렬 (order_no를 최우선으로, 그 다음 period)
            if not result_df.empty:
                sort_columns = []
                if 'order_no' in result_df.columns:
                    sort_columns.append('order_no')
                if 'period' in result_df.columns:
                    sort_columns.append('period')
                if sort_columns:
                    result_df = result_df.sort_values(sort_columns).reset_index(drop=True)

            print(f"피벗 변환 완료: {len(result_df)}행")
            return result_df

        except Exception as e:
            print(f"피벗 변환 중 오류: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()

    def extract_period_from_report_name(self, report_nm):
        """
        보고서명에서 년월 정보 추출

        Args:
            report_nm (str): 보고서명 (예: "반기보고서 (2025.06)")

        Returns:
            str: YYYYMM 형태 (예: "202506") 또는 빈 문자열
        """
        import re

        # (YYYY.MM) 패턴 찾기
        pattern = r'\((\d{4})\.(\d{2})\)'
        match = re.search(pattern, report_nm)

        if match:
            year = match.group(1)
            month = match.group(2)
            return f"{year}{month}"

        return ""

    def generate_output_filename(self, xbrl_path, report_type, metadata, report_nm=""):
        """출력 파일명 생성 - FS_회사코드_YYYYMM.csv 형식"""
        corp_code = metadata.get('corp_code', '00000000')

        # 보고서명에서 년월 추출 시도
        period_from_report = self.extract_period_from_report_name(report_nm)

        if period_from_report:
            # FS_회사코드_YYYYMM.csv 형식
            return f"FS_{corp_code}_{period_from_report}.csv"
        else:
            # fallback: 메타데이터에서 년월 정보 조합
            yyyy = metadata.get('yyyy', '0000')
            month = metadata.get('month', '00')
            return f"FS_{corp_code}_{yyyy}{month}.parquet"

    def save_to_parquet(self, df, output_path, receipt_ymd=None, xbrl_file_path=None):
        """DataFrame을 Parquet 포맷으로 저장 (crawl_time 컬럼 추가)"""
        if df.empty:
            print("저장할 데이터가 없습니다.")
            return False

        try:
            # crawl_time 컬럼 추가 (현재 시간)
            df_copy = df.copy()
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # =========================================================================
            # 파케이 저장 직전 데이터 전처리
            # =========================================================================

            # 1. 컬럼 이름 변경
            if 'label_ko' in df_copy.columns:
                df_copy = df_copy.rename(columns={'label_ko': 'account_name'})
            if 'label_en' in df_copy.columns:
                df_copy = df_copy.rename(columns={'label_en': 'account_name_en'})
            if 'concept_id' in df_copy.columns:
                df_copy = df_copy.rename(columns={'concept_id': 'account_id'})

            # 1-1. BS(재무상태표) 데이터의 총계 항목 정리
            if 'report_type' in df_copy.columns and 'account_name' in df_copy.columns and 'class1' in df_copy.columns:
                bs_mask = (df_copy['report_type'] == 'BS')

                if bs_mask.any():
                    print(f"[BS 정리] 재무상태표 총계 항목 정리 시작 ({bs_mask.sum()}행)")

                    # account_name에서 '총계' 제거: 자산총계 → 자산, 부채총계 → 부채, 자본총계 → 자본
                    bs_data = df_copy[bs_mask].copy()
                    original_account_names = bs_data['account_name'].unique()

                    # 총계 항목 매핑
                    total_mapping = {
                        '자산총계': '자산',
                        '부채총계': '부채',
                        '자본총계': '자본'
                    }

                    # account_name 변경
                    for original, new in total_mapping.items():
                        mask = bs_mask & (df_copy['account_name'] == original)
                        if mask.any():
                            df_copy.loc[mask, 'account_name'] = new
                            print(f"[BS 정리] account_name: {original} → {new} ({mask.sum()}행)")

                    # class1 변경 (account_name과 동일하게)
                    for original, new in total_mapping.items():
                        mask = bs_mask & (df_copy['class1'] == original)
                        if mask.any():
                            df_copy.loc[mask, 'class1'] = new
                            print(f"[BS 정리] class1: {original} → {new} ({mask.sum()}행)")

            # 2. 신규 컬럼 추가
            # report_name 컬럼 추가
            if 'report_type' in df_copy.columns:
                df_copy['report_name'] = df_copy['report_type'].map({
                    'BS': '재무상태표',
                    'CIS': '포괄손익계산서'
                })
            else:
                df_copy['report_name'] = ''

            # receipt_ymd 컬럼 추가 (보고서 접수일자)
            print(f"[DEBUG] save_to_parquet - receipt_ymd 매개변수: '{receipt_ymd}'")
            print(f"[DEBUG] save_to_parquet - xbrl_file_path 매개변수: '{xbrl_file_path}'")

            # 날짜 형식 변환 및 대안 처리
            formatted_receipt_ymd = ''
            final_rcept_dt = receipt_ymd

            # 1차: 매개변수로 받은 receipt_ymd 확인
            if receipt_ymd and str(receipt_ymd) not in ['None', 'null', ''] and len(str(receipt_ymd)) == 8 and str(receipt_ymd).isdigit():
                final_rcept_dt = str(receipt_ymd)
                print(f"[DEBUG] 매개변수에서 유효한 receipt_ymd 발견: {final_rcept_dt}")

            # 2차: receipt_ymd가 비어있거나 None이면 매핑에서 조회
            elif (not receipt_ymd or str(receipt_ymd) in ['None', 'null', '']) and xbrl_file_path:
                mapped_rcept_dt = self.get_rcept_dt_by_xbrl_path(xbrl_file_path)
                if mapped_rcept_dt:
                    final_rcept_dt = mapped_rcept_dt
                    print(f"[SUCCESS] 매핑에서 rcept_dt 복구: {final_rcept_dt}")
                else:
                    print(f"[WARNING] 매핑에서도 rcept_dt를 찾을 수 없음")

            # 3차: 최종적으로 rcept_dt 처리
            if final_rcept_dt and len(final_rcept_dt) == 8 and final_rcept_dt.isdigit():
                try:
                    formatted_receipt_ymd = f"{final_rcept_dt[:4]}-{final_rcept_dt[4:6]}-{final_rcept_dt[6:8]}"
                    print(f"[DEBUG] rcept_dt 형식 변환: {final_rcept_dt} -> {formatted_receipt_ymd}")
                except:
                    formatted_receipt_ymd = final_rcept_dt

            # 4차: 모든 방법이 실패하면 현재 날짜로 대체
            else:
                current_date = datetime.now().strftime('%Y-%m-%d')
                formatted_receipt_ymd = current_date
                print(f"[WARNING] 모든 방법으로 rcept_dt를 찾을 수 없어 현재 날짜로 대체: {formatted_receipt_ymd}")

            df_copy['receipt_ymd'] = formatted_receipt_ymd
            print(f"[DEBUG] receipt_ymd 컬럼에 최종 저장된 값: '{formatted_receipt_ymd}'")

            # 3. class ID 매핑 (class1, class2, class3 -> class1_id, class2_id, class3_id)
            # account_name -> account_id 매핑 딕셔너리 생성
            name_to_id_mapping = {}
            for _, row in df_copy.iterrows():
                if pd.notna(row['account_name']) and row['account_name'] != '':
                    name_to_id_mapping[row['account_name']] = row['account_id']

            # class1_id 매핑
            df_copy['class1_id'] = df_copy['class1'].apply(
                lambda x: name_to_id_mapping.get(x, '') if pd.notna(x) and x != '' else ''
            )

            # class2_id 매핑
            df_copy['class2_id'] = df_copy['class2'].apply(
                lambda x: name_to_id_mapping.get(x, '') if pd.notna(x) and x != '' else ''
            )

            # class3_id 매핑
            df_copy['class3_id'] = df_copy['class3'].apply(
                lambda x: name_to_id_mapping.get(x, '') if pd.notna(x) and x != '' else ''
            )

            # 4. period 컬럼 값 변경
            if 'period' in df_copy.columns and 'report_type' in df_copy.columns:
                # CIS인 경우 period 값 변경
                cis_mask = df_copy['report_type'] == 'CIS'

                # period에서 날짜 범위 파싱하여 3개월/누적 구분
                for idx, row in df_copy[cis_mask].iterrows():
                    period_str = str(row['period'])

                    # YYYY-MM-DD ~ YYYY-MM-DD 형식인 경우
                    if '~' in period_str:
                        date_parts = period_str.split('~')
                        if len(date_parts) == 2:
                            start_date = date_parts[0].strip()
                            end_date = date_parts[1].strip()

                            # 날짜를 파싱하여 개월 수 계산
                            try:
                                # YYYY-MM-DD 형식에서 년월 추출
                                start_year = int(start_date.split('-')[0])
                                start_month = int(start_date.split('-')[1])
                                end_year = int(end_date.split('-')[0])
                                end_month = int(end_date.split('-')[1])

                                # 개월 수 계산
                                month_diff = (end_year - start_year) * 12 + (end_month - start_month) + 1

                                if month_diff <= 3:
                                    df_copy.at[idx, 'period'] = '3개월'
                                else:
                                    df_copy.at[idx, 'period'] = '누적'
                            except:
                                # 파싱 실패 시 기본값
                                df_copy.at[idx, 'period'] = '누적'

                    # YYYY-MM-DD 형식만 있는 경우 (단일 날짜)
                    elif len(period_str) == 10 and '-' in period_str:
                        df_copy.at[idx, 'period'] = '3개월'  # 단일 날짜는 보통 분기

                    # 그 외의 경우
                    else:
                        df_copy.at[idx, 'period'] = '누적'

                # BS인 경우 period 값을 "당기"로 일괄 변경
                bs_mask = df_copy['report_type'] == 'BS'
                df_copy.loc[bs_mask, 'period'] = '당기'

            # crawl_time 추가
            df_copy['crawl_time'] = current_time

            # Parquet 저장
            df_copy.to_parquet(output_path, index=False)
            print(f"파일 저장 완료: {output_path}")
            print(f"총 {len(df_copy)}행 저장됨 (crawl_time: {current_time})")
            return True
        except Exception as e:
            print(f"파일 저장 중 오류: {e}")
            return False

    def improve_hierarchy_structure(self, df):
        """
        재무상태표의 계층구조를 분석에 적합하도록 개선합니다.

        XBRL에서 추출된 재무상태표 데이터의 계층구조는 분석하기에 불편한 형태로
        되어 있습니다. 이 메서드는 다음과 같은 개선을 수행합니다:

        주요 개선사항:
        1. 최상위 총계 항목 정리
           - "자산 [개요]" → "자산총계"
           - "부채 [개요]" → "부채총계"
           - "자본 [개요]" → "자본총계"

        2. 중복 분류 제거
           - class1과 class2가 동일한 총계 항목의 class2를 빈값으로 변경
           - 계층구조의 중복성 제거로 분석 편의성 향상

        3. 순서번호 재정렬
           - 자산총계를 order_no 0으로 설정 (최상단 배치)
           - 부채총계, 자본총계를 각 섹션의 첫 번째로 배치

        4. 불필요한 항목 제거
           - "자본과부채총계" 항목 제거 (자본총계 + 부채총계와 중복)

        Args:
            df (pd.DataFrame): 피벗 변환된 재무제표 데이터

        Returns:
            pd.DataFrame: 계층구조가 개선된 재무제표 데이터

        Note:
            - 재무상태표(BS) 데이터만 처리하며 손익계산서(CIS)는 그대로 유지
            - 개선 전후의 데이터 수 변화를 로그로 출력
        """
        df_copy = df.copy()

        # BS(재무상태표) 데이터만 처리
        bs_mask = df_copy['report_type'] == 'BS'

        # 🔍 디버깅: 함수 시작 시 유형자산 확인
        ppe_before = df_copy[bs_mask & (df_copy['label_ko'].str.contains('유형자산', na=False))]
        print(f"[DEBUG] improve_hierarchy_structure 시작 - BS 항목: {len(df_copy[bs_mask])}개, 유형자산: {len(ppe_before)}개")
        if len(ppe_before) > 0:
            print(f"[DEBUG] 유형자산 항목들: {ppe_before['order_no'].tolist()}")
            for _, item in ppe_before.iterrows():
                print(f"[DEBUG]   - order_no {item['order_no']}: {item['label_ko']} ({item['concept_id']}) [{item['fs_type']}]")

        # 1. class1의 [개요] 항목들을 총계로 변경
        # 자산 [개요] → 자산총계
        mask = bs_mask & (df_copy['class1'] == '자산 [개요]')
        df_copy.loc[mask, 'class1'] = '자산총계'

        # 부채 [개요] → 부채총계
        mask = bs_mask & (df_copy['class1'] == '부채 [개요]')
        df_copy.loc[mask, 'class1'] = '부채총계'

        # 자본 [개요] → 자본총계
        mask = bs_mask & (df_copy['class1'] == '자본 [개요]')
        df_copy.loc[mask, 'class1'] = '자본총계'

        # 2. class1과 class2가 동일한 총계 항목의 class2를 빈값으로 변경
        # 자산총계
        mask = bs_mask & (df_copy['class1'] == '자산총계') & (df_copy['class2'] == '자산총계')
        df_copy.loc[mask, 'class2'] = ''

        # 부채총계
        mask = bs_mask & (df_copy['class1'] == '부채총계') & (df_copy['class2'] == '부채총계')
        df_copy.loc[mask, 'class2'] = ''

        # 자본총계
        mask = bs_mask & (df_copy['class1'] == '자본총계') & (df_copy['class2'] == '자본총계')
        df_copy.loc[mask, 'class2'] = ''

        # 3. 총계 항목들의 order_no 재정렬
        if 'order_no' in df_copy.columns:
            # 자산총계 (class2가 빈값) → order_no = 0
            mask = bs_mask & (df_copy['class1'] == '자산총계') & (df_copy['class2'] == '')
            df_copy.loc[mask, 'order_no'] = 0

            # 부채총계 (class2가 빈값) → 첫 번째 부채 항목의 order_no 사용
            debt_items = df_copy[bs_mask & (df_copy['class1'] == '부채총계') & (df_copy['class2'] != '')]
            if not debt_items.empty:
                first_debt_order_no = debt_items['order_no'].min()
                mask = bs_mask & (df_copy['class1'] == '부채총계') & (df_copy['class2'] == '')
                df_copy.loc[mask, 'order_no'] = first_debt_order_no

            # 자본총계 (class2가 빈값) → 첫 번째 자본 항목의 order_no 사용
            equity_items = df_copy[bs_mask & (df_copy['class1'] == '자본총계') & (df_copy['class2'] != '')]
            if not equity_items.empty:
                first_equity_order_no = equity_items['order_no'].min()
                mask = bs_mask & (df_copy['class1'] == '자본총계') & (df_copy['class2'] == '')
                df_copy.loc[mask, 'order_no'] = first_equity_order_no

            print(f"   order_no 재정렬 완료: 자산총계=0, 부채총계={first_debt_order_no if not debt_items.empty else 'N/A'}, 자본총계={first_equity_order_no if not equity_items.empty else 'N/A'}")

        # 4. BS에서 "자본과부채총계" 항목 제거
        original_count = len(df_copy[bs_mask])

        # label_ko가 "자본과부채총계"인 항목들 제거
        remove_mask = bs_mask & (df_copy['label_ko'] == '자본과부채총계')

        removed_count = len(df_copy[remove_mask])
        df_copy = df_copy[~remove_mask].reset_index(drop=True)

        if removed_count > 0:
            print(f"   '자본과부채총계' 항목 제거: {removed_count}개 항목 제거됨")

        # 🔍 디버깅: 함수 종료 시 유형자산 확인
        bs_data = df_copy[df_copy['report_type'] == 'BS']
        ppe_after = bs_data[bs_data['label_ko'].str.contains('유형자산', na=False)]
        print(f"[DEBUG] improve_hierarchy_structure 완료 - BS 항목: {len(df_copy[df_copy['report_type'] == 'BS'])}개, 유형자산: {len(ppe_after)}개")
        if len(ppe_after) > 0:
            print(f"[DEBUG] 남은 유형자산 항목들: {ppe_after['order_no'].tolist()}")
        elif len(ppe_before) > 0:
            print(f"[DEBUG] ⚠️  유형자산이 사라졌습니다! 시작할 때는 {len(ppe_before)}개 있었음")

        print(f"계층 구조 개선 완료: {len(df_copy[df_copy['report_type'] == 'BS'])}개 BS 항목 처리 (원래: {original_count}개)")

        return df_copy

    def process_xbrl_file(self, xbrl_path):
        """
        XBRL 파일을 처리하여 최종 CSV 파일들을 생성

        Args:
            xbrl_path (str): XBRL 파일 경로

        Returns:
            list: 생성된 파일 경로들
        """
        if not os.path.exists(xbrl_path):
            raise FileNotFoundError(f"XBRL 파일을 찾을 수 없습니다: {xbrl_path}")

        print("[CRITICAL DEBUG] process_xbrl_file (report_info 없는 버전) 호출됨!")
        print(f"=== XBRL 파일 처리 시작 ===")
        print(f"입력 파일: {xbrl_path}")

        # Step 1: 재무제표 데이터 추출
        balance_sheet_df, income_statement_df, metadata = self.extract_financial_data(xbrl_path)

        generated_files = []

        # Step 2 & 3: 연결재무상태표와 연결손익계산서 통합 처리
        all_financial_data = []

        # 연결재무상태표 변환
        if not balance_sheet_df.empty:
            print("\n--- 연결재무상태표 처리 ---")
            pivot_bs_df = self.convert_to_pivot_format(balance_sheet_df, {**metadata, 'report_type': 'BS'})
            if not pivot_bs_df.empty:
                all_financial_data.append(pivot_bs_df)
                print(f"연결재무상태표 데이터: {len(pivot_bs_df)}행")
        else:
            print("연결재무상태표 데이터가 없습니다.")

        # 연결손익계산서 변환
        if not income_statement_df.empty:
            print("\n--- 연결손익계산서 처리 ---")
            pivot_is_df = self.convert_to_pivot_format(income_statement_df, {**metadata, 'report_type': 'CIS'})
            if not pivot_is_df.empty:
                all_financial_data.append(pivot_is_df)
                print(f"연결손익계산서 데이터: {len(pivot_is_df)}행")
        else:
            print("연결손익계산서 데이터가 없습니다.")

        # 통합 데이터가 있는 경우 하나의 파일로 저장
        if all_financial_data:
            print("\n--- 재무제표 통합 저장 ---")

            # 모든 재무제표 데이터를 하나로 합치기
            combined_df = pd.concat(all_financial_data, ignore_index=True)

            # 재무상태표 계층 구조 개선
            combined_df = self.improve_hierarchy_structure(combined_df)

            # report_type 기준으로 정렬 (BS 먼저, 그 다음 CIS)
            if 'report_type' in combined_df.columns:
                combined_df = combined_df.sort_values(['report_type', 'order_no'], na_position='last').reset_index(drop=True)

            # 통합 파일명 생성 (FS_ 접두사 사용)
            output_file = self.generate_output_filename(xbrl_path, 'FS', metadata)

            print(f"통합 재무제표 데이터: {len(combined_df)}행")
            print(f"  - BS 데이터: {len(combined_df[combined_df['report_type'] == 'BS'])}행")
            print(f"  - CIS 데이터: {len(combined_df[combined_df['report_type'] == 'CIS'])}행")

            if self.save_to_parquet(combined_df, output_file, receipt_ymd=None, xbrl_file_path=xbrl_path):
                generated_files.append(output_file)
        else:
            print("저장할 재무제표 데이터가 없습니다.")

        print(f"\n=== 처리 완료 ===")
        if generated_files:
            print(f"생성된 파일 수: {len(generated_files)}")
            for file in generated_files:
                print(f"  - {file}")
        else:
            print("생성된 파일이 없습니다.")

        return generated_files

    def process_xbrl_file_with_report_info(self, xbrl_path, report_nm="", receipt_ymd=None):
        """
        XBRL 파일을 처리하여 최종 CSV 파일들을 생성 (보고서 정보 포함)

        Args:
            xbrl_path (str): XBRL 파일 경로
            report_nm (str): 보고서명 (예: "반기보고서 (2025.06)")
            receipt_ymd (str): 보고서 접수일자 (예: "2025-06-30")

        Returns:
            list: 생성된 파일 경로들
        """
        print("=== XBRL 파일 처리 시작 ===")
        print(f"[CRITICAL DEBUG] 메서드 진입점 확인!")
        print(f"입력 파일: {xbrl_path}")
        print(f"보고서명: {report_nm}")
        print(f"[CRITICAL DEBUG] receipt_ymd 매개변수 원시값: '{receipt_ymd}' (타입: {type(receipt_ymd)})")
        import traceback
        print(f"[CRITICAL DEBUG] 호출 스택:\n{traceback.format_stack()[-3:-1]}")

        # rcept_dt 매핑 등록 (receipt_ymd가 YYYYMMDD 형태로 들어옴)
        if receipt_ymd and str(receipt_ymd) not in ['None', 'null', '']:
            self.register_xbrl_rcept_dt(xbrl_path, str(receipt_ymd))
            print(f"[MAPPING] 매핑 등록 완료: {Path(xbrl_path).name} -> {receipt_ymd}")
        else:
            print(f"[MAPPING] 매핑 등록 실패: receipt_ymd가 유효하지 않음 ('{receipt_ymd}')")

        generated_files = []

        try:
            # Step 1: XBRL 파일에서 재무 데이터 추출
            balance_sheet_df, income_statement_df, metadata = self.extract_financial_data(xbrl_path)

            if balance_sheet_df.empty and income_statement_df.empty:
                print("추출된 재무 데이터가 없습니다.")
                return []

            # Step 2: 보고서명 정보를 metadata에 추가 (필터링을 위해)
            if report_nm:
                metadata['report_nm'] = report_nm
                print(f"보고서명 메타데이터 추가: {report_nm}")

            # =========================================================================
            # 🔄 중요: BS와 CIS를 하나의 파일로 통합 저장 🔄
            # =========================================================================
            #
            # 변경사항:
            # - 기존: BS_회사코드_년월.csv + CIS_회사코드_년월.csv (2개 파일)
            # - 신규: FS_회사코드_년월.csv (1개 통합 파일)
            #
            # 이유:
            # - BS와 CIS의 컬럼 구조가 동일함 (report_type 컬럼으로 구분 가능)
            # - 데이터 분석 시 하나의 파일에서 모든 재무제표 정보 조회 가능
            # - 파일 관리 및 처리 효율성 향상
            #
            # 수정방법:
            # - 다시 분리하려면 아래 로직을 기존 개별 저장 방식으로 되돌리기
            # =========================================================================

            # Step 3 & 4: 연결재무상태표와 연결손익계산서 통합 처리
            all_financial_data = []

            # 연결재무상태표 변환
            if not balance_sheet_df.empty:
                print("\n--- 연결재무상태표 처리 ---")
                pivot_bs_df = self.convert_to_pivot_format(balance_sheet_df, {**metadata, 'report_type': 'BS'})
                if not pivot_bs_df.empty:
                    all_financial_data.append(pivot_bs_df)
                    print(f"연결재무상태표 데이터: {len(pivot_bs_df)}행")
            else:
                print("연결재무상태표 데이터가 없습니다.")

            # 연결손익계산서 변환
            if not income_statement_df.empty:
                print("\n--- 연결손익계산서 처리 ---")
                pivot_is_df = self.convert_to_pivot_format(income_statement_df, {**metadata, 'report_type': 'CIS'})
                if not pivot_is_df.empty:
                    all_financial_data.append(pivot_is_df)
                    print(f"연결손익계산서 데이터: {len(pivot_is_df)}행")
            else:
                print("연결손익계산서 데이터가 없습니다.")

            # 통합 데이터가 있는 경우 하나의 파일로 저장
            if all_financial_data:
                print("\n--- 재무제표 통합 저장 ---")

                # 모든 재무제표 데이터를 하나로 합치기
                combined_df = pd.concat(all_financial_data, ignore_index=True)

                # 재무상태표 계층 구조 개선
                combined_df = self.improve_hierarchy_structure(combined_df)

                # report_type 기준으로 정렬 (BS 먼저, 그 다음 CIS)
                if 'report_type' in combined_df.columns:
                    combined_df = combined_df.sort_values(['report_type', 'order_no'], na_position='last').reset_index(drop=True)

                # 통합 파일명 생성 (FS_ 접두사 사용)
                output_file = self.generate_output_filename(xbrl_path, 'FS', metadata, report_nm)

                print(f"통합 재무제표 데이터: {len(combined_df)}행")
                print(f"  - BS 데이터: {len(combined_df[combined_df['report_type'] == 'BS'])}행")
                print(f"  - CIS 데이터: {len(combined_df[combined_df['report_type'] == 'CIS'])}행")

                if self.save_to_parquet(combined_df, output_file, receipt_ymd=receipt_ymd, xbrl_file_path=xbrl_path):
                    generated_files.append(output_file)
            else:
                print("저장할 재무제표 데이터가 없습니다.")

            print(f"\n=== 처리 완료 ===")
            print(f"생성된 파일 수: {len(generated_files)}")
            for file in generated_files:
                print(f"  - {file}")

            print("\n성공적으로 처리되었습니다!")

        except Exception as e:
            print(f"XBRL 파일 처리 중 오류: {e}")
            import traceback
            traceback.print_exc()

        return generated_files


def main():
    """메인 함수"""
    if len(sys.argv) != 2:
        print("사용법: python xbrl_processor.py <xbrl_file_path>")
        print("예시: python xbrl_processor.py 20250813001262_ifrs/entity00171636_2025-06-30.xbrl")
        sys.exit(1)

    xbrl_path = sys.argv[1]

    try:
        # XBRL 프로세서 초기화 및 실행
        processor = XBRLProcessor()
        generated_files = processor.process_xbrl_file(xbrl_path)

        if generated_files:
            print(f"\n성공적으로 처리되었습니다!")
        else:
            print(f"\n처리 중 문제가 발생했습니다.")
            sys.exit(1)

    except Exception as e:
        print(f"오류 발생: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
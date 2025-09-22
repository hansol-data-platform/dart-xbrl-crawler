#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
XBRL 파일을 처리하여 최종 CSV 파일을 생성하는 메인 처리 엔진 - 디버그 버전
각 단계별로 유형자산 존재 여부를 추적합니다.
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
    """XBRL 파일 처리를 위한 메인 클래스"""

    def __init__(self):
        """초기화"""
        self.corp_name_mapping = self._load_corp_name_mapping()
        self.debug_mode = True  # 디버그 모드 활성화

    def _load_corp_name_mapping(self):
        """corp_list.json에서 회사코드-회사명 매핑 로드"""
        try:
            corp_list_path = 'corp_list.json'
            if os.path.exists(corp_list_path):
                with open(corp_list_path, 'r', encoding='utf-8') as f:
                    corp_list = json.load(f)
                # corp_code를 키로, name을 값으로 하는 딕셔너리 생성
                mapping = {corp['corp_code']: corp['name'] for corp in corp_list}
                print(f"회사명 매핑 로드 완료: {len(mapping)}개 회사")
                return mapping
            else:
                print(f"경고: {corp_list_path} 파일을 찾을 수 없습니다. XBRL 파일의 회사명을 사용합니다.")
                return {}
        except Exception as e:
            print(f"corp_list.json 로드 중 오류: {e}")
            return {}

    def _check_ppe_existence(self, df, step_name):
        """
        유형자산 항목의 존재 여부를 체크하는 간단한 함수

        Args:
            df (pd.DataFrame): 체크할 데이터프레임
            step_name (str): 단계 이름
        """
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

    def extract_metadata_from_xbrl(self, xbrl):
        """
        XBRL 객체에서 메타데이터 추출

        Args:
            xbrl: dart_fss XBRL 객체

        Returns:
            dict: 메타데이터 (yyyy, month, corp_code, corp_name)
        """
        metadata = {}

        # 법인코드 추출 (파일명에서)
        try:
            filename = xbrl.filename
            if 'entity' in filename:
                match = re.search(r'entity(\d{8})', filename)
                metadata['corp_code'] = match.group(1) if match else '00000000'
            else:
                metadata['corp_code'] = '00000000'
        except:
            metadata['corp_code'] = '00000000'

        # 법인명 설정: corp_list.json 매핑 우선, 없으면 XBRL에서 추출
        try:
            # 먼저 corp_list.json 매핑에서 찾기
            if metadata['corp_code'] in self.corp_name_mapping:
                metadata['corp_name'] = self.corp_name_mapping[metadata['corp_code']]
                print(f"corp_list.json에서 회사명 매핑: {metadata['corp_code']} → {metadata['corp_name']}")
            else:
                # 매핑에 없으면 XBRL에서 추출
                entity_info = xbrl.get_entity_information()
                corp_name_row = entity_info[entity_info.iloc[:, 0].str.contains('법인명', na=False)]
                if not corp_name_row.empty:
                    metadata['corp_name'] = str(corp_name_row.iloc[0, 2]).strip()
                    print(f"XBRL에서 회사명 추출: {metadata['corp_name']}")
                else:
                    metadata['corp_name'] = ''
        except Exception as e:
            print(f"법인명 설정 중 오류: {e}")
            metadata['corp_name'] = ''

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
            print(f"\n{'='*60}")
            print(f"XBRL 파일 분석 시작: {xbrl_path}")
            print(f"{'='*60}")

            # XBRL 파일 로드
            xbrl = get_xbrl_from_file(xbrl_path)

            # 메타데이터 추출
            metadata = self.extract_metadata_from_xbrl(xbrl)
            print(f"추출된 메타데이터: {metadata}")

            # 연결재무상태표 추출
            balance_sheet_df = pd.DataFrame()
            try:
                print("\n[STEP 1] XBRL에서 재무상태표 추출 시작...")
                financial_statements = xbrl.get_financial_statement(separate=False)
                if financial_statements:
                    balance_sheet = financial_statements[0]
                    balance_sheet_df = balance_sheet.to_DataFrame()
                    if not balance_sheet_df.empty:
                        print(f"  → 원본 DataFrame: {len(balance_sheet_df)}행")
                        self._check_ppe_existence(balance_sheet_df, "STEP 1: XBRL 원본 데이터")

                        print("\n[STEP 2] 메타데이터 추가 중...")
                        balance_sheet_df = self.add_metadata_to_dataframe(
                            balance_sheet_df, metadata, 'BS'
                        )
                        self._check_ppe_existence(balance_sheet_df, "STEP 2: 메타데이터 추가 후")

                        print(f"\n연결재무상태표: {len(balance_sheet_df)}행 추출 완료")
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

            print("\n[STEP 3] extract_financial_data 함수 완료")
            if not balance_sheet_df.empty:
                self._check_ppe_existence(balance_sheet_df, "STEP 3: extract_financial_data 완료")

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
        데이터를 피벗 포맷으로 변환 (중간 파일 없이 메모리에서 처리)

        Args:
            df (pd.DataFrame): 원본 데이터프레임
            metadata (dict): 메타데이터

        Returns:
            pd.DataFrame: 피벗 포맷으로 변환된 데이터프레임
        """
        if df.empty:
            return df

        try:
            print(f"\n[STEP 4] 피벗 포맷 변환 시작...")
            if metadata.get('report_type') == 'BS':
                self._check_ppe_existence(df, "STEP 4: 피벗 변환 전")

            # DataFrame 구조 분석
            columns_info, data_columns = self.analyze_dataframe_structure(df)

            # 변환된 데이터 저장할 리스트
            converted_data = []

            # 각 행에 대해 처리하면서 유형자산 카운트
            ppe_count_during = 0

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

                # 유형자산 체크
                if '유형자산' in str(label_ko):
                    ppe_count_during += 1

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

            if metadata.get('report_type') == 'BS':
                print(f"  → 피벗 변환 중 유형자산 발견 횟수: {ppe_count_during}")

            # DataFrame 생성
            result_df = pd.DataFrame(converted_data)

            print(f"\n[STEP 5] 피벗 변환 완료")
            if metadata.get('report_type') == 'BS' and not result_df.empty:
                print(f"  → 변환된 DataFrame: {len(result_df)}행")
                self._check_ppe_existence(result_df, "STEP 5: 피벗 변환 후")

            # 보고서 기간 기반 필터링
            ENABLE_PERIOD_FILTERING = True

            if ENABLE_PERIOD_FILTERING and not result_df.empty:
                report_period_yyyymm = None

                # 메타데이터에서 기간 정보 추출
                yyyy = metadata.get('yyyy', '')
                month = metadata.get('month', '')
                if yyyy and month and len(yyyy) == 4 and len(month) == 2:
                    report_period_yyyymm = f"{yyyy}{month}"

                if report_period_yyyymm and len(report_period_yyyymm) == 6:
                    # YYYYMM을 YYYY-MM 형태로 변환
                    target_year = report_period_yyyymm[:4]
                    target_month = report_period_yyyymm[4:6]
                    target_period_pattern = f"{target_year}-{target_month}"

                    print(f"\n[FILTER] 보고서 기간 필터링 적용: {target_period_pattern}")
                    print(f"   필터링 전 데이터 수: {len(result_df)}행")

                    # period 컬럼에서 해당 년월에 해당하는 행만 필터링
                    if 'period' in result_df.columns:
                        original_count = len(result_df)

                        filter_condition = result_df['period'].astype(str).str.contains(target_period_pattern, na=False)
                        result_df = result_df[filter_condition].reset_index(drop=True)

                        filtered_count = len(result_df)
                        print(f"   필터링 후 데이터 수: {filtered_count}행 (제거됨: {original_count - filtered_count}행)")

                        print(f"\n[STEP 6] 기간 필터링 후")
                        if metadata.get('report_type') == 'BS':
                            print(f"  → 필터링 후 DataFrame: {filtered_count}행")
                            self._check_ppe_existence(result_df, "STEP 6: 기간 필터링 후")

            print(f"\n[STEP 7] convert_to_pivot_format 함수 완료")
            if metadata.get('report_type') == 'BS' and not result_df.empty:
                self._check_ppe_existence(result_df, "STEP 7: 피벗 포맷 변환 최종")

            print(f"  → 최종 반환 DataFrame: {len(result_df)}행")
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
            return f"FS_{corp_code}_{yyyy}{month}.csv"

    def save_to_csv(self, df, output_path):
        """DataFrame을 UTF-8-sig CSV로 저장 (crawl_time 컬럼 추가)"""
        if df.empty:
            print("저장할 데이터가 없습니다.")
            return False

        try:
            print(f"\n[STEP 13] CSV 파일 저장 시작...")
            # crawl_time 컬럼 추가 (현재 시간)
            df_copy = df.copy()
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            df_copy['crawl_time'] = current_time

            # 저장 직전 최종 확인
            bs_data = df_copy[df_copy['report_type'] == 'BS'] if 'report_type' in df_copy.columns else pd.DataFrame()
            if not bs_data.empty:
                self._check_ppe_existence(bs_data, "STEP 13: CSV 파일에 쓰기 직전")

            # CSV 저장
            df_copy.to_csv(output_path, index=False, encoding='utf-8-sig')

            # 저장 후 파일 읽기 확인
            saved_df = pd.read_csv(output_path, encoding='utf-8-sig')
            print(f"\n[STEP 14] CSV 파일 저장 완료 및 검증")
            print(f"  → 저장된 파일: {output_path}")
            print(f"  → 총 {len(saved_df)}행 저장됨")

            bs_saved = saved_df[saved_df['report_type'] == 'BS'] if 'report_type' in saved_df.columns else pd.DataFrame()
            if not bs_saved.empty:
                self._check_ppe_existence(bs_saved, "STEP 14: 저장된 CSV 파일 검증")

            return True
        except Exception as e:
            print(f"파일 저장 중 오류: {e}")
            return False

    def improve_hierarchy_structure(self, df):
        """
        재무상태표의 계층 구조를 개선
        - 자산/부채/자본 [개요]를 자산총계/부채총계/자본총계로 변경
        - 중복되는 class2 값을 빈값으로 처리

        Args:
            df (pd.DataFrame): 변환할 데이터프레임

        Returns:
            pd.DataFrame: 계층 구조가 개선된 데이터프레임
        """
        print(f"\n[STEP 8] 계층 구조 개선 시작...")
        df_copy = df.copy()

        # BS(재무상태표) 데이터만 처리
        bs_mask = df_copy['report_type'] == 'BS'

        print(f"  → 개선 전 BS DataFrame: {len(df_copy[bs_mask])}행")
        self._check_ppe_existence(df_copy[bs_mask], "STEP 8: 계층 구조 개선 전")

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

        # 4. BS에서 "자본과부채총계" 항목 제거
        original_count = len(df_copy[bs_mask])

        # label_ko가 "자본과부채총계"인 항목들 제거
        remove_mask = bs_mask & (df_copy['label_ko'] == '자본과부채총계')

        removed_count = len(df_copy[remove_mask])
        df_copy = df_copy[~remove_mask].reset_index(drop=True)

        if removed_count > 0:
            print(f"   '자본과부채총계' 항목 제거: {removed_count}개 항목 제거됨")

        print(f"\n[STEP 9] 계층 구조 개선 완료")
        bs_data = df_copy[df_copy['report_type'] == 'BS']
        print(f"  → 개선 후 BS DataFrame: {len(bs_data)}행 (원래: {original_count}행)")
        self._check_ppe_existence(bs_data, "STEP 9: 계층 구조 개선 후")

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

            print(f"\n[STEP 10] 재무제표 데이터 통합")
            print(f"  → 통합 DataFrame: BS {len(combined_df[combined_df['report_type'] == 'BS'])}행 + CIS {len(combined_df[combined_df['report_type'] == 'CIS'])}행")
            bs_data = combined_df[combined_df['report_type'] == 'BS']
            if not bs_data.empty:
                self._check_ppe_existence(bs_data, "STEP 10: 데이터 통합 후")

            # 재무상태표 계층 구조 개선
            combined_df = self.improve_hierarchy_structure(combined_df)

            print(f"\n[STEP 11] improve_hierarchy_structure 호출 후")
            bs_data = combined_df[combined_df['report_type'] == 'BS']
            if not bs_data.empty:
                self._check_ppe_existence(bs_data, "STEP 11: 계층 구조 개선 함수 호출 후")

            # report_type 기준으로 정렬 (BS 먼저, 그 다음 CIS)
            if 'report_type' in combined_df.columns:
                combined_df = combined_df.sort_values(['report_type', 'order_no'], na_position='last').reset_index(drop=True)

                print(f"\n[STEP 12] 최종 정렬 후 (저장 직전)")
                bs_data = combined_df[combined_df['report_type'] == 'BS']
                if not bs_data.empty:
                    self._check_ppe_existence(bs_data, "STEP 12: CSV 저장 직전")

            # 통합 파일명 생성 (FS_ 접두사 사용)
            output_file = self.generate_output_filename(xbrl_path, 'FS', metadata)

            print(f"\n통합 재무제표 데이터: {len(combined_df)}행")
            print(f"  - BS 데이터: {len(combined_df[combined_df['report_type'] == 'BS'])}행")
            print(f"  - CIS 데이터: {len(combined_df[combined_df['report_type'] == 'CIS'])}행")

            if self.save_to_csv(combined_df, output_file):
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


def main():
    """메인 함수"""
    if len(sys.argv) != 2:
        print("사용법: python xbrl_processor_debug.py <xbrl_file_path>")
        print("예시: python xbrl_processor_debug.py entity00171636_2025-06-30.xbrl")
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
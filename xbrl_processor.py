#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
XBRL 파일을 처리하여 최종 CSV 파일을 생성하는 메인 처리 엔진

기능:
1. XBRL 파일에서 연결재무상태표, 연결손익계산서 추출
2. 데이터를 피벗 포맷으로 변환
3. 메타데이터 추가
4. 최종 UTF-8-sig CSV 파일 생성 (중간 파일 없이)

사용법:
    python xbrl_processor.py <xbrl_file_path>

예시:
    python xbrl_processor.py 20250813001262_ifrs/entity00171636_2025-06-30.xbrl
"""

import os
import sys
import pandas as pd
from pathlib import Path
from dart_fss.xbrl import get_xbrl_from_file
import re
from datetime import datetime


class XBRLProcessor:
    """XBRL 파일 처리를 위한 메인 클래스"""

    def __init__(self):
        """초기화"""
        pass

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

        try:
            # 법인명 추출
            entity_info = xbrl.get_entity_information()
            corp_name_row = entity_info[entity_info.iloc[:, 0].str.contains('법인명', na=False)]
            if not corp_name_row.empty:
                metadata['corp_name'] = str(corp_name_row.iloc[0, 2]).strip()
            else:
                metadata['corp_name'] = ''
        except Exception as e:
            print(f"법인명 추출 중 오류: {e}")
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

            if ENABLE_PERIOD_FILTERING and not result_df.empty and 'report_nm' in metadata:
                # 보고서명에서 기간 정보 추출 (예: "반기보고서 (2025.06)" -> "202506")
                report_period_yyyymm = self.extract_period_from_report_name(metadata.get('report_nm', ''))

                if report_period_yyyymm and len(report_period_yyyymm) == 6:
                    # YYYYMM을 YYYY-MM 형태로 변환 (예: "202506" -> "2025-06")
                    target_year = report_period_yyyymm[:4]
                    target_month = report_period_yyyymm[4:6]
                    target_period_pattern = f"{target_year}-{target_month}"

                    print(f"[FILTER] 보고서 기간 필터링 적용: {metadata.get('report_nm', '')} -> {target_period_pattern}")
                    print(f"   필터링 전 데이터 수: {len(result_df)}행")

                    # period 컬럼에서 해당 년월이 포함된 행만 필터링
                    # 예: period가 "2025-06-30" 또는 "2025-01-01 ~ 2025-06-30" 형태일 때
                    #     target_period_pattern "2025-06"이 포함되어 있으면 유지
                    if 'period' in result_df.columns:
                        original_count = len(result_df)

                        # 필터링 조건: period 컬럼에 target_period_pattern이 포함된 행만 유지
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
                    print(f"   [WARNING] 보고서명에서 기간 정보를 추출할 수 없어 필터링을 건너뜁니다: {metadata.get('report_nm', '')}")
            else:
                if not ENABLE_PERIOD_FILTERING:
                    print("   [INFO] 보고서 기간 필터링이 비활성화되어 있습니다.")
                elif result_df.empty:
                    print("   [INFO] 데이터가 없어 필터링을 건너뜁니다.")
                else:
                    print("   [INFO] 보고서명 정보가 없어 필터링을 건너뜁니다.")

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
        """출력 파일명 생성"""
        corp_code = metadata.get('corp_code', '00000000')

        # 보고서명에서 년월 추출 시도
        period_from_report = self.extract_period_from_report_name(report_nm)

        if period_from_report:
            # 새로운 파일명 형태: (BS/CIS)_회사코드_년월.csv
            return f"{report_type}_{corp_code}_{period_from_report}.csv"
        else:
            # 기존 파일명 형태 (fallback)
            base_name = Path(xbrl_path).stem
            corp_name = metadata.get('corp_name', 'unknown')
            yyyy = metadata.get('yyyy', 'unknown')
            month = metadata.get('month', 'unknown')
            report_name = '연결재무상태표' if report_type == 'BS' else '연결손익계산서'
            return f"{base_name}_{corp_name}_{yyyy}_{month}_{report_name}_피벗포맷_메타데이터포함.csv"

    def save_to_csv(self, df, output_path):
        """DataFrame을 UTF-8-sig CSV로 저장 (crawl_time 컬럼 추가)"""
        if df.empty:
            print("저장할 데이터가 없습니다.")
            return False

        try:
            # crawl_time 컬럼 추가 (현재 시간)
            df_copy = df.copy()
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            df_copy['crawl_time'] = current_time

            # CSV 저장
            df_copy.to_csv(output_path, index=False, encoding='utf-8-sig')
            print(f"파일 저장 완료: {output_path}")
            print(f"총 {len(df_copy)}행 저장됨 (crawl_time: {current_time})")
            return True
        except Exception as e:
            print(f"파일 저장 중 오류: {e}")
            return False

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

            # report_type 기준으로 정렬 (BS 먼저, 그 다음 CIS)
            if 'report_type' in combined_df.columns:
                combined_df = combined_df.sort_values(['report_type', 'order_no'], na_position='last').reset_index(drop=True)

            # 통합 파일명 생성 (FS_ 접두사 사용)
            output_file = self.generate_output_filename(xbrl_path, 'FS', metadata)

            print(f"통합 재무제표 데이터: {len(combined_df)}행")
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

    def process_xbrl_file_with_report_info(self, xbrl_path, report_nm=""):
        """
        XBRL 파일을 처리하여 최종 CSV 파일들을 생성 (보고서 정보 포함)

        Args:
            xbrl_path (str): XBRL 파일 경로
            report_nm (str): 보고서명 (예: "반기보고서 (2025.06)")

        Returns:
            list: 생성된 파일 경로들
        """
        print("=== XBRL 파일 처리 시작 ===")
        print(f"입력 파일: {xbrl_path}")
        if report_nm:
            print(f"보고서명: {report_nm}")

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

                # report_type 기준으로 정렬 (BS 먼저, 그 다음 CIS)
                if 'report_type' in combined_df.columns:
                    combined_df = combined_df.sort_values(['report_type', 'order_no'], na_position='last').reset_index(drop=True)

                # 통합 파일명 생성 (FS_ 접두사 사용)
                output_file = self.generate_output_filename(xbrl_path, 'FS', metadata, report_nm)

                print(f"통합 재무제표 데이터: {len(combined_df)}행")
                print(f"  - BS 데이터: {len(combined_df[combined_df['report_type'] == 'BS'])}행")
                print(f"  - CIS 데이터: {len(combined_df[combined_df['report_type'] == 'CIS'])}행")

                if self.save_to_csv(combined_df, output_file):
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
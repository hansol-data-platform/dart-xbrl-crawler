#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
XBRL 파일에서 메타데이터를 추출하는 공통 함수들

사용할 수 있는 메타데이터:
- yyyy: 보고서 년도
- month: 보고서 월
- corp_code: 법인코드 (8자리, 0패딩)
- corp_name: 법인명
"""

import re
import pandas as pd


def extract_metadata_from_xbrl(xbrl):
    """
    XBRL 객체에서 메타데이터를 추출

    Args:
        xbrl: dart_fss XBRL 객체

    Returns:
        dict: 추출된 메타데이터
            - yyyy: 보고서 년도 (str)
            - month: 보고서 월 (str)
            - corp_code: 법인코드 8자리 (str)
            - corp_name: 법인명 (str)
    """
    metadata = {}

    # 파일명에서 법인코드 추출 (entity 다음 8자리)
    try:
        filename = xbrl.filename
        if 'entity' in filename:
            # entity 다음의 8자리 숫자 추출
            import re
            match = re.search(r'entity(\d{8})', filename)
            if match:
                metadata['corp_code'] = match.group(1)
            else:
                metadata['corp_code'] = '00000000'
        else:
            metadata['corp_code'] = '00000000'
    except:
        metadata['corp_code'] = '00000000'

    try:
        # 1. Entity Information에서 법인 정보 추출
        entity_info = xbrl.get_entity_information()

        # 법인명 추출
        corp_name_row = entity_info[entity_info.iloc[:, 0].str.contains('법인명', na=False)]
        if not corp_name_row.empty:
            metadata['corp_name'] = str(corp_name_row.iloc[0, 2]).strip()
        else:
            metadata['corp_name'] = ''

        # 법인코드는 이미 파일명에서 추출했으므로 여기서는 건너뜀

    except Exception as e:
        print(f"Entity 정보 추출 중 오류: {e}")
        metadata['corp_name'] = ''

    try:
        # 2. Period Information에서 날짜 정보 추출
        period_info = xbrl.get_period_information()

        # 컬럼명에서 날짜 범위 찾기 (예: 20250101-20250630)
        date_columns = [col for col in period_info.columns if isinstance(col, (str, tuple))]

        date_range = None
        for col in date_columns:
            col_str = str(col[0]) if isinstance(col, tuple) else str(col)
            if re.match(r'\d{8}-\d{8}', col_str):
                date_range = col_str
                break

        if date_range:
            # 종료일에서 년도와 월 추출
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
        print(f"Period 정보 추출 중 오류: {e}")
        metadata['yyyy'] = ''
        metadata['month'] = ''

    return metadata


def add_metadata_to_dataframe(df, metadata, report_type):
    """
    DataFrame에 메타데이터 컬럼들을 추가

    Args:
        df (pd.DataFrame): 원본 DataFrame
        metadata (dict): 메타데이터 딕셔너리
        report_type (str): 'BS' (재무상태표) 또는 'CIS' (손익계산서)

    Returns:
        pd.DataFrame: 메타데이터가 추가된 DataFrame
    """
    if df.empty:
        return df

    df_copy = df.copy()

    # 새 컬럼들을 맨 앞에 추가
    df_copy.insert(0, 'yyyy', metadata.get('yyyy', ''))
    df_copy.insert(1, 'month', metadata.get('month', ''))
    df_copy.insert(2, 'corp_code', metadata.get('corp_code', '00000000'))
    df_copy.insert(3, 'corp_name', metadata.get('corp_name', ''))
    df_copy.insert(4, 'report_type', report_type)

    return df_copy


def print_metadata_summary(metadata, report_type):
    """메타데이터 정보를 출력"""
    print(f"\n=== 추출된 메타데이터 ===")
    print(f"보고서 년도 (yyyy): {metadata.get('yyyy', 'N/A')}")
    print(f"보고서 월 (month): {metadata.get('month', 'N/A')}")
    print(f"법인코드 (corp_code): {metadata.get('corp_code', 'N/A')}")
    print(f"법인명 (corp_name): {metadata.get('corp_name', 'N/A')}")
    print(f"레포트구분 (report_type): {report_type}")
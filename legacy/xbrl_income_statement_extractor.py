#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
XBRL 파일에서 연결손익계산서 데이터를 추출하는 스크립트

사용법:
    python xbrl_income_statement_extractor.py <xbrl_file_path>

예시:
    python xbrl_income_statement_extractor.py 20250813001262_ifrs/entity00171636_2025-06-30.xbrl
"""

import os
import sys
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv
import dart_fss as dart
from dart_fss.xbrl import get_xbrl_from_file
from xbrl_metadata_extractor import extract_metadata_from_xbrl, add_metadata_to_dataframe, print_metadata_summary


def load_dart_api_key():
    """환경변수에서 DART API 키를 로드"""
    load_dotenv()
    api_key = os.getenv('DART_API_KEY')
    if not api_key:
        raise ValueError("DART_API_KEY가 .env 파일에 설정되지 않았습니다.")
    return api_key


def extract_consolidated_income_statement(xbrl_file_path):
    """
    XBRL 파일에서 연결손익계산서 데이터를 추출

    Args:
        xbrl_file_path (str): XBRL 파일 경로

    Returns:
        tuple: (pd.DataFrame, dict) 연결손익계산서 데이터와 메타데이터
    """
    try:
        # XBRL 파일 읽기
        xbrl = get_xbrl_from_file(xbrl_file_path)

        # 메타데이터 추출
        metadata = extract_metadata_from_xbrl(xbrl)
        print_metadata_summary(metadata, 'CIS')

        # 연결손익계산서 추출 (separate=False는 연결손익계산서)
        income_statements = xbrl.get_income_statement(separate=False)

        if not income_statements:
            print("연결손익계산서를 찾을 수 없습니다.")
            return pd.DataFrame(), metadata

        # 첫 번째 손익계산서 테이블 사용
        income_statement = income_statements[0]

        # 테이블을 DataFrame으로 변환
        df = income_statement.to_DataFrame()

        if df.empty:
            print("손익계산서 데이터가 비어있습니다.")
            return pd.DataFrame(), metadata

        # 메타데이터 컬럼 추가
        df_with_metadata = add_metadata_to_dataframe(df, metadata, 'CIS')

        return df_with_metadata, metadata

    except Exception as e:
        print(f"데이터 추출 중 오류 발생: {e}")
        return pd.DataFrame(), {}


def format_currency(value):
    """숫자를 통화 형식으로 포맷팅"""
    try:
        if pd.isna(value) or value == 0:
            return "0"

        # 억 단위로 변환
        value_in_oku = value / 100000000
        if abs(value_in_oku) >= 1:
            return f"{value_in_oku:,.1f}억원"
        else:
            # 백만원 단위로 표시
            value_in_million = value / 1000000
            return f"{value_in_million:,.0f}백만원"
    except:
        return str(value)


def save_results(df, xbrl_file_path, metadata):
    """결과를 CSV 파일로 저장"""
    if df.empty:
        print("추출된 데이터가 없습니다.")
        return

    # 출력 파일명 생성 (메타데이터 포함)
    base_name = Path(xbrl_file_path).stem
    corp_name = metadata.get('corp_name', 'unknown')
    yyyy = metadata.get('yyyy', 'unknown')
    month = metadata.get('month', 'unknown')

    output_file = f"{base_name}_{corp_name}_{yyyy}_{month}_연결손익계산서_메타데이터포함.csv"

    # CSV로 저장
    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"결과가 '{output_file}'에 저장되었습니다.")
    print(f"총 {len(df)}행의 데이터가 저장되었으며, 메타데이터 컬럼이 추가되었습니다.")


def main():
    """메인 함수"""
    if len(sys.argv) != 2:
        print("사용법: python xbrl_income_statement_extractor.py <xbrl_file_path>")
        print("예시: python xbrl_income_statement_extractor.py 20250813001262_ifrs/entity00171636_2025-06-30.xbrl")
        sys.exit(1)

    xbrl_file_path = sys.argv[1]

    # 파일 존재 확인
    if not os.path.exists(xbrl_file_path):
        print(f"파일을 찾을 수 없습니다: {xbrl_file_path}")
        sys.exit(1)

    try:
        # DART API 키 로드
        api_key = load_dart_api_key()
        dart.set_api_key(api_key)
        print(f"DART API 키가 설정되었습니다.")

        print(f"XBRL 파일 분석 중: {xbrl_file_path}")

        # 연결손익계산서 추출
        income_statement_df, metadata = extract_consolidated_income_statement(xbrl_file_path)

        if not income_statement_df.empty:
            print("\n=== 연결손익계산서 (메타데이터 포함) ===")
            print(income_statement_df.head(10).to_string())
            if len(income_statement_df) > 10:
                print(f"... 총 {len(income_statement_df)}행의 데이터")

            # 결과 저장
            save_results(income_statement_df, xbrl_file_path, metadata)

        else:
            print("연결손익계산서 데이터를 추출할 수 없습니다.")

    except Exception as e:
        print(f"오류 발생: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
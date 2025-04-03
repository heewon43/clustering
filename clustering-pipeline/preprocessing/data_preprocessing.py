# data_preprocessing.py
import pandas as pd
from data_fetch.fetch_data import DataFetcher

class DataPreprocessor(DataFetcher):
    def __init__(self):
        super().__init__()
        self.prnts_sales_history_data = None
        self.prnts_remain_points_data = None
        self.prnts_lounge_activity_data = None
        self.cleaned_data = None

    def load_data(self):
        # 부모-교사-자녀 매핑 테이블 데이터 가져오기
        self.prnts_sales_history_data = self.fetch_prnts_sales_history()
        if self.prnts_sales_history_data is not None:
            print("Parents Sales History Data loaded successfully")

        # 부모 잔여 포인트 데이터 가져오기
        self.prnts_remain_points_data = self.fetch_prnts_remain_points()
        if self.prnts_remain_points_data is not None:
            print("Parents Remain Points Data loaded successfully")

        # 부모 라운지 활동 내역 데이터 가져오기
        self.prnts_lounge_activity_data = self.fetch_prnts_lounge_activity()
        if self.prnts_lounge_activity_data is not None:
            print("Parents Lounge Activity Data loaded successfully")

        # 부모 나이 정보 가져오기
        self.prnts_age_info_data  = self.fetch_prnts_age_info()
        if self.prnts_age_info_data is not None:
            print("Parents Age Data loaded successfully")

    def process_data(self):
        # 데이터 병합 및 결측치 제거
        if (self.prnts_sales_history_data is not None) and (self.prnts_remain_points_data is not None) and (self.prnts_lounge_activity_data is not None):
            # 데이터 병합 (inner join) - 'prnts_cstmr_id' 기준
            merged_data = self.prnts_sales_history_data.merge(self.prnts_remain_points_data, on='prnts_cstmr_id', how='inner')
            merged_data = merged_data.merge(self.prnts_age_info_data, on ='prnts_cstmr_id', how='inner')
            merged_data = merged_data.merge(self.prnts_lounge_activity_data, on='prnts_cstmr_id', how='inner')

            # 결측치 제거
            self.cleaned_data = merged_data.dropna()

            # 결과 출력
            print("\nMerged Data after inner join and NaN removal:")
            print(self.cleaned_data.head())
        else:
            print("One or more datasets could not be loaded successfully")

# 나이도 merge 추가함
# Parents Sales History Data , Parents Remain Points Data , Parents Lounge Activity Data, prnts_age_info_data 이 4개를 가져와서 'prnts_cstmr_id' 기준으로 inner join
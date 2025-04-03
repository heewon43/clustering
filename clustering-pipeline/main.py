# import sys
# import os
#
# # 현재 파일의 경로를 Python Path에 추가
# sys.path.append('/home/drave12/smart_sales/smart-sales-pipeline')
from data_fetch.query import QueryProvider
from data_fetch.fetch_data import DataFetcher
from preprocessing.data_preprocessing import DataPreprocessor
from clustering.clustering_parents import ClusteringProcessor
from clustering.clustering_child import ClusteringChildProcessor

if __name__ == "__main__":
    # DataFetcher 인스턴스 생성
    fetcher = DataFetcher()

    # # Redshift에서 부모-교사-자녀 매핑 테이블 데이터 가져오기
    # prnts_sales_history_data = fetcher.fetch_prnts_sales_history()
    # if prnts_sales_history_data is not None:
    #     print("Parents Sales History Data:")
    #     print(prnts_sales_history_data.head())
    #
    # # Redshift에서 부모 잔여 포인트 데이터 가져오기
    # prnts_remain_points_data = fetcher.fetch_prnts_remain_points()
    # if prnts_remain_points_data is not None:
    #     print("\nParents Remain Points Data:")
    #     print(prnts_remain_points_data.head())
    #
    # # athena에서 부모 라운지 활동 내역 데이터 가져오기
    # prnts_lounge_activity_data = fetcher.fetch_prnts_lounge_activity()
    # if prnts_lounge_activity_data is not None:
    #     print("\nParents Lounge Activity Data:")
    #     print(prnts_lounge_activity_data.head())

    # # Athena에서 사용자 히스토리 데이터 가져오기
    # user_history_data = fetcher.fetch_user_history()
    # if user_history_data is not None:
    #     print("\nUser History Data:")
    #     print(user_history_data.head())
    #
    # ## # Athena에서 고객 ID 정보 데이터 가져오기
    # ## customer_id_info_data = fetcher.fetch_customer_id_info()
    # ## if customer_id_info_data is not None:
    # ##     print("\nCustomer ID Info Data:")
    # ##     print(customer_id_info_data.head())
    #
    # # Athena에서 부모 나이 정보 데이터 가져오기
    # prnts_age_info_data = fetcher.fetch_prnts_age_info()
    # if prnts_age_info_data is not None:
    #     print("\nParents Age Info Data:")
    #     print(prnts_age_info_data.head())

    ############################## S3에서 데이터 가져오기 ###################################
    # S3에서 특정 두 개의 CSV 파일 가져오기 (child_clustering.csv, parents_clustering.csv)
    specific_csv_data = fetcher.fetch_child_and_parents_clustering()
    if specific_csv_data is not None:
        if 'child_clustering.csv' in specific_csv_data:
            print("\nChild Clustering Data:")
            print(specific_csv_data['child_clustering.csv'].head())

        if 'parents_clustering.csv' in specific_csv_data:
            print("\nParents Clustering Data:")
            print(specific_csv_data['parents_clustering.csv'].head())


##############################전처리 ###################################
    # DataPreprocessor 인스턴스 생성 및 데이터 로드 및 전처리 실행 및 확인
    preprocessor = DataPreprocessor()
    preprocessor.load_data()
    preprocessor.process_data()

    # 전처리된 데이터 출력
    if preprocessor.cleaned_data is not None:
        print("\nCleaned Merged Data:")
        print(preprocessor.cleaned_data.head())


##############################클러스터링 처리 ###################################
    # 클러스터링 실행
    clustering_processor = ClusteringProcessor()
    clustering_processor.run()

    # 자녀 클러스터링 처리
    child_clustering_processor = ClusteringChildProcessor()
    child_clustering_processor.run()
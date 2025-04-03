import boto3
import psycopg2
import pandas as pd
from dotenv import load_dotenv
import os
from pyathena import connect
from data_fetch.query import QueryProvider  # 쿼리 클래스를 import

class DataFetcher:
    def __init__(self):
        # 환경 변수 로드
        load_dotenv()

        # AWS 및 Redshift 연결 정보 설정
        self.access_key = os.getenv('access_key')
        self.secret_key = os.getenv('secret_key')
        self.bucket_name = os.getenv('bucket_name')
        self.region = os.getenv('region')
        self.s3_staging_dir = os.getenv('s3_staging_dir')
        self.workgroup = os.getenv('workgroup')

        # Redshift 연결 정보
        self.redshift_host = os.getenv('redshift_host')
        self.redshift_port = os.getenv('redshift_port', '5439')  # 기본 포트는 5439
        self.redshift_user = os.getenv('redshift_user')
        self.redshift_password = os.getenv('redshift_password')
        self.redshift_dbname = os.getenv('redshift_dbname')

        # QueryProvider 인스턴스 생성
        self.query_provider = QueryProvider()

        # S3 및 Athena 클라이언트 초기화
        self.s3_client = boto3.client('s3',
                                      aws_access_key_id=self.access_key,
                                      aws_secret_access_key=self.secret_key,
                                      region_name=self.region)
        self.athena_client = connect(aws_access_key_id=self.access_key,
                                     aws_secret_access_key=self.secret_key,
                                     s3_staging_dir=self.s3_staging_dir,
                                     region_name=self.region,
                                     work_group=self.workgroup)

    # Athena에서 데이터를 가져오는 함수
    def fetch_data_from_athena(self, query):
        try:
            # Athena 쿼리 실행
            df = pd.read_sql(query, self.athena_client)
            return df
        except Exception as e:
            print(f"Error fetching data from Athena: {e}")
            return None

    # Redshift에서 데이터를 가져오는 함수
    def fetch_data_from_redshift(self, query):
        try:
            # Redshift 데이터베이스 연결
            conn = psycopg2.connect(
                dbname=self.redshift_dbname,
                user=self.redshift_user,
                password=self.redshift_password,
                host=self.redshift_host,
                port=self.redshift_port
            )

            # 쿼리 실행 및 결과 반환
            df = pd.read_sql_query(query, conn)
            conn.close()
            return df
        except Exception as e:
            print(f"Error fetching data from Redshift: {e}")
            return None

    # Redshift용 쿼리 실행 함수: 교사-부모-자녀 매핑 테이블
    def fetch_prnts_sales_history(self):
        query = self.query_provider.get_redshift_prnts_sales_history_query()
        return self.fetch_data_from_redshift(query)

    # Redshift용 쿼리 실행 함수: 부모 잔여 포인트 내역
    def fetch_prnts_remain_points(self):
        query = self.query_provider.get_redshift_prnts_remain_points_query()
        return self.fetch_data_from_redshift(query)

    # Athena용 쿼리 실행 함수: 부모 라운지 활동 내역
    def fetch_prnts_lounge_activity(self):
        query = self.query_provider.get_athena_prnts_lounge_activity_query()
        return self.fetch_data_from_athena(query)

    # Athena용 쿼리 실행 함수: 사용자의 히스토리 정보
    def fetch_user_history(self):
        query = self.query_provider.get_athena_child_history_query()
        return self.fetch_data_from_athena(query)

    # Athena용 쿼리 실행 함수: 고객 ID 정보
    # def fetch_customer_id_info(self):
    #     query = self.query_provider.get_athena_customer_id_info_query()
    #     return self.fetch_data_from_athena(query)

    # Athena용 쿼리 실행 함수: 부모의 나이 정보
    def fetch_prnts_age_info(self):
        query = self.query_provider.get_athena_prnts_age_info_query()
        return self.fetch_data_from_athena(query)

    ################################################################################################
    ################################################################################################
    # S3에서 특정 CSV 파일 가져오는 함수
    def fetch_specific_csv_from_s3(self, s3_prefix, file_names):
        try:
            # 지정된 S3 경로에서 모든 객체 목록 가져오기
            response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=s3_prefix)
            if 'Contents' not in response:
                print("No files found at specified S3 location.")
                return None

            # 특정 파일을 담을 데이터프레임 딕셔너리 초기화
            specific_dfs = {}

            for obj in response['Contents']:
                key = obj['Key']
                file_name = key.split('/')[-1]
                if file_name in file_names:
                    # S3에서 CSV 파일 읽기
                    response = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
                    df = pd.read_csv(response['Body'], dtype={'chldn_cstmr_id':str, 'prnts_cstmr_id':str}) # 여기 수정함
                    specific_dfs[file_name] = df

            # 파일별 데이터프레임 반환
            if specific_dfs:
                return specific_dfs
            else:
                print("No specified CSV files found at S3 location.")
                return None

        except Exception as e:
            print(f"Error fetching specific CSV files from S3: {e}")
            return None

    # S3에서 특정 두 개의 CSV 파일 가져오는 함수
    def fetch_child_and_parents_clustering(self):
        s3_prefix = 'research/24y_smart_sales/clustering/'
        file_names = ['child_clustering.csv', 'parents_clustering.csv']
        specific_dfs = self.fetch_specific_csv_from_s3(s3_prefix, file_names)
        if specific_dfs:
            self.child_clustering_data = specific_dfs.get('child_clustering.csv')
            self.parents_clustering_data = specific_dfs.get('parents_clustering.csv')
            return specific_dfs
        else:
            return None

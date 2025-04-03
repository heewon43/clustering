from sklearn.preprocessing import StandardScaler
from sklearn.manifold import TSNE
from sklearn.neighbors import NearestNeighbors
from preprocessing.data_preprocessing import DataPreprocessor
from data_fetch.fetch_data import DataFetcher
import boto3
import io
import numpy as np
import pandas as pd

class ClusteringChildProcessor:
    def __init__(self):
        self.fetcher = DataFetcher()
        self.child_clustering_data = None
        self.user_history_data = None
        self.scaler = StandardScaler()
        self.aws_data = {
            'access_key': self.fetcher.access_key,
            'secret_key': self.fetcher.secret_key,
            'region': self.fetcher.region,
            'bucket_name': self.fetcher.bucket_name
        }

    def load_and_sample_data(self):
        # 1. Child Clustering Data 가져오기
        self.child_clustering_data = self.fetcher.fetch_child_and_parents_clustering().get('child_clustering.csv')
        if self.child_clustering_data is None:
            raise ValueError("Error loading Child Clustering Data")

        # 샘플링
        sampled_df = self.child_clustering_data.sample(n=5000, random_state=42)
        sampled_df_col = sampled_df[[
            "age",
            "ct_click",
            "ct_completed",
            "video_count",
            "ebook_count",
            "lang",
            "social_history",
            "encyclopedia",
            "math_science",
            "culture_art"]]

        return sampled_df, sampled_df_col

    def perform_scaling_and_tsne(self, sampled_df_col):
        # 2. 스케일링 적용
        scaled_data = self.scaler.fit_transform(sampled_df_col)

        # 3. 차원 축소 t-SNE
        tsne = TSNE(n_components=2, random_state=42)
        tsne_result = tsne.fit_transform(scaled_data)
        return tsne_result

    def calculate_cluster_centers(self, tsne_result, sampled_df, clusters):
        # 4. 클러스터 중심 계산
        sampled_df["tsne_1"] = tsne_result[:, 0]
        sampled_df["tsne_2"] = tsne_result[:, 1]
        cluster_centers = sampled_df.groupby("cluster")[["tsne_1", "tsne_2"]].mean()

        cluster_centers_high_dim = sampled_df.groupby("cluster")[[
            "age",
            "ct_click",
            "ct_completed",
            "video_count",
            "ebook_count",
            "lang",
            "social_history",
            "encyclopedia",
            "math_science",
            "culture_art"]].mean()

        print(cluster_centers)
        return cluster_centers_high_dim

    def process_user_history(self):
        # 5. fetch_user_history를 사용하여 데이터 로드
        self.user_history_data = self.fetcher.fetch_user_history()
        if self.user_history_data is None:
            raise ValueError("Error loading User History Data")

        rest_of_data = self.user_history_data
        scaled_rest_of_data = rest_of_data[[
            "age",
            "ct_click",
            "ct_completed",
            "video_count",
            "ebook_count",
            "lang",
            "social_history",
            "encyclopedia",
            "math_science",
            "culture_art"]]

        return rest_of_data, scaled_rest_of_data

    def assign_clusters(self, scaled_rest_of_data, cluster_centers_high_dim, rest_of_data):
        # 6. Nearest neighbors 방법으로 사용자 클러스터 할당
        neighbors = NearestNeighbors(n_neighbors=1)
        neighbors.fit(cluster_centers_high_dim)
        distances, indices = neighbors.kneighbors(scaled_rest_of_data)
        cluster = indices.flatten()
        rest_of_data["cluster"] = cluster

        print(cluster_centers_high_dim)
        print(rest_of_data)

        return rest_of_data

    def save_to_s3(self, rest_of_data, cluster_centers_high_dim):
        # 최종 클러스터링 결과와 클러스터 정보 S3에 저장
        csv_buffer = io.StringIO()
        rest_of_data.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)

        s3_client = boto3.client('s3',
                                 aws_access_key_id=self.aws_data['access_key'],
                                 aws_secret_access_key=self.aws_data['secret_key'],
                                 region_name=self.aws_data['region'])

        current_date = pd.Timestamp.now().strftime('%Y-%m-%d')

        # S3에 클러스터 할당된 데이터 저장
        s3_client.put_object(
            Bucket=self.aws_data['bucket_name'],
            Key=f'research/24y_smart_sales/clustering_output/child_clustering_output/stnd_ymd={current_date}/child_clustering_result.csv',
            Body=csv_buffer.getvalue()
        )

        # 클러스터 중심 정보 S3에 저장
        csv_buffer = io.StringIO()
        cluster_centers_high_dim.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)

        s3_client.put_object(
            Bucket=self.aws_data['bucket_name'],
            Key=f'research/24y_smart_sales/clustering_output/child_clustering_output/stnd_ymd={current_date}/child_cluster_centers.csv',
            Body=csv_buffer.getvalue()
        )

    def run(self):
        # Load and sample data
        sampled_df, sampled_df_col = self.load_and_sample_data()

        # Perform scaling and t-SNE
        tsne_result = self.perform_scaling_and_tsne(sampled_df_col)

        # Calculate cluster centers
        clusters = sampled_df.get("cluster", -1)  # Assuming clustering results are already present in the dataframe
        cluster_centers_high_dim = self.calculate_cluster_centers(tsne_result, sampled_df, clusters)

        # Process user history and assign clusters
        rest_of_data, scaled_rest_of_data = self.process_user_history()
        rest_of_data = self.assign_clusters(scaled_rest_of_data, cluster_centers_high_dim, rest_of_data)

        # Save the results to S3
        self.save_to_s3(rest_of_data, cluster_centers_high_dim)
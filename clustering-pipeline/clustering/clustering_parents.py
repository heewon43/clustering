# clustering.py
from sklearn.preprocessing import StandardScaler
from sklearn.manifold import TSNE
from sklearn.neighbors import NearestNeighbors
from preprocessing.data_preprocessing import DataPreprocessor
from data_fetch.fetch_data import DataFetcher
import boto3
import io
import pandas as pd

class ClusteringProcessor:
    def __init__(self):
        self.fetcher = DataFetcher()  # 인스턴스화하여 사용
        self.preprocessor = DataPreprocessor()  # 인스턴스화하여 사용
        self.parents_clustering_data = None
        self.cleaned_data = None
        self.scaler = StandardScaler()
        self.s3_client = boto3.client('s3',
                                      aws_access_key_id=self.fetcher.access_key,
                                      aws_secret_access_key=self.fetcher.secret_key,
                                      region_name=self.fetcher.region)

    def load_and_sample_data(self):
        # 1. Parents Clustering Data 가져오기
        self.parents_clustering_data = self.fetcher.fetch_child_and_parents_clustering().get('parents_clustering.csv')
        if self.parents_clustering_data is None:
            raise ValueError("Error loading Parents Clustering Data")

        # 샘플링
        sampled_df = self.parents_clustering_data.sample(n=5000, random_state=42)
        sampled_df_col = sampled_df[[
            "recent_purchase_in_months",
            "total_purchase_count",
            "total_purchase_amount",
            "purchase_freque_last_6_months",
            "purchase_cost_last_6_months",
            "total_series_count",
            "remain_point",
            "age",
            'months_since_last_visit', 'lounge_home_visit_count',
            'search_count_total', 'mtrl_search_count', 'ai_reading_coaching_total']]

        return sampled_df, sampled_df_col

    def perform_scaling_and_tsne(self, sampled_df_col):
        # 2. 스케일링 적용
        scaled_data = self.scaler.fit_transform(sampled_df_col)

        # 3. 차원 축소 t-sne
        tsne = TSNE(n_components=2, random_state=42)
        tsne_result = tsne.fit_transform(scaled_data)
        return tsne_result

    def calculate_cluster_centers(self, tsne_result, sampled_df, clusters):
        # 4. 클러스터 중심 계산
        sampled_df["tsne_1"] = tsne_result[:, 0]
        sampled_df["tsne_2"] = tsne_result[:, 1]
        cluster_centers = sampled_df.groupby("cluster")[["tsne_1", "tsne_2"]].mean()

        cluster_centers_high_dim = sampled_df.groupby("cluster")[[
            "recent_purchase_in_months",
            "total_purchase_count",
            "total_purchase_amount",
            "purchase_freque_last_6_months",
            "purchase_cost_last_6_months",
            "total_series_count",
            "remain_point",
            "age"]].mean()

        print(cluster_centers)
        return cluster_centers_high_dim

    def process_cleaned_data(self):
        # 5. Cleaned Merged Data 가져오기
        self.preprocessor.load_data()
        self.preprocessor.process_data()
        if self.preprocessor.cleaned_data is None:
            raise ValueError("Error loading Cleaned Data")

        rest_of_data = self.preprocessor.cleaned_data
        scaled_rest_of_data = rest_of_data[[
            "recent_purchase_in_months",
            "total_purchase_count",
            "total_purchase_amount",
            "purchase_freque_last_6_months",
            "purchase_cost_last_6_months",
            "total_series_count",
            "remain_point",
            "age"]]

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

        # 결과를 S3에 저장
        csv_buffer = io.StringIO()
        rest_of_data.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)

        current_date = pd.Timestamp.now().strftime('%Y-%m-%d')
        self.s3_client.put_object(
            Bucket=self.fetcher.bucket_name,
            Key=f'research/24y_smart_sales/clustering_output/parents_clustering_output/parents_clustering_{current_date}.csv',
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

        # Process cleaned data and assign clusters
        rest_of_data, scaled_rest_of_data = self.process_cleaned_data()
        self.assign_clusters(scaled_rest_of_data, cluster_centers_high_dim, rest_of_data)


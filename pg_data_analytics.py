from pyspark.sql.functions import col, lit, avg, udf
from pyspark.sql.types import DoubleType
from spark_session import create_spark_session

class PGDataAnalyticsReckoner:
    def __init__(self):
        self.spark = create_spark_session()
        self.opulentpoint_saledata_df = None
        self.trade_odyssey_records_df = None
        self.harmony_nexus_df = None
        self.filtered_refined_data_df = None
        self.mean_revenue = None
        self.refined_data_df = None
        self.combined_refined_data_df = None

    def read_data(self, pos_path, trade_path):
        self.opulentpoint_saledata_df = self.spark.read.csv(pos_path, header=True, inferSchema=True)
        self.trade_odyssey_records_df = self.spark.read.csv(trade_path, header=True, inferSchema=True)

    def join_data(self):
        self.harmony_nexus_df = self.opulentpoint_saledata_df.join(self.trade_odyssey_records_df, "Company", "inner")

    def write_data(self, output_path):
        self.opulentpoint_saledata_df.write.csv(output_path, header=True, mode="overwrite")

    def filter_data(self):
        self.filtered_refined_data_df = self.opulentpoint_saledata_df.filter(col("Volume Sales MSU") > 4000)

    def calculate_mean_revenue(self):
        self.mean_revenue = self.opulentpoint_saledata_df.groupBy("Company") \
            .agg(avg("Volume Sales MSU").alias("average_Volume_sales_MSU"))

    def transform_data(self):
        day_split = 10

        @udf(returnType=DoubleType())
        def calculate_transformed_mlc(value_sales_convert_to_mlc):
            return (value_sales_convert_to_mlc * 7 * day_split) / 1000

        self.refined_data_df = self.opulentpoint_saledata_df.withColumn(
            "transformed_mlc",
            calculate_transformed_mlc(col("Value Sales MLC (in USD)"))
        )

    def combine_data(self):
        self.combined_refined_data_df = self.opulentpoint_saledata_df.select(col("Company").alias("Company_pos")) \
            .join(self.trade_odyssey_records_df.select(col("Company").alias("Company_trade")))

    def process_data(self, pos_path, trade_path, output_path):
        self.read_data(pos_path, trade_path)
        self.join_data()
        self.write_data(output_path)
        self.filter_data()
        self.calculate_mean_revenue()
        self.transform_data()
        self.combine_data()

    def display_results(self):
        print("Refined Data:")
        self.refined_data_df.show()
        
        print("\nCombined Data:")
        self.combined_refined_data_df.show(5)
        
        print("\nMean Revenue:")
        self.mean_revenue.show()

if __name__ == "__main__":
    p_and_g_analytics = PGDataAnalyticsReckoner()
    p_and_g_analytics.process_data('/content/pos.csv', '/content/pos2.csv', '/content/pos_csv.csv')
    p_and_g_analytics.display_results()
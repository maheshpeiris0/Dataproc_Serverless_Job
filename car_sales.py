# import libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def create_parquet_files():
    # create spark session
    spark = SparkSession.builder.appName('myapp').getOrCreate()
    input_file = 'gs://input_bucket/car_sales_data.csv'   
    output_path = 'gs://output_bucket/car_sales/'
    # read csv file
    df = spark.read.csv(input_file, header=True, inferSchema=True)
    # filter data
    dataset = df.filter(col('Commission Rate') > 0.1)
    # get unique car makes
    unique_car_makes = dataset.select('Car Make').distinct().rdd.flatMap(lambda x: x).collect()
    # create dataframes for each car make
    filtered_dataframes = {}
    for value in unique_car_makes:
        filtered_dataframes[value]=dataset.filter(col('Car Make') == value)
    # write dataframes to parquet files
    for value, f_df in filtered_dataframes.items():
        f_df.repartition(1).write.parquet(f'{output_path}{value}.parquet', mode='overwrite')

if __name__ == '__main__':
    create_parquet_files()
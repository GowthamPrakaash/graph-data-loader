from pyspark.sql import SparkSession

from source2landing.Loader import Source2LandingLoader

output_dir = "/Users/gganesa7/PycharmProjects/graph-data-loader/output/"
landing_dir = output_dir + "landing/"


def main():
    spark = SparkSession.builder.appName('graph-data-loader').getOrCreate()
    Source2LandingLoader().load_data(spark, landing_dir)


if __name__ == "__main__":
    main()

from typing import Dict
from pyspark.sql import SparkSession, DataFrame

from Datasets import NAEB, NPASS


class Source2LandingLoader:
    datasets = [NAEB, NPASS]

    def load_data(self, spark: SparkSession, output_dir: str):
        for ds in self.datasets:
            dataset = ds()
            write_dir = output_dir + "/" + dataset.name
            data: Dict[str, DataFrame] = dataset.get_data(spark)
            for file, dataframe in data.items():
                dataframe.write.parquet(write_dir + "/" + file)

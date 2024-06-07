from typing import Dict

from pyspark.sql import SparkSession, DataFrame


class NAEB:
    name = "NAEB"
    source_path = "/Users/gganesa7/PycharmProjects/graph-data-loader/datasets/naeb/data/naeb_dump/"
    dataset = {
        "sources": source_path + "sources.csv",
        "species": source_path + "species.csv",
        "tribes": source_path + "tribes.csv",
        "use_categories": source_path + "use_categories.csv",
        "use_subcategories": source_path + "use_subcategories.csv",
        "uses": source_path + "uses.csv",
    }

    def get_data(self, spark: SparkSession) -> Dict[str, DataFrame]:
        return {file: spark.read.csv(path) for file, path in self.dataset}


class NPASS:
    name = "NPASS"
    source_path = "/Users/gganesa7/PycharmProjects/graph-data-loader/datasets/npass/"
    dataset = {
        "activities": source_path + "NPASSv2.0_download_naturalProducts_activities.txt",
        "generalInfo": source_path + "NPASSv2.0_download_naturalProducts_generalInfo.txt",
        "species_pair": source_path + "NPASSv2.0_download_naturalProducts_species_pair.txt",
        "speciesInfo": source_path + "NPASSv2.0_download_naturalProducts_speciesInfo.txt",
        "structureInfo": source_path + "NPASSv2.0_download_naturalProducts_structureInfo.txt",
        "targetInfo": source_path + "NPASSv2.0_download_naturalProducts_targetInfo.txt",
    }

    def get_data(self, spark: SparkSession) -> Dict[str, DataFrame]:
        return {file: spark.read.csv(path, sep='\t') for file, path in self.dataset}

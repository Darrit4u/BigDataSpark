import os
from dataclasses import dataclass

from pyspark.sql import SparkSession


def env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value is not None and value != "" else default


@dataclass
class SparkJob:
    app_name: str
    jdbc_jar_path: str | None = None
    jdbc_jars: list[str] | None = None
    log_level: str = "ERROR"

    def start(self) -> SparkSession:
        builder = SparkSession.builder.appName(self.app_name)

        jar_candidates: list[str] = []
        if self.jdbc_jar_path:
            jar_candidates.append(self.jdbc_jar_path)
        if self.jdbc_jars:
            jar_candidates.extend(self.jdbc_jars)

        jar_list = list(dict.fromkeys(j for j in jar_candidates if j))
        if jar_list:
            builder = (
                builder.config("spark.jars", ",".join(jar_list))
                .config("spark.driver.extraClassPath", os.pathsep.join(jar_list))
                .config("spark.executor.extraClassPath", os.pathsep.join(jar_list))
            )

        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel(self.log_level)
        return spark

    @staticmethod
    def stop(spark: SparkSession | None) -> None:
        if spark is not None:
            spark.stop()

import argparse
from typing import Dict, Any

import yaml


class Utils(object):
    @staticmethod
    def read_cfg(config_path: str) -> Dict[str, Any]:
        with open(config_path) as f:
            return yaml.safe_load(f)

    @staticmethod
    def set_local_env(spark, aws_access_key: str, aws_secret_access_key: str) -> None:
        hadoop_conf = spark._jsc.hadoopConfiguration()
        hadoop_conf.set(
            "fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem", 'spark.jars.packages')
        hadoop_conf.set("fs.s3a.access.key", aws_access_key)
        hadoop_conf.set("fs.s3a.secret.key", aws_secret_access_key)

    @staticmethod
    def pyspark_arg_parser() -> argparse.Namespace:
        parser: argparse.ArgumentParser = \
            argparse.ArgumentParser(description="pyspark emr argument")
        parser.add_argument('job_module')
        parser.add_argument('job_class')
        parser.add_argument('--env')
        parser.add_argument('--config_path')
        parser.add_argument('--input_path')
        parser.add_argument('--output_path')
        return parser.parse_args()

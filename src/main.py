import os
import time
import importlib
from typing import Dict

from pyspark import SparkContext
from pyspark.sql.session import SparkSession

from src.utils import Utils


class SparkJobManager(object):
    """設定のjobの名称から動的にjobクラス取得し、実行するクラス
    """
    @classmethod
    def run(cls, sc: SparkContext, args: Dict) -> None:
        job_module = importlib.import_module(f'jobs.{args.job_module}')
        job_class = getattr(job_module, args.job_class)
        job_class(sc, args).execute()


if __name__ == "__main__":
    sc: SparkContext = SparkSession.builder.appName("pyspark-emr-app").getOrCreate()
    sc.conf.set("spark.sql.debug.maxToStringFields", 100)
    # TODO 各jobに対応した専用のArgsClassを定義すると安全になる??
    args = Utils.pyspark_arg_parser()

    if args.env == "local":
        from dotenv import load_dotenv
        load_dotenv('../.env')
        Utils.set_local_env(
            sc,
            os.environ.get("AWS_ACCESS_ID"),
            os.environ.get("AWS_ACCESS_SECRET_KEY")
        )
    
    start = time.time()
    SparkJobManager.run(sc, args)
    end = time.time()
    print(f"\nExecution of job {args.job_module} took {end - start} seconds")

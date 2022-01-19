from typing import List, Dict, Any

from pyspark import SparkContext
from pyspark.sql import functions as F

from src.utils import Utils
from src.json_utils import JsonUtils
from src.jobs.job_cmn import JobExecutor


class ParseJsonStringAndFlatten(JobExecutor):
    def __init__(self, sc: SparkContext, args: Dict[str, Any]):
        self.sc: SparkContext = sc
        self.args: Dict[str, Any] = args

    def execute(self):
        df = self.sc.read.option("multiline", "true").json(self.args.input_path)
        df_str2json = df.withColumn(
            "json_string_root", 
            F.from_json(
                F.col("json_string_root"),
                JsonUtils.get_json_string_schema(self.sc, df, "json_string_root")
            )
        )

        cols_map = Utils.read_cfg(self.args.config_path)["flatten_cols_map"]
        exsist_cols: List[str] = JsonUtils.filter_exist_cols(cols_map)
        df_filtered = JsonUtils.flatten_df(
            df_str2json,
            {k: v for k, v in cols_map.items() if k in exsist_cols}
        )

        df_filtered = df_filtered.withColumn("year", F.split(F.col("datetime"), "\\-").getItem(0)) \
                                 .withColumn("month", F.split(F.col("datetime"), "\\-").getItem(1))
        df_filtered = df_filtered.repartition("id", "date") \
                                 .sortWithinPartitions(["datetime", "id", "date"]) \
                                 .dropDuplicates(subset=['id', 'date'])

        df_filtered.repartition("id", "date") \
                   .write \
                   .format('json') \
                   .mode("append") \
                   .partitionBy(["year", "month"]) \
                   .option('header', 'true') \
                   .save(self.args.output_path)

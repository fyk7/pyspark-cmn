from typing import List, Dict

import pyspark


# TODO Arrayを処理する関数の追加
class JsonUtils(object):

    @staticmethod
    def get_json_string_schema(
        sc: pyspark.SparkConf, df: pyspark.sql.DataFrame, root_col_name: str
    ) -> pyspark.sql.DataFrame.schema:
        return sc.read.json(
            df.rdd.map(lambda row: getattr(row, root_col_name))
        ).schema

    @staticmethod
    def filter_exist_cols(
        df: pyspark.sql.DataFrame, before_cols: List[str]
    ) -> List[str]:
        return [
            k for k in before_cols
            # simpleString()の戻り値に、存在しているcolumns名が含まれている
            if f"{k.split('.')[-1]}:" in df.schema.simpleString()
        ]
        
    @staticmethod
    def flatten_df(df, cols_map: Dict[str, str]) -> pyspark.sql.DataFrame:
        from pyspark.sql import fuctions as F
        select_cols_stms_literal = \
            (", ").join(
                [f'F.col("{k}").alias("{v}")' for k, v in cols_map.items()]
            )
        return df.select(list(eval(select_cols_stms_literal)))

/spark/bin/spark-submit \
--packages org.apache.hadoop:hadoop-aws:3.2.0 \
--conf "spark.driver.extraClassPath=/location/to/aws-java-sdk.jar" \
--conf "spark.driver.extraClassPath=/location/to/hadoop-aws.jar" \
--files dist/local.yml \
--py-files dist/jobs.zip,dist/libs.zip \
dist/main.py \
parse_json_string_and_flatten \
ParseJsonStringAndFlatten \
--config_path "dist/local.yml" \
--env local \
--input_path "s3a://<bucket_name>/<key_prefix>/"
--output_path "s3a://<bucket_name>/<key_prefix>/"


# Upload local dist/* to s3
aws s3 sync ./dist s3://<bucket_name>/<key_prefix>/dist --profile=<your_aws_profile>
# On EMR
aws s3 sync s3://<bucket_name>/<key_prefix>/dist ./dist

spark-submit \
--master yarn \
--files dist/local.yml \
--py-files dist/jobs.zip,dist/libs.zip \
dist/main.py \
parse_json_string_and_flatten \
ParseJsonStringAndFlatten \
--config_path "dist/local.yml"
--input_path "s3a://<bucket_name>/<key_prefix>/"
--output_path "s3a://<bucket_name>/<key_prefix>/"

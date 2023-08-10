import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# SparkSession 생성
spark = SparkSession \
    .builder \
    .appName("AWS Glue - PySpark code for processing CSV file") \
    .getOrCreate()

# 1. S3에서 searchTrend1 CSV 파일 읽기
input_path = "s3://asc-seoultech-bucket/searchTrend1.csv"
data = spark.read.csv(input_path, header=True, inferSchema=True)

# 2. 결측치를 포함한 열 제거
data_no_missing = data.dropna()

# 3. CTPRVN_NM 컬럼이 '강원도' 이고 SIGNGU_NM 컬럼이 '영월군' 인 열을 추출 및 저장
new_asc = data_no_missing.filter((col('CTPRVN_NM') == '강원도') & (col('SIGNGU_NM') == '영월군'))

# 결과 확인 및 저장
new_asc.show()

# 저장 경로 설정 후 저장 
output_path = "s3://asc-seoultech-bucket/output"
new_asc.write.csv(output_path, mode="overwrite", header=True)

# SparkSession 종료
spark.stop()


job.commit()

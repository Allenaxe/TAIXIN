import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg as _avg, count as _count, when, row_number, lit, when
from pyspark.sql.window import Window

# 初始化
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Step 1. 讀取四個 Table
df1 = glueContext.create_dynamic_frame.from_catalog(database="metadata", table_name="_train_accts_data_202412_csv").toDF()
df2 = glueContext.create_dynamic_frame.from_catalog(database="metadata", table_name="_train_eccus_data_202412_csv").toDF()
df3 = glueContext.create_dynamic_frame.from_catalog(database="metadata", table_name="_train_id_data_202412_csv").toDF()
df4 = glueContext.create_dynamic_frame.from_catalog(database="metadata", table_name="_train_sav_txn_data_202412_csv").toDF()

# Step 3. 補空值（選擇性
for field in df1.schema.fields:
    if field.dataType.typeName() in ['integer', 'long', 'double', 'float']:
        df1 = df1.withColumn(field.name, when(col(field.name).isNull(), lit(0)).otherwise(col(field.name)))
    else:
        df1 = df1.withColumn(field.name, when(col(field.name).isNull(), lit('')).otherwise(col(field.name)))
        
for field in df2.schema.fields:
    if field.dataType.typeName() in ['integer', 'long', 'double', 'float'] and field.name != "data_dt":
        df2 = df2.withColumn(field.name, when(col(field.name).isNull(), lit(0)).otherwise(col(field.name)))
    else:
        df2 = df2.withColumn(field.name, when(col(field.name).isNull(), lit('')).otherwise(col(field.name)))
        
for field in df3.schema.fields:
    if field.dataType.typeName() in ['integer', 'long', 'double', 'float']:
        df3 = df3.withColumn(field.name, when(col(field.name).isNull(), lit(0)).otherwise(col(field.name)))
    else:
        df3 = df3.withColumn(field.name, when(col(field.name).isNull(), lit('')).otherwise(col(field.name)))

for field in df4.schema.fields:
    if field.dataType.typeName() in ['integer', 'long', 'double', 'float']:
        df4 = df4.withColumn(field.name, when(col(field.name).isNull(), lit(0)).otherwise(col(field.name)))
    else:
        df4 = df4.withColumn(field.name, when(col(field.name).isNull(), lit('')).otherwise(col(field.name)))

# 合併 df1, df2, df3
df = (
    df1.join(df2, ["ACCT_NBR","CUST_ID"], "outer")
      .join(df3, ["CUST_ID"], "outer")
)

# df4 群組計算
df4_group = df4.groupBy("ACCT_NBR","CUST_ID").agg(
    _count("*").alias("TXN_COUNT"),
    _sum("TX_AMT").alias("TXN_AMT_SUM"),
    _avg("TX_AMT").alias("TXN_AMT_MEAN"),
    _avg("eb_check").alias("EB_CHECK_SUM"),
    _avg("mb_check").alias("MB_CHECK_SUM"),
    (_sum("SAME_NUMBER_IP")/_count("*")).alias("IP_RATE"),
    (_sum("SAME_NUMBER_UUID")/_count("*")).alias("UUID_RATE")
)
df = df.join(df4_group, ["ACCT_NBR","CUST_ID"], "outer")

# 虛擬交易 ID99999 次數
virtual_id = (
    df4.filter(col("OWN_TRANS_ID")=="ID99999")
      .groupBy("ACCT_NBR","CUST_ID")
      .agg(_count("*").alias("VIRTUAL_ID"))
)
df = df.join(virtual_id, ["ACCT_NBR","CUST_ID"], "outer")

# DRCR 出 & 入
drcr_out = (
    df4.filter(col("DRCR")==1)
      .groupBy("ACCT_NBR","CUST_ID")
      .agg(_count("*").alias("DRCR_OUT"))
)
drcr_in = (
    df4.filter(col("DRCR")==2)
      .groupBy("ACCT_NBR","CUST_ID")
      .agg(_count("*").alias("DRCR_IN"))
)
df = df.join(drcr_out, ["ACCT_NBR","CUST_ID"], "outer")
df = df.join(drcr_in, ["ACCT_NBR","CUST_ID"], "outer")

# 清晨交易 (TX_TIME 0-6)
df4_time = df4.withColumn("TX_TIME_NUM", col("TX_TIME").cast("double"))
time_count = (
    df4_time.filter((col("TX_TIME_NUM")>=0) & (col("TX_TIME_NUM")<=6))
            .groupBy("ACCT_NBR","CUST_ID")
            .agg(_count("*").alias("TX_TIME"))
)
df = df.join(time_count, ["ACCT_NBR","CUST_ID"], "outer")

# 最頻繁 CHANNEL_CODE
channel_counts = (
    df4.groupBy("ACCT_NBR","CUST_ID","CHANNEL_CODE")
      .agg(_count("*").alias("TX_COUNT"))
)
window_ch = Window.partitionBy("ACCT_NBR","CUST_ID").orderBy(col("TX_COUNT").desc())
max_channel = (
    channel_counts.withColumn("rank", row_number().over(window_ch))
                  .filter(col("rank")==1)
                  .drop("TX_COUNT","rank")
)
df = df.join(max_channel, ["ACCT_NBR","CUST_ID"], "outer")

# 最頻繁 TRN_CODE
trn_counts = (
    df4.groupBy("ACCT_NBR","CUST_ID","TRN_CODE")
      .agg(_count("*").alias("TX_COUNT"))
)
window_tr = Window.partitionBy("ACCT_NBR","CUST_ID").orderBy(col("TX_COUNT").desc())
max_trn = (
    trn_counts.withColumn("rank", row_number().over(window_tr))
              .filter(col("rank")==1)
              .drop("TX_COUNT","rank")
)
df = df.join(max_trn, ["ACCT_NBR","CUST_ID"], "outer")


df = df.withColumn(
    "y",
    when(col("data_dt") != "", lit(1)).otherwise(lit(0))
)

df = df.drop("data_dt")

# Step 4. 合併分區 & 輸出為 Parquet 到 S3
single_df = df.coalesce(1)
final_dyf = DynamicFrame.fromDF(single_df, glueContext, "final_dyf")

glueContext.write_dynamic_frame.from_options(
    frame=final_dyf,
    connection_type="s3",
    connection_options={"path": "s3://taishindata/clean data/"},
    format="csv"
)

job.commit()
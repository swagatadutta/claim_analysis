from pyspark.sql import SparkSession
from helper_functions import *

spark = SparkSession.builder.getOrCreate()

# READING RAW LAYER
claims_df = spark.read.csv("raw_data/sample_claims.csv", header=True, inferSchema=True)
logs_df   = spark.read.csv("raw_data/sample_claim_logs.csv", header=True, inferSchema=True)
payouts_df= spark.read.csv("raw_data/sample_claim_payouts.csv", header=True, inferSchema=True)

claims_df.createOrReplaceTempView("raw_claims")
logs_df.createOrReplaceTempView("raw_claim_logs")
payouts_df.createOrReplaceTempView("raw_claim_payouts")

sql_execution_order = [
    
    # ENHANCED RAW FACTS
    "fact_claim.sql",
    "fact_claim_status.sql",
    "fact_claim_payout.sql",
    
    # STEP AGGREGATION
    "agg_logs.sql",
    "agg_payout.sql",
    
    # INTERMEDIATE CLAIM MASTER TABLE
    "int_claim_master.sql",
    
    # KPIS
    "kpi_step_timing.sql",
    "kpi_workflow_efficiency.sql", 
    "kpi_bottleneck_analysis.sql",
    "kpi_friction_correlation.sql",
    "kpi_summary_dashboard.sql",
]

# EXECUTE SQLS
for sql_file in sql_execution_order:
    print(f"processing {sql_file}")
    spark.sql(read_sql_file(f"sql/{sql_file}"))
    print(f"✅ {sql_file}")

views_df = spark.sql("SHOW TABLES").filter("isTemporary = true")
views = [row.tableName for row in views_df.collect()]

# WRITE SPARK OUTPUT CSVs
clear_directory('spark_output') #housekeeping task

for view_name in sorted(views):
    df = spark.table(view_name)
    csv_path = f"spark_output/{view_name}.csv"
    df.coalesce(1) \
        .write.mode("overwrite") \
        .option("header", "true") \
        .csv(csv_path)

# WRITE CLEAN OUTPUT CSVs IN "final_output" DIR
clear_directory('final_output') #housekeeping_task
save_spark_csv_to_output_csv('spark_output', 'final_output')

# SAMPLE CLAIM ID = faa5f524f46ca8fad25181a5ab4afc1b5cae77c478f279aa92079367093bfa56
# SAMPLE CLAIM REF = fa5b517753e393c3fd72493d1587b8ca8de914206281a237946ad0c18fb8b4a9



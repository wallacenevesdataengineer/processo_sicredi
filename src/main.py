from pyspark.sql import SparkSession
from ingestion.bronze import run_pipeline_bronze
from transformations.silver import run_pipeline_silver
from transformations.gold import run_pipeline_gold

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Pipeline Sicredi Modular").getOrCreate()

    # Bronze: ingestão bruta com metadados técnicos
    run_pipeline_bronze(spark)
    # Silver: processamento, validação e deduplicação
    run_pipeline_silver(spark)
    # Gold: métricas e modelo dimensional
    run_pipeline_gold(spark)

    print("Pipeline completa executada com sucesso.")

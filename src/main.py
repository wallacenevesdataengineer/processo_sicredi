from pyspark.sql import SparkSession
# from ingestion.bronze import processa_bronze_generic
from transformations.silver import run_pipeline_silver
# from transformations.gold import run_pipeline_gold
# from config import BRONZE_SCHEMA, SILVER_SCHEMA, GOLD_SCHEMA, CLIENTES_RAW_PATH

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Pipeline Sicredi Modular").getOrCreate()

    # Bronze: ingestão
    # processa_bronze_generic(
    #     spark, CLIENTES_RAW_PATH, BRONZE_SCHEMA, "clientes",
    #     "clientes.csv", "2023-10-01"
    # )
    # Silver: processamento
    run_pipeline_silver(spark)
    # Gold: métricas
    # run_pipeline_gold(spark, SILVER_SCHEMA, GOLD_SCHEMA)

    print("Pipeline completa executada com sucesso.")

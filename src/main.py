from pyspark.sql import SparkSession
from ingestion.bronze import processa_bronze_clientes
from transformations.silver import processa_clientes, processa_contas, processa_cartoes, processa_transacoes, processa_estornos
from transformations.gold import calcula_metricas_gold
from config import BRONZE_SCHEMA, SILVER_SCHEMA, GOLD_SCHEMA, CLIENTES_RAW_PATH

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Pipeline Sicredi Modular").getOrCreate()

    # Bronze: ingestão
    processa_bronze_clientes(spark, CLIENTES_RAW_PATH, BRONZE_SCHEMA)
    # Silver: processamento
    processa_clientes(spark)
    processa_contas(spark)
    processa_cartoes(spark)
    processa_transacoes(spark)
    processa_estornos(spark)
    # Gold: métricas
    calcula_metricas_gold(spark, SILVER_SCHEMA, GOLD_SCHEMA)

    print("Pipeline completa executada com sucesso.")

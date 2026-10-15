from pyspark.sql import functions as F

def processa_bronze_clientes(spark, raw_data_path, bronze_schema):
    """
    Lê dados brutos (CSV/JSON/parquet/etc), padroniza nomes, tipos
    e salva na camada bronze.
    """
    df_raw = spark.read.format("csv").option("header", True).load(raw_data_path)
    df_bronze = (
        df_raw
        .withColumnRenamed("IdCliente", "id_cliente")
        .withColumn("id_cliente", F.col("id_cliente").cast("long"))
        # ...demais casts ou outros formatos...
    )
    df_bronze.write.mode("overwrite").saveAsTable(f"{bronze_schema}.clientes")
    return df_bronze

# Crie funções similares para contas, cartoes, transacoes, estornos conforme necessário.

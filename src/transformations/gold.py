from pyspark.sql import functions as F

def calcula_metricas_gold(spark, silver_schema, gold_schema):
    """
    Gera métricas/agregações para indicadores de negócio na Gold.
    """
    trans = spark.table(f"{silver_schema}.transacoes_cartao")
    indicadores = (
        trans
        .groupBy("id_cartao")
        .agg(
            F.count("*").alias("qtd_transacoes"),
            F.sum("valor").alias("valor_total"),
            F.avg("valor").alias("valor_medio")
        )
    )
    indicadores.write.mode("overwrite").saveAsTable(f"{gold_schema}.indicadores_cartao")
    return indicadores

# Adicione funções similares para outras métricas e indicadores necessários no projeto.

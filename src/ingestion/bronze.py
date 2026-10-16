from pyspark.sql import functions as F

def adiciona_metadados_tecnicos(df, arquivo_origem, data_referencia):
    timestamp_ingestao = F.current_timestamp()
    hash_linha = F.sha2(F.concat_ws("|", *df.columns), 256)
    return (
        df
        .withColumn("arquivo_origem", F.lit(arquivo_origem))
        .withColumn("data_referencia", F.lit(data_referencia))
        .withColumn("timestamp_ingestao", timestamp_ingestao)
        .withColumn("hash_linha", hash_linha)
    )

def processa_bronze_generic(spark, raw_data_path, bronze_schema, tabela_nome, arquivo_origem, data_referencia, options=None):
    """
    Função genérica para ingestão bronze de qualquer tabela.
    Ingeri múltiplos arquivos, persiste em Delta, sem transformar negócio.
    Adiciona metadados técnicos.
    """
    opts = options or {}
    df_raw = spark.read.format(opts.get("format", "csv")).options(**opts.get("read_options", {"header": True})).load(raw_data_path)
    df_bronze = adiciona_metadados_tecnicos(df_raw, arquivo_origem, data_referencia)
    df_bronze.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{bronze_schema}.{tabela_nome}")
    return df_bronze

# Exemplo de chamada:
# processa_bronze_generic(spark, "/Volumes/transacoes/contas/20231001/*.csv", "bronze", "contas", "contas_20231001.csv", "2023-10-01")
# Repita para cartoes, transacoes, estornos, clientes. Múltiplos arquivos/datas podem ser ingeridos em loop.

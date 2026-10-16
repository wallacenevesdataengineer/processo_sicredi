from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# ============================================================
# Configurações da camada Bronze
# ============================================================

CATALOG = "workspace"
BRONZE_SCHEMA = f"{CATALOG}.bronze"
VOLUME_PATH = "/Volumes/workspace/default/transacoes/"

# Configuração das tabelas bronze: (tabela, arquivo_csv, data_referencia)
TABELAS_CONFIG = [
    {"tabela": "cartoes", "arquivo": "cartoes.csv", "data_referencia": "2025-01-01"},
    {"tabela": "clientes", "arquivo": "clientes.csv", "data_referencia": "2025-01-01"},
    {"tabela": "contas", "arquivo": "contas.csv", "data_referencia": "2025-01-01"},
    {"tabela": "estornos", "arquivo": "estornos.csv", "data_referencia": "2025-01-01"},
    {"tabela": "transacoes_cartao", "arquivo": "transacoes_cartao_20250101.csv", "data_referencia": "2025-01-01"},
]

# Colunas de ID que devem ser convertidas para long
COLUNAS_ID = ["id_cartao", "id_conta", "id_cliente", "id_transacao", "id_estorno"]


# ============================================================
# Funções auxiliares
# ============================================================

def adiciona_metadados_tecnicos(df, arquivo_origem, data_referencia):
    """
    Adiciona colunas técnicas de rastreabilidade ao DataFrame bronze:
    arquivo_origem, data_referencia, timestamp_ingestao e hash_linha.
    """
    colunas_originais = df.columns  # única chamada de Analyze RPC
    colunas_para_hash = [c for c in colunas_originais if c != "_metadata"]
    return (
        df
        .withColumn("arquivo_origem", F.lit(arquivo_origem))
        .withColumn("data_referencia", F.lit(data_referencia))
        .withColumn("timestamp_ingestao", F.current_timestamp())
        .withColumn("hash_linha", F.sha2(F.concat_ws("|", *colunas_para_hash), 256))
    )


def _cast_ids_to_long(df):
    """Converte colunas de ID conhecidas para long usando withColumns (Spark Connect otimizado)."""
    colunas_atuais = set(df.columns)  # única chamada de Analyze RPC
    casts = {col_id: F.col(col_id).cast("long") for col_id in COLUNAS_ID if col_id in colunas_atuais}
    if casts:
        df = df.withColumns(casts)
    return df


# ============================================================
# Função principal de ingestão bronze
# ============================================================

def processa_bronze_generic(spark, raw_data_path, bronze_schema, tabela_nome, arquivo_origem, data_referencia, recreate=False):
    """
    Função genérica para ingestão bronze de qualquer tabela CSV.

    - Lê o arquivo CSV com header
    - Converte colunas de ID para long
    - Adiciona metadados técnicos (arquivo_origem, data_referencia, timestamp_ingestao, hash_linha)
    - Se recreate=True e a tabela existe: dropa e recria (para mudanças de schema)
    - Se a tabela já existe: faz dedup por hash_linha mantendo o timestamp mais recente
    - Se a tabela não existe: cria com mergeSchema

    Retorna o DataFrame bronze resultante.
    """
    destino = f"{bronze_schema}.{tabela_nome}"

    df_raw = spark.read.option("header", True).csv(raw_data_path)
    df_raw = _cast_ids_to_long(df_raw)
    df_bronze = adiciona_metadados_tecnicos(df_raw, arquivo_origem, data_referencia)

    if recreate and spark.catalog.tableExists(destino):
        spark.sql(f"DROP TABLE IF EXISTS {destino}")
        print(f"Tabela removida para recriação: {destino}")

    if spark.catalog.tableExists(destino):
        # Deduplicação: mantém apenas registros únicos por hash_linha (timestamp mais recente)
        dt = DeltaTable.forName(spark, destino)
        bronze_atual = dt.toDF()

        colunas_atuais = list(bronze_atual.columns)
        colunas_novas = list(df_bronze.columns)
        todas_colunas = list(set(colunas_atuais) | set(colunas_novas))

        bronze_atual_alinhado = bronze_atual.select(
            *[F.col(c) if c in colunas_atuais else F.lit(None).alias(c) for c in todas_colunas]
        )
        bronze_novo_alinhado = df_bronze.select(
            *[F.col(c) if c in colunas_novas else F.lit(None).alias(c) for c in todas_colunas]
        )
        union_df = bronze_atual_alinhado.unionByName(bronze_novo_alinhado)

        w = Window.partitionBy("hash_linha").orderBy(F.col("timestamp_ingestao").desc())
        bronze_dedup = (
            union_df
            .withColumn("rn", F.row_number().over(w))
            .filter("rn = 1")
            .drop("rn")
        )
        bronze_dedup.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(destino)
        count_dedup = bronze_dedup.count()
        print(f"Reprocessamento (dedup): {arquivo_origem} -> {destino} ({count_dedup} unicos)")
        return bronze_dedup
    else:
        df_bronze.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(destino)
        count_ini = df_bronze.count()
        print(f"Carga inicial: {arquivo_origem} -> {destino} ({count_ini} registros)")
        return df_bronze


# ============================================================
# Orquestração do pipeline Bronze
# ============================================================

def run_pipeline_bronze(spark, recreate=False):
    """
    Executa o pipeline completo da camada Bronze para todas as tabelas configuradas.
    Segue o mesmo padrao de orquestracao usado em run_pipeline_silver e run_pipeline_gold.
    Use recreate=True quando houver mudanca de schema (ex: tipos de colunas alterados).
    """
    for cfg in TABELAS_CONFIG:
        caminho = f"{VOLUME_PATH}{cfg['arquivo']}"
        processa_bronze_generic(
            spark,
            caminho,
            BRONZE_SCHEMA,
            cfg["tabela"],
            cfg["arquivo"],
            cfg["data_referencia"],
            recreate=recreate
        )
    print("Camada Bronze concluida.")


# ============================================================
# Criação alternativa via SQL (read_files, sem metadados)
# ============================================================

def criar_tabelas_sql(spark):
    """
    Cria tabelas bronze via SQL CREATE TABLE AS SELECT * FROM read_files.
    Alternativa sem metadados técnicos — útil para carga rápida ou conferência.
    """
    sqls = {
        "cartoes": "'/Volumes/workspace/default/transacoes/cartoes.csv'",
        "clientes": "'/Volumes/workspace/default/transacoes/clientes.csv'",
        "contas": "'/Volumes/workspace/default/transacoes/contas.csv'",
        "estornos": "'/Volumes/workspace/default/transacoes/estornos.csv'",
        "transacoes_cartao": "'/Volumes/workspace/default/transacoes/transacoes_cartao_20250101.csv'",
    }
    for tabela, path in sqls.items():
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {BRONZE_SCHEMA}.{tabela}
            USING DELTA
            AS SELECT * FROM read_files(
                {path},
                format => 'csv',
                header => true
            )
        """)
        cnt = spark.table(f"{BRONZE_SCHEMA}.{tabela}").count()
        print(f"SQL: {BRONZE_SCHEMA}.{tabela} criada com {cnt} registros.")


# ============================================================
# Funções de verificação
# ============================================================

def verificar_arquivos_csv(spark):
    """
    Verifica o conteúdo dos arquivos CSV no volume, exibindo os primeiros registros e total de linhas.
    Usa Spark para leitura (Spark Connect compatível).
    """
    for cfg in TABELAS_CONFIG:
        caminho = f"{VOLUME_PATH}{cfg['arquivo']}"
        df = spark.read.option("header", True).csv(caminho)
        total = df.count()
        print(f"\nArquivo: {cfg['arquivo']} — {total} linhas")
        df.show(5, truncate=False)


def verificar_tabelas_bronze(spark):
    """
    Verifica se as tabelas bronze foram criadas e exibe a contagem de registros.
    """
    for cfg in TABELAS_CONFIG:
        tabela = f"{BRONZE_SCHEMA}.{cfg['tabela']}"
        try:
            cnt = spark.table(tabela).count()
            print(f"Tabela {tabela}: {cnt} registros.")
        except Exception as e:
            print(f"Erro ao acessar {tabela}: {e}")


# ============================================================
# Execução standalone
# ============================================================

if __name__ == "__main__":
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    run_pipeline_bronze(spark, recreate=True)
    verificar_tabelas_bronze(spark)

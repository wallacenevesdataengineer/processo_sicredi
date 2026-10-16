from pyspark.sql import functions as F, Window
import uuid


CATALOG = "workspace"
BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver"

# Função para salvar registros inválidos/quarentena

def salvar_quarentena(df_avaliado, colunas, tabela):
    quarentena = df_avaliado.filter(F.col("motivo_erro") != "").select(
        *[F.col(c).cast("string").alias(c) for c in colunas],
        F.col("motivo_erro"),
        F.current_timestamp().alias("timestamp_processamento")
    )
    quarentena.write.mode("overwrite").saveAsTable(tabela)

# ============================================================
# 1. CLIENTES — tipagem, validação, quarentena e dedup
# ============================================================

def processa_clientes(spark):
    COLUNAS_CLIENTES = ["id_cliente", "nome", "cpf", "data_nascimento", "cidade", "estado", "data_atualizacao"]
    clientes_tipado = spark.table(f"{BRONZE}.clientes").select(
        F.col("id_cliente").cast("long").alias("id_cliente"),
        F.col("nome").cast("string").alias("nome"),
        F.col("cpf").cast("string").alias("cpf"),
        F.expr("try_cast(data_nascimento as date)").alias("data_nascimento"),
        F.col("cidade").cast("string").alias("cidade"),
        F.col("estado").cast("string").alias("estado"),
        F.expr("try_cast(data_atualizacao as date)").alias("data_atualizacao")
    )
    # Deduplicação: clientes mais recentes
    window = Window.partitionBy("id_cliente").orderBy(F.col("data_atualizacao").desc())
    clientes_unicos = clientes_tipado.withColumn("row_num", F.row_number().over(window)).filter(F.col("row_num") == 1).drop("row_num")
    clientes_unicos = clientes_unicos.withColumns({
        "ano_mes": F.date_format(F.col("data_atualizacao"), "yyyy-MM"),
        "timestamp_processamento": F.current_timestamp()
    })
    # Sempre cria/força o particionamento na primeira execução
    clientes_unicos.write.format("delta") \
        .option("overwriteSchema", "true") \
        .mode("overwrite") \
        .partitionBy("ano_mes") \
        .saveAsTable(f"{SILVER}.clientes")
    spark.catalog.clearCache()
    # MERGE incremental
    temp_view = f"vw_clientes_staging_{str(uuid.uuid4())[:8]}"
    clientes_unicos.createOrReplaceTempView(temp_view)
    spark.sql(f"""
        MERGE INTO {SILVER}.clientes tgt
        USING {temp_view} src
        ON tgt.id_cliente = src.id_cliente
        WHEN MATCHED AND src.data_atualizacao > tgt.data_atualizacao THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    return clientes_unicos

# ============================================================
# 2. CONTAS — tipagem, validação, RI
# ============================================================

def processa_contas(spark):
    COLUNAS_CONTAS = ["id_conta", "id_cliente", "tipo_conta", "status_conta", "data_abertura"]
    contas_tipado = spark.table(f"{BRONZE}.contas").select(
        F.col("id_conta").cast("long").alias("id_conta"),
        F.col("id_cliente").cast("long").alias("id_cliente"),
        F.col("tipo_conta").cast("string").alias("tipo_conta"),
        F.col("status_conta").cast("string").alias("status_conta"),
        F.expr("try_cast(data_abertura as date)").alias("data_abertura")
    )
    clientes_silver = spark.table(f"{SILVER}.clientes").select(F.col("id_cliente"), F.lit(True).alias("_ri"))
    contas_ri = contas_tipado.join(clientes_silver, "id_cliente", "left")
    window = Window.partitionBy("id_conta").orderBy(F.col("data_abertura").desc())
    contas_unicas = contas_ri.withColumn("row_num", F.row_number().over(window)).filter(F.col("row_num") == 1).drop("row_num")
    contas_validas = contas_unicas.filter(F.col("_ri") == True).drop("_ri")
    contas_validas = contas_validas.withColumns({
        "ano_mes": F.date_format(F.col("data_abertura"), "yyyy-MM"),
        "timestamp_processamento": F.current_timestamp()
    })
    # Sempre cria/força o particionamento na primeira execução
    contas_validas.write.format("delta") \
        .option("overwriteSchema", "true") \
        .mode("overwrite") \
        .partitionBy("ano_mes") \
        .saveAsTable(f"{SILVER}.contas")
    spark.catalog.clearCache()
    # MERGE incremental
    temp_view = f"vw_contas_staging_{str(uuid.uuid4())[:8]}"
    contas_validas.createOrReplaceTempView(temp_view)
    spark.sql(f"""
        MERGE INTO {SILVER}.contas tgt
        USING {temp_view} src
        ON tgt.id_conta = src.id_conta
        WHEN MATCHED AND src.data_abertura > tgt.data_abertura THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    return contas_validas

# ============================================================
# 3. CARTÕES — tipagem, validação, RI e versão mais recente
# ============================================================
def processa_cartoes(spark):
    COLUNAS_CARTOES = ["id_cartao", "id_conta", "bandeira", "tipo_cartao", "status_cartao", "data_emissao", "data_atualizacao"]
    cartoes_tipado = spark.table(f"{BRONZE}.cartoes").select(
        F.col("id_cartao").cast("long").alias("id_cartao"),
        F.col("id_conta").cast("long").alias("id_conta"),
        F.col("bandeira").cast("string").alias("bandeira"),
        F.col("tipo_cartao").cast("string").alias("tipo_cartao"),
        F.col("status_cartao").cast("string").alias("status_cartao"),
        F.expr("try_cast(data_emissao as date)").alias("data_emissao"),
        F.expr("try_cast(data_atualizacao as date)").alias("data_atualizacao"),
    )
    contas_silver = spark.table(f"{SILVER}.contas").select(F.col("id_conta"), F.lit(True).alias("_ri"))
    cartoes_ri = cartoes_tipado.join(contas_silver, "id_conta", "left")
    window = Window.partitionBy("id_cartao").orderBy(F.col("data_atualizacao").desc())
    cartoes_unicos = cartoes_ri.withColumn("row_num", F.row_number().over(window)).filter(F.col("row_num") == 1)
    cartoes_validos = cartoes_unicos.filter(F.col("_ri") == True).drop("_ri", "row_num")
    cartoes_validos = cartoes_validos.withColumns({
        "ano_mes": F.date_format(F.col("data_atualizacao"), "yyyy-MM"),
        "timestamp_processamento": F.current_timestamp()
    })
    # Sempre cria/força o particionamento na primeira execução
    cartoes_validos.write.format("delta") \
        .option("overwriteSchema", "true") \
        .mode("overwrite") \
        .partitionBy("ano_mes") \
        .saveAsTable(f"{SILVER}.cartoes")
    spark.catalog.clearCache()
    # MERGE incremental
    temp_view = f"vw_cartoes_staging_{str(uuid.uuid4())[:8]}"
    cartoes_validos.createOrReplaceTempView(temp_view)
    spark.sql(f"""
        MERGE INTO {SILVER}.cartoes tgt
        USING {temp_view} src
        ON tgt.id_cartao = src.id_cartao
        WHEN MATCHED AND src.data_atualizacao > tgt.data_atualizacao THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    return cartoes_validos

# ============================================================
# 4. TRANSAÇÕES — tipagem, validação, RI e deduplicação
# ============================================================
def processa_transacoes(spark):
    COLUNAS_TRANSACOES = ["id_transacao", "id_cartao", "data_transacao", "valor", "nome_estabelecimento", "mcc", "cidade_estabelecimento"]
    transacoes_tipado = spark.table(f"{BRONZE}.transacoes_cartao").select(
        F.col("id_transacao").cast("long").alias("id_transacao"),
        F.col("id_cartao").cast("long").alias("id_cartao"),
        F.expr("try_cast(data_transacao as date)").alias("data_transacao"),
        F.col("valor").cast("decimal(18,2)").alias("valor"),
        F.col("nome_estabelecimento").cast("string").alias("nome_estabelecimento"),
        F.col("mcc").cast("int").alias("mcc"),
        F.col("cidade_estabelecimento").cast("string").alias("cidade_estabelecimento"),
    )
    cartoes_silver = spark.table(f"{SILVER}.cartoes").select(F.col("id_cartao"), F.lit(True).alias("_ri"))
    transacoes_ri = transacoes_tipado.join(cartoes_silver, "id_cartao", "left")
    window = Window.partitionBy("id_transacao").orderBy(F.col("data_transacao").desc(), F.col("valor").desc())
    transacoes_unicas = transacoes_ri.withColumn("row_num", F.row_number().over(window)).filter(F.col("row_num") == 1)
    transacoes_validas = transacoes_unicas.filter(F.col("_ri") == True).drop("_ri", "row_num")
    transacoes_validas = transacoes_validas.withColumns({
        "ano_mes": F.date_format(F.col("data_transacao"), "yyyy-MM"),
        "timestamp_processamento": F.current_timestamp()
    })
    # Sempre cria/força o particionamento na primeira execução
    transacoes_validas.write.format("delta") \
        .option("overwriteSchema", "true") \
        .mode("overwrite") \
        .partitionBy("ano_mes") \
        .saveAsTable(f"{SILVER}.transacoes_cartao")
    spark.catalog.clearCache()
    # MERGE incremental
    temp_view = f"vw_transacoes_staging_{str(uuid.uuid4())[:8]}"
    transacoes_validas.createOrReplaceTempView(temp_view)
    spark.sql(f"""
        MERGE INTO {SILVER}.transacoes_cartao tgt
        USING {temp_view} src
        ON tgt.id_transacao = src.id_transacao
        WHEN MATCHED AND src.data_transacao > tgt.data_transacao THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    return transacoes_validas

# ============================================================
# 5. ESTORNOS + FLAG DE ESTORNO
# ============================================================
def processa_estornos(spark):
    COLUNAS_ESTORNOS = ["id_estorno", "id_transacao", "data_estorno", "motivo_estorno"]
    estornos_tipado = spark.table(f"{BRONZE}.estornos").select(
        F.col("id_estorno").cast("long").alias("id_estorno"),
        F.col("id_transacao").cast("long").alias("id_transacao"),
        F.expr("try_cast(data_estorno as date)").alias("data_estorno"),
        F.col("motivo_estorno").cast("string").alias("motivo_estorno"),
    )
    transacoes_silver = spark.table(f"{SILVER}.transacoes_cartao").select(F.col("id_transacao"), F.lit(True).alias("_ri"))
    estornos_ri = estornos_tipado.join(transacoes_silver, "id_transacao", "left")
    estornos_validos = estornos_ri.filter(F.col("_ri") == True).drop("_ri")
    estornos_validos = estornos_validos.withColumns({
        "ano_mes": F.date_format(F.col("data_estorno"), "yyyy-MM"),
        "timestamp_processamento": F.current_timestamp()
    })
    # Sempre cria/força o particionamento na primeira execução
    estornos_validos.write.format("delta") \
        .option("overwriteSchema", "true") \
        .mode("overwrite") \
        .partitionBy("ano_mes") \
        .saveAsTable(f"{SILVER}.estornos")
    spark.catalog.clearCache()
    # MERGE incremental
    temp_view = f"vw_estornos_staging_{str(uuid.uuid4())[:8]}"
    estornos_validos.createOrReplaceTempView(temp_view)
    spark.sql(f"""
        MERGE INTO {SILVER}.estornos tgt
        USING {temp_view} src
        ON tgt.id_estorno = src.id_estorno
        WHEN MATCHED AND src.data_estorno > tgt.data_estorno THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    return estornos_validos

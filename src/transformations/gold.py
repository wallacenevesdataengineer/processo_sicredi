from pyspark.sql import functions as F

def gold_transacoes_cartao(spark, silver_schema, gold_schema):
    """
    Cria visão analítica unindo cliente, conta, cartão, transação.
    """
    trans = spark.table(f"{silver_schema}.transacoes_cartao").select(
        "id_transacao", "id_cartao", "data_transacao", "valor",
        "nome_estabelecimento", "mcc", "cidade_estabelecimento"
    )
    cart = spark.table(f"{silver_schema}.cartoes").select(
        "id_cartao", "id_conta", "bandeira", "tipo_cartao", "status_cartao"
    )
    cont = spark.table(f"{silver_schema}.contas").select(
        "id_conta", "id_cliente", "tipo_conta", "status_conta"
    )
    cli = spark.table(f"{silver_schema}.clientes").select(
        "id_cliente", "nome", "cpf", "cidade", "estado"
    )
    joined = (
        trans
        .join(cart, "id_cartao", "left")
        .join(cont, "id_conta", "left")
        .join(cli, "id_cliente", "left")
    )
    joined.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{gold_schema}.transacoes_cartao")
    return joined

def gold_resumo_cliente_mes(spark, silver_schema, gold_schema):
    """
    Indicadores mensais por cliente.
    """
    trans = spark.table(f"{gold_schema}.transacoes_cartao")
    resumo = (
        trans.withColumn("mes", F.date_format("data_transacao", "yyyy-MM"))
          .groupBy("id_cliente", "mes")
          .agg(
              F.count("*").alias("transacoes_mes"),
              F.sum("valor").alias("valor_total_mes"),
              F.avg("valor").alias("valor_medio_mes")
          )
    )
    resumo.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{gold_schema}.resumo_cliente_mes")
    return resumo

def gold_ranking_estabelecimento(spark, gold_schema):
    """
    Ranking de estabelecimentos por valor e quantidade de transações.
    """
    trans = spark.table(f"{gold_schema}.transacoes_cartao")
    ranking = (
        trans.groupBy("nome_estabelecimento")
            .agg(
                F.count("*").alias("qtd_transacoes"),
                F.sum("valor").alias("valor_total")
            )
            .orderBy(F.desc("valor_total"), F.desc("qtd_transacoes"))
    )
    ranking.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{gold_schema}.ranking_estabelecimento")
    return ranking

def gold_alertas_qualidade(spark, silver_schema, gold_schema):
    """
    Resumo dos registros rejeitados por tipo de erro.
    """
    entidades = [
        "quarentena_clientes",
        "quarentena_contas",
        "quarentena_cartoes",
        "quarentena_transacoes_cartao",
        "quarentena_estornos"
    ]
    from functools import reduce
    dfs = []
    for ent in entidades:
        try:
            df = spark.table(f"{silver_schema}.{ent}")
            if "motivo_erro" in df.columns:
                alerta = df.groupBy("motivo_erro").agg(F.count("*").alias("ocorrencias")).withColumn("entidade", F.lit(ent))
                dfs.append(alerta)
        except Exception:
            continue
    if not dfs:
        return None
    unioned = reduce(lambda a,b: a.unionByName(b), dfs)
    unioned.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{gold_schema}.alertas_qualidade")
    return unioned

# ============================================================
# STAR SCHEMA — Dimensões
# ============================================================

def gold_dim_cliente(spark, silver_schema, gold_schema):
    """Cria dim_cliente a partir de silver.clientes_particionado."""
    spark.sql(f"""
    CREATE OR REPLACE TABLE {gold_schema}.dim_cliente
    USING DELTA
    PARTITIONED BY (ano_mes)
    AS SELECT
      ROW_NUMBER() OVER (ORDER BY id_cliente) AS sk_cliente,
      id_cliente,
      nome,
      cpf,
      data_nascimento,
      cidade,
      estado,
      data_atualizacao,
      ano_mes
    FROM {silver_schema}.clientes_particionado
    """)
    cnt = spark.table(f"{gold_schema}.dim_cliente").count()
    print(f"dim_cliente: {cnt} registros")
    return cnt

def gold_dim_conta(spark, silver_schema, gold_schema):
    """Cria dim_conta a partir de silver.contas_particionado."""
    spark.sql(f"""
    CREATE OR REPLACE TABLE {gold_schema}.dim_conta
    USING DELTA
    PARTITIONED BY (ano_mes)
    AS SELECT
      ROW_NUMBER() OVER (ORDER BY id_conta) AS sk_conta,
      id_conta,
      id_cliente,
      tipo_conta,
      status_conta,
      data_abertura,
      ano_mes
    FROM {silver_schema}.contas_particionado
    """)
    cnt = spark.table(f"{gold_schema}.dim_conta").count()
    print(f"dim_conta: {cnt} registros")
    return cnt

def gold_dim_cartao(spark, silver_schema, gold_schema):
    """Cria dim_cartao a partir de silver.cartoes_particionado."""
    spark.sql(f"""
    CREATE OR REPLACE TABLE {gold_schema}.dim_cartao
    USING DELTA
    PARTITIONED BY (ano_mes)
    AS SELECT
      ROW_NUMBER() OVER (ORDER BY id_cartao) AS sk_cartao,
      id_cartao,
      id_conta,
      bandeira,
      tipo_cartao,
      status_cartao,
      data_emissao,
      data_atualizacao,
      ano_mes
    FROM {silver_schema}.cartoes_particionado
    """)
    cnt = spark.table(f"{gold_schema}.dim_cartao").count()
    print(f"dim_cartao: {cnt} registros")
    return cnt

def gold_dim_estabelecimento(spark, silver_schema, gold_schema):
    """Cria dim_estabelecimento a partir de silver.transacoes_financeiras_particionado."""
    spark.sql(f"""
    CREATE OR REPLACE TABLE {gold_schema}.dim_estabelecimento
    USING DELTA
    AS SELECT
      ROW_NUMBER() OVER (ORDER BY nome_estabelecimento, mcc, cidade_estabelecimento) AS sk_estabelecimento,
      nome_estabelecimento,
      mcc,
      cidade_estabelecimento
    FROM (
      SELECT DISTINCT
        nome_estabelecimento,
        mcc,
        cidade_estabelecimento
      FROM {silver_schema}.transacoes_financeiras_particionado
    )
    """)
    cnt = spark.table(f"{gold_schema}.dim_estabelecimento").count()
    print(f"dim_estabelecimento: {cnt} registros")
    return cnt

def gold_dim_tempo(spark, silver_schema, gold_schema):
    """Cria dim_tempo a partir de silver.transacoes_financeiras_particionado."""
    spark.sql(f"""
    CREATE OR REPLACE TABLE {gold_schema}.dim_tempo
    USING DELTA
    AS SELECT
      ROW_NUMBER() OVER (ORDER BY ano_mes) AS sk_tempo,
      ano_mes,
      CAST(SUBSTRING(ano_mes, 1, 4) AS INT) AS ano,
      CAST(SUBSTRING(ano_mes, 6, 2) AS INT) AS mes,
      CASE
        WHEN CAST(SUBSTRING(ano_mes, 6, 2) AS INT) <= 3 THEN 1
        WHEN CAST(SUBSTRING(ano_mes, 6, 2) AS INT) <= 6 THEN 2
        WHEN CAST(SUBSTRING(ano_mes, 6, 2) AS INT) <= 9 THEN 3
        ELSE 4
      END AS trimestre,
      CASE CAST(SUBSTRING(ano_mes, 6, 2) AS INT)
        WHEN 1 THEN 'Jan'
        WHEN 2 THEN 'Fev'
        WHEN 3 THEN 'Mar'
        WHEN 4 THEN 'Abr'
        WHEN 5 THEN 'Mai'
        WHEN 6 THEN 'Jun'
        WHEN 7 THEN 'Jul'
        WHEN 8 THEN 'Ago'
        WHEN 9 THEN 'Set'
        WHEN 10 THEN 'Out'
        WHEN 11 THEN 'Nov'
        WHEN 12 THEN 'Dez'
      END AS nome_mes
    FROM (
      SELECT DISTINCT ano_mes
      FROM {silver_schema}.transacoes_financeiras_particionado
      WHERE ano_mes IS NOT NULL
    )
    """)
    cnt = spark.table(f"{gold_schema}.dim_tempo").count()
    print(f"dim_tempo: {cnt} registros")
    return cnt

# ============================================================
# STAR SCHEMA — Tabela Fato
# ============================================================

def gold_fact_transacoes_financeiras(spark, silver_schema, gold_schema):
    """Cria fact_transacoes_financeiras com chaves surrogate para todas as dimensões."""
    spark.sql(f"""
    CREATE OR REPLACE TABLE {gold_schema}.fact_transacoes_financeiras
    USING DELTA
    PARTITIONED BY (ano_mes)
    AS SELECT
      t.id_transacao,
      dc.sk_cliente,
      dc2.sk_conta,
      dc3.sk_cartao,
      de.sk_estabelecimento,
      dt.sk_tempo,
      t.data_transacao,
      t.valor,
      1 AS qtd_transacao,
      t.ano_mes
    FROM {silver_schema}.transacoes_financeiras_particionado t
    JOIN {gold_schema}.dim_cartao dc3
      ON t.id_cartao = dc3.id_cartao
    JOIN {gold_schema}.dim_conta dc2
      ON dc3.id_conta = dc2.id_conta
    JOIN {gold_schema}.dim_cliente dc
      ON dc2.id_cliente = dc.id_cliente
    JOIN {gold_schema}.dim_estabelecimento de
      ON COALESCE(t.nome_estabelecimento, 'N/A') = COALESCE(de.nome_estabelecimento, 'N/A')
      AND COALESCE(CAST(t.mcc AS STRING), 'N/A') = COALESCE(CAST(de.mcc AS STRING), 'N/A')
      AND COALESCE(t.cidade_estabelecimento, 'N/A') = COALESCE(de.cidade_estabelecimento, 'N/A')
    JOIN {gold_schema}.dim_tempo dt
      ON t.ano_mes = dt.ano_mes
    """)
    qtd_fato = spark.table(f"{gold_schema}.fact_transacoes_financeiras").count()
    print(f"fact_transacoes_financeiras: {qtd_fato} registros")
    return qtd_fato

# ============================================================
# STAR SCHEMA — Validação
# ============================================================

def gold_validacao_star_schema(spark, silver_schema, gold_schema):
    """Valida integridade referencial e consistência de valores do star schema."""
    tabelas = [
        ("dim_cliente",                 f"{gold_schema}.dim_cliente"),
        ("dim_conta",                   f"{gold_schema}.dim_conta"),
        ("dim_cartao",                  f"{gold_schema}.dim_cartao"),
        ("dim_estabelecimento",         f"{gold_schema}.dim_estabelecimento"),
        ("dim_tempo",                   f"{gold_schema}.dim_tempo"),
        ("fact_transacoes_financeiras",  f"{gold_schema}.fact_transacoes_financeiras"),
    ]

    linhas = [(nome, spark.table(tbl).count()) for nome, tbl in tabelas]
    df_resumo = spark.createDataFrame(linhas, "tabela string, registros long")
    print("\n=== Resumo de registros ===")
    df_resumo.show(truncate=False)

    # Integridade referencial (registros órfãos na fato)
    fato = spark.table(f"{gold_schema}.fact_transacoes_financeiras")
    ri_checks = []
    for sk_col, dim_tbl in [
        ("sk_cliente",         f"{gold_schema}.dim_cliente"),
        ("sk_conta",           f"{gold_schema}.dim_conta"),
        ("sk_cartao",          f"{gold_schema}.dim_cartao"),
        ("sk_estabelecimento", f"{gold_schema}.dim_estabelecimento"),
        ("sk_tempo",           f"{gold_schema}.dim_tempo"),
    ]:
        orfaos = fato.join(spark.table(dim_tbl), sk_col, "left_anti").count()
        ri_checks.append((sk_col, dim_tbl.split(".")[-1], orfaos))

    df_ri = spark.createDataFrame(ri_checks, "fk_col string, dimensao string, orfaos long")
    print("\n=== Integridade referencial (órfãos) ===")
    df_ri.show(truncate=False)

    # Soma de valor (deve bater com silver)
    soma_gold   = fato.agg(F.sum("valor").alias("soma")).collect()[0]["soma"]
    soma_silver = spark.table(f"{silver_schema}.transacoes_financeiras_particionado").agg(F.sum("valor").alias("soma")).collect()[0]["soma"]
    print(f"\nSoma valor: gold={soma_gold} | silver={soma_silver} | match={soma_gold == soma_silver}")

    # Query exemplo — top 5 clientes por valor total
    print("\n=== Top 5 clientes por valor total ===")
    spark.sql(f"""
    SELECT
      dc.nome,
      dc.estado,
      COUNT(*)      AS qtd_transacoes,
      SUM(f.valor)   AS valor_total
    FROM {gold_schema}.fact_transacoes_financeiras f
    JOIN {gold_schema}.dim_cliente dc ON f.sk_cliente = dc.sk_cliente
    GROUP BY dc.nome, dc.estado
    ORDER BY valor_total DESC
    LIMIT 5
    """).show(truncate=False)

    return df_resumo

# ============================================================
# REGRAS DE NEGÓCIO — Análises Semânticas (SQL Avançado)
# ============================================================

def gold_semantic_cte_transacoes_cliente(spark, gold_schema):
    """CTE: análise de transações por cliente com soma de valor por mês."""
    df = spark.sql(f"""
    WITH transacoes_cliente AS (
        SELECT id_cliente, date_format(data_transacao, 'yyyy-MM') AS ano_mes, valor
        FROM {gold_schema}.transacoes_cartao
        WHERE valor > 0
    )
    SELECT id_cliente, ano_mes, SUM(valor) AS valor_total
    FROM transacoes_cliente
    GROUP BY id_cliente, ano_mes
    ORDER BY valor_total DESC
    """)
    print("\n=== CTE: Transações por cliente ===")
    df.show(20, truncate=False)
    return df

def gold_semantic_row_number_versionamento(spark, gold_schema):
    """ROW_NUMBER: transação mais recente de cada cliente por mês."""
    df = spark.sql(f"""
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY id_cliente, date_format(data_transacao, 'yyyy-MM') ORDER BY data_transacao DESC) AS rn
    FROM {gold_schema}.transacoes_cartao
    QUALIFY rn = 1
    """)
    print("\n=== ROW_NUMBER: Transação mais recente por cliente/mês ===")
    df.show(20, truncate=False)
    return df

def gold_semantic_lag_lead_mensal(spark, gold_schema):
    """LAG/LEAD: movimentação de valor entre meses consecutivos por cliente."""
    df = spark.sql(f"""
    SELECT 
        id_cliente,
        mes AS ano_mes,
        valor_total_mes AS valor_total,
        LAG(valor_total_mes) OVER (PARTITION BY id_cliente ORDER BY mes) AS valor_mes_anterior,
        LEAD(valor_total_mes) OVER (PARTITION BY id_cliente ORDER BY mes) AS valor_proximo_mes,
        (valor_total_mes - LAG(valor_total_mes) OVER (PARTITION BY id_cliente ORDER BY mes)) AS variacao
    FROM {gold_schema}.resumo_cliente_mes
    """)
    print("\n=== LAG/LEAD: Movimentação mensal ===")
    df.show(20, truncate=False)
    return df

def gold_semantic_ranking_clientes(spark, gold_schema):
    """RANK/DENSE_RANK: ranking de clientes por valor movimentado no mês."""
    df = spark.sql(f"""
    SELECT 
        id_cliente, mes AS ano_mes, valor_total_mes AS valor_total,
        RANK() OVER (ORDER BY valor_total_mes DESC) AS ranking_valor,
        DENSE_RANK() OVER (ORDER BY valor_total_mes DESC) AS ranking_valor_dense
    FROM {gold_schema}.resumo_cliente_mes
    """)
    print("\n=== RANK/DENSE_RANK: Ranking de clientes ===")
    df.show(20, truncate=False)
    return df

def gold_semantic_crescimento_50pct(spark, gold_schema):
    """Clientes com crescimento superior a 50% entre dois meses consecutivos."""
    df = spark.sql(f"""
    SELECT *
    FROM (
      SELECT 
        id_cliente, mes AS ano_mes, valor_total_mes AS valor_total,
        LAG(valor_total_mes) OVER (PARTITION BY id_cliente ORDER BY mes) AS valor_mes_anterior,
        CASE
          WHEN LAG(valor_total_mes) OVER (PARTITION BY id_cliente ORDER BY mes) > 0 THEN
               (valor_total_mes - LAG(valor_total_mes) OVER (PARTITION BY id_cliente ORDER BY mes)) / LAG(valor_total_mes) OVER (PARTITION BY id_cliente ORDER BY mes)
          ELSE NULL END AS crescimento_relativo
      FROM {gold_schema}.resumo_cliente_mes
    ) t
    WHERE crescimento_relativo > 0.5
    """)
    print("\n=== Clientes com crescimento > 50% ===")
    df.show(20, truncate=False)
    return df

def gold_semantic_quartis_ntile(spark, gold_schema):
    """NTILE: classificação de clientes em quartis por valor movimentado."""
    df = spark.sql(f"""
    SELECT 
        id_cliente, mes AS ano_mes, valor_total_mes AS valor_total,
        NTILE(4) OVER (ORDER BY valor_total_mes DESC) AS quartil_valor_movimentado
    FROM {gold_schema}.resumo_cliente_mes
    """)
    print("\n=== NTILE: Quartis de valor movimentado ===")
    df.show(20, truncate=False)
    return df

# ============================================================
# Função para rodar pipeline completa
# ============================================================

def run_pipeline_gold(spark, silver_schema="workspace.silver", gold_schema="workspace.gold"):
    # Pipeline analítico (tabelas denormalizadas)
    gold_transacoes_cartao(spark, silver_schema, gold_schema)
    gold_resumo_cliente_mes(spark, silver_schema, gold_schema)
    gold_ranking_estabelecimento(spark, gold_schema)
    gold_alertas_qualidade(spark, silver_schema, gold_schema)

    # Star schema (dimensões + fato + validação)
    gold_dim_cliente(spark, silver_schema, gold_schema)
    gold_dim_conta(spark, silver_schema, gold_schema)
    gold_dim_cartao(spark, silver_schema, gold_schema)
    gold_dim_estabelecimento(spark, silver_schema, gold_schema)
    gold_dim_tempo(spark, silver_schema, gold_schema)
    gold_fact_transacoes_financeiras(spark, silver_schema, gold_schema)
    gold_validacao_star_schema(spark, silver_schema, gold_schema)

    # Regras de negócio — análises semânticas (SQL avançado)
    gold_semantic_cte_transacoes_cliente(spark, gold_schema)
    gold_semantic_row_number_versionamento(spark, gold_schema)
    gold_semantic_lag_lead_mensal(spark, gold_schema)
    gold_semantic_ranking_clientes(spark, gold_schema)
    gold_semantic_crescimento_50pct(spark, gold_schema)
    gold_semantic_quartis_ntile(spark, gold_schema)

    print("Camada Gold concluída.")

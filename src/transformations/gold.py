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

# Funções para rodar pipeline completa

def run_pipeline_gold(spark, silver_schema="workspace.silver", gold_schema="workspace.gold"):
    gold_transacoes_cartao(spark, silver_schema, gold_schema)
    gold_resumo_cliente_mes(spark, silver_schema, gold_schema)
    gold_ranking_estabelecimento(spark, gold_schema)
    gold_alertas_qualidade(spark, silver_schema, gold_schema)
    print("Camada Gold concluída.")

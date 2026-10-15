import pytest
from pyspark.sql import SparkSession, Row
from quality.rules import dedup_por_chave

def test_dedup_por_chave():
    spark = SparkSession.builder.getOrCreate()
    data = [
        Row(id_cliente=1, nome="João", data_atualizacao="2021-01-02"),
        Row(id_cliente=1, nome="João X", data_atualizacao="2021-01-05"),
    ]
    df = spark.createDataFrame(data)
    result = dedup_por_chave(df, ['id_cliente'], [df.data_atualizacao.desc()])
    assert result.count() == 1
    assert result.collect()[0]['data_atualizacao'] == "2021-01-05"

from pyspark.sql import SparkSession, Row
from quality.rules import valida_nulos

def test_valida_nulos():
    spark = SparkSession.builder.getOrCreate()
    data = [Row(id=1, nome='Ana'), Row(id=2, nome=None)]
    df = spark.createDataFrame(data)
    nulos = valida_nulos(df, 'nome')
    assert nulos.count() == 1
    assert nulos.collect()[0]['id'] == 2

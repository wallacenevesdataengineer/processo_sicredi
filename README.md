O processo desenvolvido para case do Sicredi com objetivo de analise de processos de clientes e transações de processos.
A arquitetura Medallion com camadas bronze, silver e gold.
A primeira etapa são armazenar os arquivos CSV. Foi incluído dentro do volumes em schema default dentro do databricks.
Na camada bronze extraindo desses arquivos e criando as tabelas bronze.
Na Silver fazendo às transformações de dados também criado tabelas de quarentena com os dados inválidos vindo da base da bronze.
Na camada Gold analítica criada com as tabelas com regras de negocio definidas. 

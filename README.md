O processo desenvolvido para case do Sicredi com objetivo de analise de processos de clientes,transações, contas e estornos.
A arquitetura Medallion com camadas bronze, silver e gold.
A primeira etapa são armazenar os arquivos CSV. Foi incluído dentro do volumes em schema default dentro do databricks.
Na camada bronze extraindo desses arquivos e criando as tabelas bronze.
Na Silver fazendo às transformações de dados também criando tabelas de quarentena com os dados inválidos vindo da base da bronze.
Na camada Gold analítica criada com as tabelas com regras de negocio definidas. 

<img width="785" height="329" alt="image" src="https://github.com/user-attachments/assets/6ba258bb-5ab2-48f8-80d3-4c00bc413f16" />


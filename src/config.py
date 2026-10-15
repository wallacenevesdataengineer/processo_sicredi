# Parâmetros e configuração centralizada do pipeline

CATALOG = "workspace"
BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver"
GOLD = f"{CATALOG}.gold"

CAMADAS = ["bronze", "silver", "gold"]

DATA_PROCESSAMENTO = None  # para definir dinamicamente

COLUNAS_CLIENTES = ["id_cliente", "nome", "cpf", "data_nascimento", "cidade", "estado", "data_atualizacao"]
COLUNAS_CONTAS = ["id_conta", "id_cliente", "tipo_conta", "status_conta", "data_abertura"]
COLUNAS_CARTOES = ["id_cartao", "id_conta", "bandeira", "tipo_cartao", "status_cartao", "data_emissao", "data_atualizacao"]
COLUNAS_TRANSACOES = ["id_transacao", "id_cartao", "data_transacao", "valor", "nome_estabelecimento", "mcc", "cidade_estabelecimento"]
COLUNAS_ESTORNOS = ["id_estorno", "id_transacao", "data_estorno", "motivo_estorno"]

# Nome das tabelas quarentena
QUARENTENA_CLIENTES = f"{SILVER}.quarentena_clientes"
QUARENTENA_CONTAS = f"{SILVER}.quarentena_contas"
QUARENTENA_CARTOES = f"{SILVER}.quarentena_cartoes"
QUARENTENA_TRANSACOES = f"{SILVER}.quarentena_transacoes"
QUARENTENA_ESTORNOS = f"{SILVER}.quarentena_estornos"

# Você pode acrescentar mais parâmetros globais aqui para facilitar manutenção do pipeline

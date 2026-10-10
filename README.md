# Currency Data Pipeline

Pipeline de dados de câmbio orquestrado pelo Apache Airflow.

## Extração da Frankfurter

A integração com a API v2 da Frankfurter fica isolada em
`scripts/extract/extract.py`. A DAG deve apenas importar e orquestrar essa
função:

```python
from scripts.extract import extract_currencies, extract_rates


def extract_currency_rates(**context):
    return extract_rates(
        base="BRL",
        quotes=["USD", "EUR"],
        date=context["ds"],
    )
```

Também é possível extrair um intervalo histórico:

```python
rates = extract_rates(
    base="BRL",
    quotes=["USD", "EUR"],
    start_date="2026-01-01",
    end_date="2026-01-31",
)
```

O catálogo de moedas pode ser extraído separadamente para alimentar uma tabela
de dimensão com código ISO, nome, símbolo e período de disponibilidade:

```python
currencies = extract_currencies(include_legacy=False)
```

Não é necessário consultar o catálogo antes de cada extração de taxas. Quando
`quotes` é omitido, a própria rota de taxas retorna todas as moedas disponíveis
para a moeda-base e os filtros informados.

A função devolve a resposta normalizada da API v2 como uma lista de registros,
um por par de moedas. Erros HTTP, de conexão e respostas inválidas geram
`FrankfurterError`, permitindo que o mecanismo de retries do Airflow cuide das
novas tentativas.

As chamadas HTTP utilizam a biblioteca `requests` com uma sessão reutilizável e
timeout configurável.

## Persistência local da camada raw

A função `run_extract` coordena as duas chamadas e persiste os resultados. Ela
recebe opcionalmente o lote; fora do Airflow, gera um lote com o timestamp UTC:

```python
from scripts.extract import run_extract


result = run_extract(batch_id="20260928190000")
```

O retorno é apenas o caminho absoluto do arquivo, pequeno o suficiente para ser
compartilhado via XCom. Os dados são gravados com esta organização:

```text
data/raw/
├── currencies/batch_id=<batch_id>/data.json
└── rates/batch_id=<batch_id>/data.json
```

A gravação continua usando um arquivo temporário e uma substituição atômica.
Assim, uma falha durante a escrita não deixa um JSON parcialmente gravado.
Reexecuções no mesmo caminho substituem deterministicamente o arquivo anterior.

No Docker, `data/` é compartilhado em `/opt/airflow/data` por todos os serviços
e persistido no host. Seu conteúdo é ignorado pelo Git.

## DAG do pipeline

A DAG `currency_exchange_rates_dag` executa diariamente com esta sequência:

```text
generate_batch_id → run_extract → run_bronze → run_silver → run_gold
```

`generate_batch_id` cria o lote a partir da data lógica do DagRun convertida
para `America/Sao_Paulo` (UTC−3 atualmente). `run_extract` recebe esse valor
pela TaskFlow API, que armazena e resolve o XCom
automaticamente, e retorna somente o lote e os caminhos dos arquivos.
`run_bronze` recebe o lote retornado por `run_extract`, e `run_silver` recebe
o lote retornado por `run_bronze`. `run_gold` recebe o lote da silver e constrói
as dimensões e a fato. Essas referências também criam as
dependências entre as tasks: uma camada só inicia após o sucesso da anterior.
Bronze, silver e gold persistem os Parquets e carregam suas tabelas PostgreSQL.

O Airflow `3.3.0`, a mesma versão padrão do Docker Compose, é gerenciado no
`pyproject.toml` e no `uv.lock`. Em Linux/macOS, `uv sync` instala também o
Airflow. No Windows nativo, a dependência é omitida por condição de plataforma;
o pipeline continua executando o Airflow nos containers Linux do Compose.

A imagem definida no `Dockerfile` estende `apache/airflow:3.3.0` e instala as
dependências exportadas do `uv.lock`, incluindo Polars. Todos os serviços do
Airflow no Compose usam essa imagem. Após mudar as dependências, reconstrua
e atualize os containers:

```bash
docker compose up -d --build
```

Também é possível executar a extração fora do Airflow:

```bash
python scripts/extract/extract.py
```

A execução direta configura logs no terminal. No Airflow, as mesmas mensagens
ficam disponíveis nos logs da task `run_extract`, incluindo início e fim da
extração, endpoint consultado, status HTTP, quantidade de registros, arquivos
persistidos e eventuais falhas.

Por enquanto, moedas e cotações ficam na mesma DAG para compartilharem o mesmo
lote. Elas podem ser separadas futuramente caso precisem de agendamentos ou
políticas de retry diferentes.

Os containers recebem `scripts` em `/opt/airflow/scripts` e usam
`PYTHONPATH=/opt/airflow`, portanto o mesmo import funciona localmente e no
Airflow.

## Camada bronze

`scripts/bronze.py` lê os JSONs de um mesmo lote e mantém todos os campos da API
como texto nullable, sem converter datas ou taxas para tipos semânticos. Apenas
os metadados operacionais mantêm tipos apropriados para monitoramento:

- `_source_path`: caminho do JSON que originou a linha;
- `_source_file`: nome do arquivo de origem;
- `_source_extension`: extensão do arquivo de origem;
- `_source_row_number`: posição da linha no JSON, iniciando em 1;
- `_ingestion_batch_id`: identificador do lote de ingestão;
- `_ingested_at_utc`: instante da ingestão com timezone;
- `_record_hash`: hash SHA-256 determinístico dos campos de origem da linha.

Os Parquets são escritos atomicamente nestes caminhos:

```text
data/bronze/
├── currencies/<batch_id>/currencies.parquet
└── rates/<batch_id>/rates.parquet
```

Sem `--batch-id`, o script escolhe o lote mais recente que exista tanto em
`currencies` quanto em `rates`:

```bash
uv run python -m scripts.bronze
uv run python -m scripts.bronze --batch-id 20260930104015
```

Para gerar somente os Parquets, sem conexão com o banco:

```bash
uv run python -m scripts.bronze --skip-database
```

## Camada silver

`scripts/silver.py` lê os Parquets bronze de um mesmo lote, transforma os dados
com Polars, carrega `silver.currencies` e `silver.rates` no PostgreSQL e grava
arquivos Parquet nestes caminhos:

```text
data/silver/
├── currencies/<batch_id>/currencies.parquet
└── rates/<batch_id>/rates.parquet
```

Instale as dependências e execute:

```bash
uv sync
uv run python -m scripts.silver
uv run python -m scripts.silver --batch-id 20261001084723
```

Sem `--batch-id`, o script seleciona o maior identificador em ordem lexical
que tenha os dois Parquets bronze completos (o mais recente para lotes no
formato `YYYYMMDDHHMMSS`). `--bronze-dir` e `--silver-dir` permitem configurar
as raízes dos arquivos. Também funciona com `python scripts/silver.py`.

A carga usa o serviço `postgres-pipeline` do Compose e as mesmas configurações
`DATABASE_*` da bronze, lidas do `.env`. Inicie o serviço antes da execução:

```bash
docker compose up -d postgres-pipeline
```

Por padrão, os dois destinos são persistidos. Para gravar apenas os Parquets:

```bash
uv run python -m scripts.silver --skip-database
```

Em Python, use `run_silver(persist_database=False)` para o mesmo comportamento.
`--sql-dir` permite selecionar outra pasta de DDL.

Antes de cada carga, `sql/silver` é executado em ordem lexical para criar o
schema e as tabelas de forma idempotente. A carga usa `COPY FROM STDIN` e
substitui somente o `_ingestion_batch_id` solicitado nas duas tabelas. DDL, remoção do
lote e inserção dos dois datasets compartilham uma transação: uma falha no
banco desfaz toda a carga. Os Parquets são escritos antes dessa transação e
permanecem disponíveis se a carga falhar. A conversão para pandas com tipos
Arrow ocorre somente na chamada ao writer compartilhado de PostgreSQL;
as transformações continuam em Polars.

As funções gerais podem ser reutilizadas em qualquer dataset:

- `clean_text`: remove espaços nas extremidades, converte texto vazio em nulo
  e permite configurar caixa, redução de espaços internos e o tipo de destino;
- `cast_columns`: recebe um mapa de coluna para tipo Polars;
- `parse_dates`: limpa textos e converte datas usando um formato configurável;
- `require_columns`: valida a presença das colunas esperadas;
- `persist_parquet`: grava um DataFrame Polars com substituição atômica.

```python
import polars as pl
from scripts.silver import clean_text, run_silver

frame = frame.with_columns(
    clean_text("base", case="upper"),
    clean_text("rate", cast=pl.Float64),
)
result = run_silver(batch_id="20261001084723")
```

`transform_currencies` aplica as regras do catálogo: código ISO em maiúsculas,
nome com espaços internos normalizados, símbolo preservado e datas como
`Date`. `iso_numeric` permanece como texto com três dígitos, preservando zeros
à esquerda (`008`); valores vazios viram nulos. Códigos numéricos, símbolos e
datas de cobertura aceitam nulos; nome e código ISO são obrigatórios. As datas
descrevem a cobertura de publicação da API. Quando ambas estão preenchidas,
`start_date` não pode ser posterior a `end_date`.

`transform_rates` normaliza `base` e `quote` em maiúsculas, converte `date`
para `Date` e `rate` para `Float64`, sem arredondamento explícito. Os quatro
campos são obrigatórios e as taxas devem ser positivas e finitas. `Float64`
usa representação aproximada de ponto flutuante.

Códigos de moeda devem ter três letras. Conversões inválidas interrompem o lote
com erro; as funções gerais permitem `strict=False` para converter valores
inválidos em nulos quando essa política for necessária. Linhas, sua ordem e
metadados bronze são preservados, sem deduplicação ou exclusão silenciosa.
Ambos os datasets são validados antes de gravar qualquer saída. A escrita é
atômica por arquivo; reexecutar o mesmo lote substitui seus Parquets. O retorno
contém o lote, caminhos absolutos e contagens de linhas, permitindo usar
`run_silver` em uma task do Airflow.

Os logs da silver mostram leitura da bronze, início de cada dataset, limpeza
de textos, conversão de tipos, validação, escrita de Parquet, execução dos DDL,
remoção do lote anterior e carga via COPY. Cada dataset tem um resumo com lote,
linhas recebidas, linhas processadas, linhas rejeitadas e duração. A conclusão
da carga no PostgreSQL só é registrada após a saída do contexto transacional.

Uma rejeição registra a regra que falhou, a quantidade de linhas afetadas e
uma amostra de até cinco registros, incluindo `_source_row_number` e os valores
envolvidos. Erros de conversão também identificam valores inválidos. O resumo
da falha conta linhas distintas identificadas até a interrupção; outras regras
podem não ter sido avaliadas. Quando não é possível determinar a quantidade
(por exemplo, por falta de colunas), o log mostra `rows_rejected=unknown`.
`rows_processed` conta apenas linhas de um dataset concluído com sucesso, por
isso é zero no resumo de falha. A política de rejeição continua interrompendo
o lote, sem excluir linhas e seguir com uma carga parcial.

## Camada gold

`scripts/gold.py` constrói um modelo estrela a partir dos Parquets silver:

- `gold.dim_currency`: uma moeda por código ISO, compartilhada pelos papéis
  de moeda base e moeda cotada;
- `gold.dim_date`: um dia por linha, com atributos de calendário;
- `gold.fact_exchange_rate`: uma cotação por data, moeda base e moeda cotada,
  com três chaves estrangeiras e a taxa original, sem métricas agrupadas.

As transformações usam Polars, funções documentadas e helpers parametrizados
para deduplicação, resolução de chaves e persistência. As chaves permanecem
estáveis entre lotes. A dimensão de moeda usa SCD tipo 1; fatos sobrepostos
são atualizados pela última carga executada com sucesso. Reexecutar um lote
não duplica cotações. Toda a carga SQL é transacional.

```bash
uv run python -m scripts.gold
uv run python -m scripts.gold --batch-id 20261009090000
uv run python -m scripts.gold --skip-database
```

Os DDLs ficam em `sql/gold`. Os Parquets seguem o formato
`data/gold/<tabela>/<batch_id>/<tabela>.parquet`. O modelo completo, diagrama,
políticas de carga, funções e exemplo de consulta estão em
[docs/gold.md](docs/gold.md).

## PostgreSQL do pipeline

O serviço `postgres-pipeline` usa a imagem `postgres:16`, a porta local definida
por `DATABASE_PORT` (5432 por padrão no Compose)
e o volume `postgres-pipeline-db-volume`. Ele é separado do serviço `postgres`
e do volume usados pelos metadados do Airflow.

Copie e ajuste `.env.example` quando necessário. O `.env` real é ignorado pelo
Git e lido pelo Pydantic:

```bash
docker compose up -d postgres-pipeline
```

Antes de cada carga, a camada executa em ordem os arquivos idempotentes de
`SQL/bronze`. Esses arquivos criam o schema `bronze`, as tabelas `currencies` e
`rates` e garantem que as colunas vindas da API sejam `TEXT`. A mesma estrutura
da silver fica em `sql/silver`, com tipos semânticos. `sql/gold` cria as dimensões de moeda e data e a fato de cotações, com
chaves primárias, estrangeiras e índices.

A gravação usa o protocolo `COPY FROM STDIN` do psycopg. Reexecutar o mesmo lote
remove e recarrega somente aquele `_ingestion_batch_id`, na mesma transação dos arquivos
SQL, sem duplicar registros.

O pacote `database` centraliza configuração validada, conexão/transação e as
operações `DELETE`, `TRUNCATE`, `INSERT`, upsert com
`INSERT ... ON CONFLICT DO UPDATE` e carga de DataFrames via `COPY`.

## Testes

```bash
python -m unittest discover -s tests -v
```


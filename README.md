# iFood Data Engineering Case

## 1. Objetivo

Este repositório contém a solução desenvolvida para o case técnico de Data Engineering do iFood.

O objetivo do projeto é construir uma solução de dados para ingestão, armazenamento, transformação e disponibilização dos dados públicos de corridas de Yellow Taxi da NYC TLC, considerando inicialmente o período de janeiro a maio de 2023.

A solução utiliza PySpark e Databricks, seguindo uma abordagem lakehouse em camadas:

- Landing: armazenamento dos arquivos originais, com volume no próprio databricks para otimizar o fluxo da pipe.
- Bronze: leitura controlada e rastreável dos dados brutos.
- Silver: padronização, limpeza e aplicação de regras de qualidade.
- Gold: disponibilização analítica para consumo via SQL/PySpark.

---

## 2. Fonte dos dados

Os dados utilizados são os arquivos públicos de Yellow Taxi Trip Records disponibilizados pela NYC Taxi & Limousine Commission.

Fonte oficial:

https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

Arquivos utilizados na primeira etapa:

```text
yellow_tripdata_2023-01.parquet
yellow_tripdata_2023-02.parquet
yellow_tripdata_2023-03.parquet
yellow_tripdata_2023-04.parquet
yellow_tripdata_2023-05.parquet
```


Os arquivos são baixados a partir do endpoint público:

https://d37ci6vzurychx.cloudfront.net/trip-data/
---
## 3. Arquitetura proposta

A solução foi desenhada com separação entre camadas de dados:
```text
NYC TLC public files
        |
        v
Landing Zone
        |
        v
Bronze
        |
        v
Silver
        |
        v
Gold / SQL analysis
```
### 3.1 Landing Zone

A Landing Zone armazena os arquivos originais da fonte sem aplicação de regras de negócio, limpeza ou transformação.

Objetivos da Landing:

preservar os arquivos originais;
garantir rastreabilidade da origem;
organizar os dados por ano e mês;
permitir reprocessamento futuro;
registrar metadados básicos da ingestão.

Caminho utilizado no Databricks:

dbfs:/Volumes/workspace/ifood_case/case_files/landing/nyc_tlc/yellow_taxi/

Estrutura esperada:
```text
landing/
└── nyc_tlc/
    └── yellow_taxi/
        └── year=2023/
            ├── month=01/
            │   └── yellow_tripdata_2023-01.parquet
            ├── month=02/
            │   └── yellow_tripdata_2023-02.parquet
            ├── month=03/
            │   └── yellow_tripdata_2023-03.parquet
            ├── month=04/
            │   └── yellow_tripdata_2023-04.parquet
            └── month=05/
                └── yellow_tripdata_2023-05.parquet
```
---

## 4. Stack utilizada
Python
PySpark
Databricks Free Edition / Databricks Serverless
Unity Catalog Volumes
GitHub
---
## 5. Estrutura atual do repositório
```text
ifood-case/
├── landing/
│   └── .gitkeep
├── metadata/
│   └── .gitkeep
├── notebooks/
│   └── 01_ingest_landing
├── src/
│   ├── __init__.py
│   └── ingest_landing.py
├── .gitignore
├── README.md
└── requirements.txt
```

### 5.1 src/ingest_landing.py

Contém o código fonte da etapa de ingestão para a Landing Zone.

Principais responsabilidades:

montar as URLs dos arquivos da NYC TLC;
criar o schema e o volume no Unity Catalog;
baixar os arquivos Parquet oficiais;
armazenar os arquivos diretamente no Databricks Volume;
organizar os arquivos por ano e mês;
retornar metadados da execução da ingestão.

### 5.2 notebooks/01_ingest_landing

Notebook responsável por executar e validar a ingestão.

Principais responsabilidades:

importar o código fonte do projeto;
garantir a criação do Unity Catalog Volume;
executar a ingestão dos meses de janeiro a maio de 2023;
salvar o manifesto de ingestão;
validar a estrutura criada na landing;
validar a leitura controlada dos arquivos Parquet.

---
## 6. Decisões técnicas

### 6.1 Uso de Databricks Volumes

Durante a execução no Databricks Free Edition, o uso de dbfs:/FileStore não estava disponível, retornando erro de acesso ao DBFS root.

Por esse motivo, a solução foi ajustada para utilizar Unity Catalog Volumes:

dbfs:/Volumes/workspace/ifood_case/case_files/

Essa decisão deixa a solução mais alinhada ao modelo atual de governança e armazenamento do Databricks.

### 6.2 Escrita direta no Volume

Inicialmente, a solução baixava os arquivos para /tmp e depois copiava para o destino final com dbutils.fs.cp.

No ambiente utilizado, o acesso a arquivos locais fora de /Workspace foi bloqueado. Por isso, a implementação foi ajustada para baixar os arquivos diretamente no caminho /Volumes/..., evitando a etapa intermediária em /tmp.

### 6.3 Preservação dos arquivos originais

A Landing Zone não aplica transformação, cast, filtro ou limpeza nos arquivos originais.

A padronização de schema será feita nas camadas posteriores, principalmente Bronze e Silver.

### 6.4 Leitura controlada dos arquivos

Durante a validação, foi identificada diferença de tipos físicos entre arquivos Parquet mensais. Por exemplo, uma coluna pode ser lida como double em um mês e como INT64 em outro.

Por isso, a validação da landing lê os arquivos mês a mês, aplica casts controlados nas colunas obrigatórias e depois realiza unionByName.
---
## 7. Pré-requisitos

Para executar a solução, é necessário:

ter uma conta no Databricks;
ter acesso ao Databricks Free Edition ou workspace equivalente;
ter o repositório clonado como Git folder no Databricks;
executar os notebooks com compute Serverless ou cluster compatível;
ter acesso à internet para download dos arquivos públicos da NYC TLC.
---
## 8. Como executar a Etapa 1 — Landing Ingestion

### 8.1 Clonar o repositório no Databricks

No Databricks:

Workspace
  -> Create / Add
  -> Git folder
  -> Clone repository

Informe a URL do repositório GitHub.

8.2 Abrir o notebook

Abra o notebook:

notebooks/01_ingest_landing

Conecte o notebook a um compute Serverless ou cluster disponível.

8.3 Executar as células do notebook

Cada diretório mensal deve conter um arquivo Parquet:
```text
month=01/yellow_tripdata_2023-01.parquet
month=02/yellow_tripdata_2023-02.parquet
month=03/yellow_tripdata_2023-03.parquet
month=04/yellow_tripdata_2023-04.parquet
month=05/yellow_tripdata_2023-05.parquet
```
A leitura dos arquivos foi validada de forma controlada, mês a mês, com seleção e cast das colunas obrigatórias:

VendorID
passenger_count
total_amount
tpep_pickup_datetime
tpep_dropoff_datetime

Total de registros lidos na validação: 16.186.386

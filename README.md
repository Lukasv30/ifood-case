# iFood Data Engineering Case

## 1. Visão Geral

Este repositório contém a solução desenvolvida para o case técnico de Data Engineering do iFood.

O objetivo do projeto é construir uma solução de dados para ingestão, armazenamento, transformação, disponibilização e análise dos dados públicos de corridas de Yellow Taxi da NYC TLC, considerando inicialmente o período de janeiro a maio de 2023.

A solução foi implementada com PySpark no Databricks e segue uma arquitetura lakehouse em camadas:

```text
Landing → Raw → Silver → Gold
```

A entrega contempla:

01. ingestão dos arquivos públicos da NYC TLC;
02. armazenamento dos arquivos originais em uma Landing Zone;
03. criação de tabelas modeladas no Data Lake;
04. tratamento de schema e qualidade dos dados;
05. disponibilização de dados para consumo via SQL;
06. respostas analíticas para as perguntas propostas no case.

---
## 2. Fonte dos Dados

Os dados utilizados são os arquivos públicos de Yellow Taxi Trip Records disponibilizados pela NYC Taxi & Limousine Commission.

Página oficial:

https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

Arquivos considerados nesta primeira versão:
```text
yellow_tripdata_2023-01.parquet
yellow_tripdata_2023-02.parquet
yellow_tripdata_2023-03.parquet
yellow_tripdata_2023-04.parquet
yellow_tripdata_2023-05.parquet
```
Endpoint utilizado para download dos arquivos:

https://d37ci6vzurychx.cloudfront.net/trip-data/

---

## 3. Arquitetura da Solução

A arquitetura foi organizada em camadas para separar responsabilidades, facilitar manutenção e permitir reprocessamento.
```text
NYC TLC public files
        |
        v
 Landing Zone
        |
        v
     Raw Layer
        |
        v
  Silver Layer
        |
        v
    Gold Layer
        |
        v
SQL / PySpark Consumption
```
### 3.1 Landing Zone

A Landing Zone armazena os arquivos originais da fonte, sem transformação.

Responsabilidades:

01. baixar os arquivos Parquet oficiais da NYC TLC;
02. preservar os arquivos brutos;
03. organizar os dados por ano e mês;
04. registrar manifesto de ingestão;
05. permitir reprocessamento futuro.

Caminho utilizado no Databricks:
```text
dbfs:/Volumes/workspace/ifood_case/case_files/landing/nyc_tlc/yellow_taxi/

Estrutura esperada:

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
### 3.2 Raw Layer

A Raw Layer é a primeira camada tabular da solução.

Ela lê os arquivos da Landing Zone, trata diferenças técnicas de schema entre os arquivos mensais e cria uma tabela Delta consultável no Unity Catalog.

Tabela criada:

workspace.ifood_case.raw_yellow_taxi_trips

Responsabilidades:

01. ler os arquivos da Landing Zone;
02. realizar leitura mês a mês para evitar conflito de schema;
03. selecionar as colunas necessárias ao case;
04. aplicar casts técnicos mínimos;
05. adicionar metadados de origem;
06. persistir os dados como tabela Delta.
   
### 3.3 Silver Layer

A Silver Layer representa a camada limpa, padronizada e preparada para consumo analítico.

Tabela criada:

workspace.ifood_case.silver_yellow_taxi_trips

Responsabilidades:

01. ler a tabela Raw;
02. garantir presença das colunas obrigatórias;
03. aplicar regras mínimas de qualidade;
04. remover registros inválidos;
05. criar colunas auxiliares de data e hora;
06. persistir os dados tratados como tabela Delta.

Regras de qualidade aplicadas:

01. VendorID não nulo;
02. passenger_count não nulo e maior que zero;
03. total_amount não nulo e maior ou igual a zero;
04. tpep_pickup_datetime não nulo;
05. tpep_dropoff_datetime não nulo;
06. tpep_dropoff_datetime maior que tpep_pickup_datetime;
07. tpep_pickup_datetime dentro do período de janeiro a maio de 2023.

Colunas auxiliares criadas:

pickup_date
pickup_year
pickup_month
pickup_day
pickup_hour
trip_duration_minutes
silver_loaded_at

### 3.4 Gold Layer

A Gold Layer é a camada de consumo analítico.

Ela foi criada para disponibilizar dados aos usuários finais por meio de SQL ou PySpark.

Tabelas criadas:
| Tabela | Objetivo |
|---|---|
| workspace.ifood_case.gold_yellow_taxi_trips |	Tabela analítica de corridas tratadas |
| workspace.ifood_case.gold_avg_total_amount_by_month |	Média mensal de total_amount |
| workspace.ifood_case.gold_avg_passenger_count_may_by_hour |	Média de passenger_count por hora no mês de maio |

A escolha de SQL como linguagem principal de consumo foi feita porque SQL é amplamente utilizado por analistas, engenheiros de dados e áreas de negócio, além de ser suportado diretamente pelo Databricks SQL Editor.
---
## 4. Stack Utilizada

Python
PySpark
Spark SQL
Delta Lake
Databricks Free Edition / Serverless
Unity Catalog
Unity Catalog Volumes
GitHub
---
## 5. Estrutura do Repositório

```text
ifood-case/
├── src/
│   ├── __init__.py
│   ├── ingest_landing.py
│   ├── build_raw.py
│   ├── build_silver.py
│   └── build_gold.py
│
├── notebooks/
│   ├── 01_ingest_landing.ipynb
│   ├── 02_build_raw.ipynb
│   ├── 03_build_silver.ipynb
│   └── 04_build_gold.ipynb
│
├── analysis/
│   ├── 01_avg_total_amount_by_month.sql
│   ├── 02_avg_passenger_count_may_by_hour.sql
│   ├── 00_respostas_perguntas_de_negocio.ipynb
│   └── results.md
│
├── .gitignore
├── README.md
└── requirements.txt
```
### 5.1 Pasta src/

Contém o código fonte reutilizável da solução.

Essa pasta concentra a lógica principal da pipeline, separando código produtizável dos notebooks de execução.

Arquivos:

| Arquivo | Responsabilidade |
|---|---|
| ingest_landing.py |	Download dos arquivos da TLC e escrita na Landing Zone |
| build_raw.py	| Construção da tabela Raw a partir da Landing |
| build_silver.py |	Aplicação de qualidade e criação da tabela Silver |
| build_gold.py |	Criação das tabelas Gold de consumo e agregação |
---
### 5.2 Pasta notebooks/

Contém os notebooks de execução da pipeline.

Os notebooks orquestram as funções implementadas em src/.

### 5.3 Pasta analysis/

Contém as consultas e resultados analíticos do case.

Essa pasta é voltada à resposta das perguntas propostas e à exploração da camada Gold.
---
## 6. Decisões Técnicas

### 6.1 Uso de Databricks

O Databricks foi escolhido por oferecer ambiente Spark gerenciado, suporte a notebooks, tabelas Delta, SQL e integração com GitHub.

Além disso, o case recomenda o uso do Databricks Community Edition ou ambiente equivalente.

### 6.2 Uso de Unity Catalog Volumes

Durante a implementação, o caminho tradicional dbfs:/FileStore não estava disponível no ambiente utilizado.

Por isso, a solução foi implementada utilizando Unity Catalog Volumes:

dbfs:/Volumes/workspace/ifood_case/case_files/

Isso também melhora o alinhamento com práticas modernas de governança no Databricks.

### 6.3 Escrita direta em /Volumes

O ambiente bloqueava a cópia de arquivos locais a partir de /tmp para o destino final.

Para resolver isso, os arquivos passaram a ser baixados diretamente para o caminho /Volumes/..., evitando dependência de filesystem local intermediário.

### 6.4 Separação em camadas

A solução foi separada em Landing, Raw, Silver e Gold para garantir:

01. rastreabilidade;
02. isolamento de responsabilidades;
03. reprocessamento;
04. governança;
05. clareza para consumidores finais.
   
### 6.5 Leitura mês a mês dos arquivos Parquet

Durante a validação da Landing Zone, foram identificadas diferenças de schema físico entre arquivos mensais.

Por isso, os arquivos são lidos mês a mês, recebem casts explícitos e só depois são unidos com unionByName.

Essa abordagem evita falhas de leitura causadas por divergências físicas entre arquivos Parquet.
---
## 7. Pré-requisitos

Para executar o projeto, é necessário:

01. conta no Databricks;
02. acesso a um workspace Databricks Free Edition ou equivalente;
03. compute Serverless ou cluster com suporte a PySpark;
04. repositório clonado como Git folder no Databricks;
05. acesso à internet para download dos arquivos públicos da NYC TLC.

**O arquivo requirements.txt é voltado principalmente para desenvolvimento local. No Databricks, PySpark e Spark já fazem parte do runtime.**

Conteúdo sugerido:

pyspark>=3.5.0
pytest>=8.0.0
---
## 8. Como Executar
### 8.1 Clonar o repositório no Databricks

No Databricks:

Workspace
  → Create / Add
  → Git folder
  → Clone repository

Informe a URL do repositório GitHub.

Depois de clonar, conecte os notebooks a um compute Serverless ou cluster disponível.

### 8.2 Executar Etapa 1 — Landing Ingestion

01. Notebook:

notebooks/01_ingest_landing.ipynb

02. Caminho da Landing:

dbfs:/Volumes/workspace/ifood_case/case_files/landing/nyc_tlc/yellow_taxi/

03. Caminho do manifesto:

dbfs:/Volumes/workspace/ifood_case/case_files/metadata/landing_ingestion_manifest

### 8.3 Executar Etapa 2 — Raw Layer

01. Notebook:

notebooks/02_build_raw.ipynb

02. Tabela criada:

workspace.ifood_case.raw_yellow_taxi_trips

### 8.4 Executar Etapa 3 — Silver Layer

01. Notebook:

notebooks/03_build_silver.ipynb

02. Tabela criada:

workspace.ifood_case.silver_yellow_taxi_trips

### 8.5 Executar Etapa 4 — Gold Layer

01. Notebook:

notebooks/04_build_gold.ipynb

02. Tabelas criadas:

workspace.ifood_case.gold_yellow_taxi_trips
workspace.ifood_case.gold_avg_total_amount_by_month
workspace.ifood_case.gold_avg_passenger_count_may_by_hour

### 8.6 Executar Análises

Consultas disponíveis:

01. analysis/01_avg_total_amount_by_month.sql
02. analysis/02_avg_passenger_count_may_by_hour.sql

Também está disponível um notebook de exploração SQL com as respostas de negócio solicitadas:

analysis/00_respostas_perguntas_de_negocio.ipynb

## 9. Manifesto de Ingestão

A etapa de ingestão gera um manifesto com informações operacionais sobre cada arquivo processado.

Campos do manifesto:

| Campo | Descrição |
|---|---|
| year |	Ano do arquivo ingerido |
| month |	Mês do arquivo ingerido | 
| source_url |	URL pública de origem | 
| landing_path |	Caminho final no Data Lake |
| status |	Status da ingestão |
| file_size_bytes |	Tamanho do arquivo |
| ingestion_started_at |	Timestamp de início |
| ingestion_finished_at |	Timestamp de fim |

Status esperado: **success**
---

## 10. Validações Realizadas
###10.1 Landing

Validações:

01. existência dos diretórios de janeiro a maio;
02. existência dos arquivos Parquet em cada diretório mensal;
03. leitura controlada dos arquivos;
04. validação das colunas obrigatórias.

Total de registros lidos na validação da Landing: 16.186.386

### 10.2 Raw

Validações:

01. tabela criada no Unity Catalog;
02. contagem total de registros;
03. contagem por mês;
04. schema padronizado;
05. presença das colunas exigidas.

### 10.3 Silver

Validações:

01. ausência de nulos em colunas obrigatórias;
02. remoção de passenger_count <= 0;
03. remoção de total_amount < 0;
04. remoção de viagens com horário inválido;

**período restrito entre janeiro e maio de 2023.**

### 10.4 Gold

Validações:

01. tabelas Gold criadas no catálogo;
02. consultas SQL executáveis;
03. agregações conferidas;
04. camada disponível para consumo.
---

## 11. Disponibilização para Usuários Finais

Os dados são disponibilizados para consumo na camada Gold.

A linguagem escolhida para o consumo principal foi SQL.

**Exemplo de consulta na tabela Gold:**
```text
SELECT *
FROM workspace.ifood_case.gold_yellow_taxi_trips
LIMIT 100;
```

**Consulta para resposta da primeira pergunta:**
```text
SELECT
    pickup_year,
    pickup_month,
    avg_total_amount,
    trip_count
FROM workspace.ifood_case.gold_avg_total_amount_by_month
ORDER BY pickup_year, pickup_month;
```

**Consulta para resposta da segunda pergunta:**
```text
SELECT
    pickup_hour,
    avg_passenger_count,
    trip_count
FROM workspace.ifood_case.gold_avg_passenger_count_may_by_hour
ORDER BY pickup_hour;
```
---
## 12. Perguntas do Case
### 12.1 Pergunta 1

**Qual a média de valor total (total_amount) recebido em um mês considerando todos os Yellow Taxis da frota?**

Consulta:
```text
SELECT
    pickup_year,
    pickup_month,
    avg_total_amount,
    trip_count
FROM workspace.ifood_case.gold_avg_total_amount_by_month
ORDER BY pickup_year, pickup_month;
```

Resultado obtido:
```text
Ano	Mês	Média total_amount	Quantidade de corridas
2023	1	27.46	2.917.665
2023	2	27.37	2.764.200
2023	3	28.28	3.226.999
2023	4	28.78	3.109.876
2023	5	29.45	3.319.397
```
### 12.2 Pergunta 2

**Qual a média de passageiros (passenger_count) por cada hora do dia que pegaram táxi no mês de maio considerando todos os táxis da frota?**

Consulta:
```text
SELECT
    pickup_hour,
    avg_passenger_count,
    trip_count
FROM workspace.ifood_case.gold_avg_passenger_count_may_by_hour
ORDER BY pickup_hour;
```

Resultado:
```text
pickup_hour	avg_passenger_count	trip_count
0	1.43	88547
1	1.44	57501
2	1.46	37001
3	1.45	24073
4	1.4	15726
5	1.28	18186
6	1.26	45431
7	1.28	91710
8	1.3	125390
9	1.31	140792
10	1.35	153473
11	1.36	167229
12	1.38	180326
13	1.39	184462
14	1.39	200572
15	1.4	204870
16	1.4	204992
17	1.39	223959
18	1.38	237971
19	1.39	213682
20	1.4	189914
21	1.42	194116
22	1.43	179479
23	1.42	139995
```
---
## 13. Processo de Análise Exploratória

Durante o desenvolvimento foram realizadas análises exploratórias para entender:

01. volume de registros por mês;
02. disponibilidade das colunas obrigatórias;
03. diferenças de schema entre arquivos mensais;
04. valores nulos em campos obrigatórios;
05. registros com valores negativos;
06 registros com quantidade inválida de passageiros;
07. viagens com horário de dropoff anterior ou igual ao pickup;
08. distribuição temporal das corridas por mês e hora.

Essas análises orientaram as regras implementadas na camada Silver.
---
## 14. Critérios de Avaliação

### 14.1 Qualidade e organização do código

A solução separa código fonte e execução:

funções reutilizáveis em src/;
notebooks apenas para orquestração;
camadas separadas por responsabilidade;
nomes de tabelas padronizados;
documentação no README.

### 14.2 Possíveis evoluções para um cenário produtivo:

orquestração com Databricks Workflows;
testes automatizados com PyTest;
validações com Great Expectations ou Deequ;
controle incremental por mês;
alertas para falha de ingestão;
dashboard no Databricks SQL;
versionamento de dados com Delta Lake time travel;
parametrização por período;
pipeline CI/CD.

## 15. Status do Projeto
| Etapa | Status |
|---|---|
| Setup do repositório |	Concluído ✅ |
| Ingestão para Landing Zone |	Concluído ✅ | 
| Manifesto de ingestão |	Concluído ✅ |
| Raw Layer |	Concluído ✅ |
| Silver Layer |	Concluído ✅ |
| Gold Layer |	Concluído ✅ |
| Consultas SQL |	Concluído ✅ |
| Documentação |	Concluído ✅ |
| Resultados finais |	Concluído ✅ |

---

## 16. Conclusão

A solução implementa uma arquitetura lakehouse simples, rastreável e extensível para ingestão e análise dos dados de Yellow Taxi da NYC TLC.

O projeto cobre o fluxo completo solicitado no case:

Fonte pública → Landing → Raw → Silver → Gold → SQL Analysis

Os dados são disponibilizados para consumo via SQL na camada Gold, e as respostas analíticas são estruturadas em tabelas e consultas reutilizáveis.
---
## By: Lucas Victor Silva

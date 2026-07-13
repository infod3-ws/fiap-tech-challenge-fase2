# Setup do Ambiente

## Pré-requisitos
- Python 3.11+ (testado com 3.14)
- Conta GCP com billing ativo
- `gcloud` CLI instalado

## 1. Autenticação GCP
gcloud auth login
gcloud auth application-default login
gcloud config set project PROJECT_ID

## 2. Ambiente Python
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt

## 3. Infraestrutura necessária
gcloud services enable bigquery.googleapis.com storage.googleapis.com pubsub.googleapis.com
gcloud storage buckets create gs://BUCKET_NAME --location=southamerica-east1
gcloud pubsub topics create alfabetizacao-eventos-alunos
gcloud pubsub subscriptions create alfabetizacao-sub-alunos --topic=alfabetizacao-eventos-alunos

## 4. Ordem de execução do pipeline
python3 scripts/etl_bronze.py
python3 scripts/etl_silver.py
python3 scripts/etl_gold.py
python3 quality/data_quality.py

## 5. Streaming (dois terminais)
# Terminal 1
python3 scripts/streaming_consumer.py
# Terminal 2
python3 scripts/streaming_producer.py
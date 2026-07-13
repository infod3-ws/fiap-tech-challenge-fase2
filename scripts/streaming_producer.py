import json
import logging
import random
import time
from datetime import datetime, timezone

import pandas as pd
from google.cloud import pubsub_v1, storage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)

BILLING_PROJECT = "carbide-ratio-502315-a2"
BUCKET_NAME     = "fiap-tc2-datalake"
TOPIC_ID        = "alfabetizacao-eventos-alunos"

QTD_EVENTOS   = 200   # tamanho da simulação — ajustável
INTERVALO_SEG = 0.3   # pausa entre eventos, simula chegada gradual

publisher  = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(BILLING_PROJECT, TOPIC_ID)

storage_client = storage.Client(project=BILLING_PROJECT)
bucket          = storage_client.bucket(BUCKET_NAME)

def carregar_amostra():
    """Reaproveita uma partição pequena da Silver como base para simular eventos novos."""
    blob = bucket.blob("silver/alunos/ano=2024/alunos.parquet")
    blob.download_to_filename("/tmp/amostra_streaming.parquet")
    df = pd.read_parquet("/tmp/amostra_streaming.parquet")
    return df.sample(n=QTD_EVENTOS, random_state=42).to_dict(orient="records")

def montar_evento(registro):
    return {
        "id_aluno":     str(registro["id_aluno"]),
        "id_municipio": str(registro["id_municipio"]),
        "ano":          int(registro["ano"]),
        "rede":         registro["rede"],
        "alfabetizado": registro["alfabetizado"],
        "proficiencia": float(registro["proficiencia"]) if pd.notna(registro["proficiencia"]) else None,
        "_evento_timestamp": datetime.now(timezone.utc).isoformat(),
        "_evento_tipo": "nova_medicao_desempenho",
    }

def main():
    log.info("=" * 60)
    log.info(f"INICIANDO PRODUCER — {QTD_EVENTOS} eventos simulados")
    log.info("=" * 60)

    amostra = carregar_amostra()
    enviados = 0

    for registro in amostra:
        evento     = montar_evento(registro)
        payload    = json.dumps(evento).encode("utf-8")
        future     = publisher.publish(topic_path, payload)
        message_id = future.result()

        enviados += 1
        log.info(f"[PRODUCER] Evento {enviados}/{QTD_EVENTOS} publicado | "
                  f"id_aluno={evento['id_aluno']} | message_id={message_id}")

        time.sleep(INTERVALO_SEG + random.uniform(0, 0.2))

    log.info("=" * 60)
    log.info(f"PRODUCER FINALIZADO — {enviados} eventos publicados")
    log.info("=" * 60)

if __name__ == "__main__":
    main()
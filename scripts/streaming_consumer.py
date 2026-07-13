import json
import logging
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

BILLING_PROJECT   = "carbide-ratio-502315-a2"
BUCKET_NAME       = "fiap-tc2-datalake"
SUBSCRIPTION_ID   = "alfabetizacao-sub-alunos"

JANELA_SEGUNDOS   = 5     # tamanho do micro-batch (streaming em pequenas janelas)
TIMEOUT_SEM_EVENTO = 15   # encerra o consumer se ficar tanto tempo sem receber nada

subscriber = pubsub_v1.SubscriberClient()
sub_path   = subscriber.subscription_path(BILLING_PROJECT, SUBSCRIPTION_ID)

storage_client = storage.Client(project=BILLING_PROJECT)
bucket          = storage_client.bucket(BUCKET_NAME)

buffer_eventos = []
ack_ids        = []
ultimo_evento  = time.time()

def salvar_janela():
    global buffer_eventos, ack_ids
    if not buffer_eventos:
        return

    df = pd.DataFrame(buffer_eventos)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    data_hoje = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    arquivo = f"/tmp/streaming_alunos_{ts}.parquet"
    df.to_parquet(arquivo, index=False)

    destino = f"bronze/alunos_streaming/data_ingestao={data_hoje}/janela_{ts}.parquet"
    bucket.blob(destino).upload_from_filename(arquivo)

    log.info(f"[CONSUMER] Janela salva: {len(df)} eventos -> gs://{BUCKET_NAME}/{destino}")

    subscriber.acknowledge(request={"subscription": sub_path, "ack_ids": ack_ids})
    log.info(f"[CONSUMER] {len(ack_ids)} mensagens confirmadas (ack)")

    buffer_eventos = []
    ack_ids = []

def callback(message):
    global buffer_eventos, ack_ids, ultimo_evento
    try:
        evento = json.loads(message.data.decode("utf-8"))
        buffer_eventos.append(evento)
        ack_ids.append(message.ack_id)
        ultimo_evento = time.time()
        log.info(f"[CONSUMER] Evento recebido | id_aluno={evento.get('id_aluno')} | "
                 f"buffer={len(buffer_eventos)}")
    except Exception as e:
        log.error(f"[CONSUMER] Falha ao processar mensagem: {e}")
        message.nack()
        return
    message.ack_id_pending = True  # marcado, mas ack real acontece em lote na janela

def main():
    log.info("=" * 60)
    log.info(f"INICIANDO CONSUMER — janelas de {JANELA_SEGUNDOS}s")
    log.info(f"Escutando: {sub_path}")
    log.info("=" * 60)

    streaming_pull_future = subscriber.subscribe(sub_path, callback=callback)

    try:
        while True:
            time.sleep(JANELA_SEGUNDOS)
            if buffer_eventos:
                salvar_janela()
            if time.time() - ultimo_evento > TIMEOUT_SEM_EVENTO:
                log.info(f"[CONSUMER] Sem eventos novos ha {TIMEOUT_SEM_EVENTO}s — encerrando")
                break
    finally:
        streaming_pull_future.cancel()
        salvar_janela()  # garante que nada fica preso no buffer
        log.info("=" * 60)
        log.info("CONSUMER FINALIZADO")
        log.info("=" * 60)

if __name__ == "__main__":
    main()
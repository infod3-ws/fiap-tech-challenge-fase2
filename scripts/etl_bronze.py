import logging
import hashlib
from datetime import datetime, timezone

import basedosdados as bd
from google.cloud import storage

# ============================================================
# CONFIGURAÇÃO
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)

BILLING_PROJECT = "carbide-ratio-502315-a2"
BUCKET_NAME     = "fiap-tc2-datalake"
DATASET         = "br_inep_avaliacao_alfabetizacao"

INGESTION_TS   = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
INGESTION_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")

# Entidades exigidas pelo desafio + a tabela de dicionário como apoio
ENTIDADES = {
    "uf":                          f"SELECT * FROM `basedosdados.{DATASET}.uf`",
    "meta_alfabetizacao_brasil":   f"SELECT * FROM `basedosdados.{DATASET}.meta_alfabetizacao_brasil`",
    "meta_alfabetizacao_uf":       f"SELECT * FROM `basedosdados.{DATASET}.meta_alfabetizacao_uf`",
    "meta_alfabetizacao_municipio":f"SELECT * FROM `basedosdados.{DATASET}.meta_alfabetizacao_municipio`",
    "municipio":                   f"SELECT * FROM `basedosdados.{DATASET}.municipio`",
    "alunos":                      f"SELECT * FROM `basedosdados.{DATASET}.alunos`",
    "dicionario":                  f"SELECT * FROM `basedosdados.{DATASET}.dicionario`",
}

# ============================================================
# FUNÇÕES
# ============================================================

def extrair(entidade, query):
    log.info(f"[BRONZE] Extraindo entidade: {entidade}")
    try:
        df = bd.read_sql(query=query, billing_project_id=BILLING_PROJECT)
        log.info(f"[BRONZE] {len(df)} registros | colunas={len(df.columns)}")
        return df
    except Exception as e:
        log.error(f"[BRONZE] Falha ao extrair '{entidade}': {e}")
        raise

def adicionar_metadados(df, entidade, query):
    log.info(f"[BRONZE] Adicionando metadados de auditoria: {entidade}")
    df = df.copy()
    df["_ingestion_timestamp"] = INGESTION_TS
    df["_ingestion_date"]      = INGESTION_DATE
    df["_source_entity"]       = entidade
    df["_source_query"]        = query
    df["_record_hash"] = df.astype(str).apply(
        lambda r: hashlib.md5("|".join(r.values).encode()).hexdigest(), axis=1
    )
    return df

def salvar_particionado(df, entidade):
    """Particiona por 'ano' quando a coluna existe; a dicionario não tem ano, salva sem partição."""
    client = storage.Client(project=BILLING_PROJECT)
    bucket = client.bucket(BUCKET_NAME)

    if "ano" in df.columns:
        anos = sorted(df["ano"].dropna().unique())
        log.info(f"[BRONZE] Particionando '{entidade}' por ano: {list(anos)}")
        for ano in anos:
            df_ano  = df[df["ano"] == ano]
            arquivo = f"/tmp/{entidade}_{ano}.parquet"
            df_ano.to_parquet(arquivo, index=False)

            destino = f"bronze/{entidade}/ano={int(ano)}/{entidade}_{INGESTION_TS}.parquet"
            bucket.blob(destino).upload_from_filename(arquivo)
            log.info(f"[BRONZE] {len(df_ano)} registros -> gs://{BUCKET_NAME}/{destino}")
    else:
        arquivo = f"/tmp/{entidade}.parquet"
        df.to_parquet(arquivo, index=False)
        destino = f"bronze/{entidade}/{entidade}_{INGESTION_TS}.parquet"
        bucket.blob(destino).upload_from_filename(arquivo)
        log.info(f"[BRONZE] {len(df)} registros -> gs://{BUCKET_NAME}/{destino}")

# ============================================================
# EXECUÇÃO
# ============================================================

def main():
    log.info("=" * 60)
    log.info("INICIANDO INGESTÃO BRONZE")
    log.info(f"Projeto GCP : {BILLING_PROJECT}")
    log.info(f"Bucket      : {BUCKET_NAME}")
    log.info("=" * 60)

    resumo = {}
    for entidade, query in ENTIDADES.items():
        df = extrair(entidade, query)
        df = adicionar_metadados(df, entidade, query)
        salvar_particionado(df, entidade)
        resumo[entidade] = len(df)

    log.info("=" * 60)
    log.info("SUMÁRIO BRONZE")
    for entidade, total in resumo.items():
        log.info(f"  {entidade:<35} : {total} registros")
    log.info("=" * 60)

if __name__ == "__main__":
    main()
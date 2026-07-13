import logging
from datetime import datetime, timezone

import pandas as pd
from google.cloud import storage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)

BILLING_PROJECT = "carbide-ratio-502315-a2"
BUCKET_NAME     = "fiap-tc2-datalake"
PROCESSED_AT    = datetime.now(timezone.utc).isoformat()

client = storage.Client(project=BILLING_PROJECT)
bucket = client.bucket(BUCKET_NAME)

# ============================================================
# LEITURA — Bronze
# ============================================================

def ler_bronze(entidade, particionado=True):
    """Lê todos os parquets de uma entidade na Bronze, juntando os anos."""
    prefixo = f"bronze/{entidade}/"
    blobs   = list(bucket.list_blobs(prefix=prefixo))
    if not blobs:
        raise FileNotFoundError(f"Nenhum arquivo encontrado em {prefixo}")

    dfs = []
    for blob in blobs:
        if blob.name.endswith(".parquet"):
            arquivo_local = f"/tmp/{blob.name.split('/')[-1]}"
            blob.download_to_filename(arquivo_local)
            dfs.append(pd.read_parquet(arquivo_local))

    df = pd.concat(dfs, ignore_index=True)
    log.info(f"[SILVER] '{entidade}' lido da Bronze: {len(df)} registros")
    return df

# ============================================================
# DICIONÁRIO — decodificação genérica
# ============================================================

def construir_mapa_dicionario(df_dicionario, id_tabela, nome_coluna):
    """Retorna um dict {codigo: valor_legivel} para uma tabela/coluna específica."""
    filtro = df_dicionario[
        (df_dicionario["id_tabela"] == id_tabela) &
        (df_dicionario["nome_coluna"] == nome_coluna)
    ]
    return dict(zip(filtro["chave"], filtro["valor"]))

def decodificar(df, coluna, mapa, manter_original=True):
    if not mapa:
        log.warning(f"[SILVER] Sem mapa de decodificação para '{coluna}' — mantendo valor original")
        return df
    if manter_original:
        df[f"{coluna}_codigo"] = df[coluna]
    df[coluna] = df[coluna].astype(str).map(mapa).fillna(df[coluna])
    return df

# ============================================================
# TRANSFORMAÇÕES POR ENTIDADE
# ============================================================

def transformar_alunos(df, dicionario):
    log.info("[SILVER] Transformando 'alunos'")
    antes = len(df)

    # Tratamento de nulos/tipos básicos
    df = df.dropna(subset=["id_aluno", "id_municipio", "ano"])
    df["ano"]          = df["ano"].astype(int)
    df["id_municipio"] = df["id_municipio"].astype(str)
    df["proficiencia"] = pd.to_numeric(df["proficiencia"], errors="coerce")

    # Decodificação de campos categóricos usando o dicionário
    for coluna in ["rede", "presenca", "preenchimento_caderno", "alfabetizado"]:
        mapa = construir_mapa_dicionario(dicionario, "alunos", coluna)
        df = decodificar(df, coluna, mapa)

    # Deduplicação pela chave natural
    df = df.drop_duplicates(subset=["id_aluno", "ano"])
    log.info(f"[SILVER] 'alunos': {antes} -> {len(df)} apos limpeza/dedup")
    return df

def transformar_municipio(df, dicionario):
    log.info("[SILVER] Transformando 'municipio'")
    df["ano"]          = df["ano"].astype(int)
    df["id_municipio"] = df["id_municipio"].astype(str)
    mapa_rede = construir_mapa_dicionario(dicionario, "municipio", "rede")
    df = decodificar(df, "rede", mapa_rede)
    df = df.drop_duplicates(subset=["id_municipio", "ano", "rede"])
    return df

def transformar_meta_municipio(df):
    log.info("[SILVER] Transformando 'meta_alfabetizacao_municipio'")
    df["ano"]          = df["ano"].astype(int)
    df["id_municipio"] = df["id_municipio"].astype(str)
    df = df.drop_duplicates(subset=["id_municipio", "ano", "rede"])
    return df

# ============================================================
# QUARENTENA — Registros em Quarentena por Referência
# ============================================================
def separar_quarentena_referencial(df_alunos, df_municipio):
    """Separa alunos cujo id_municipio não existe na tabela de referência."""
    chaves_validas = set(df_municipio["id_municipio"].astype(str))
    mask_valido = df_alunos["id_municipio"].astype(str).isin(chaves_validas)

    df_quarentena = df_alunos[~mask_valido].copy()
    df_quarentena["_quarentena_motivo"] = "id_municipio sem correspondencia na tabela municipio"

    df_pass = df_alunos[mask_valido].copy()

    log.info(f"[SILVER] Quarentena referencial: {len(df_quarentena)} registros isolados")
    log.info(f"[SILVER] Pass: {len(df_pass)} registros seguem para integração")

    return df_pass, df_quarentena

# ============================================================
# INTEGRAÇÃO — Joins
# ============================================================

def integrar(df_alunos, df_municipio, df_meta_municipio):
    log.info("[SILVER] Integrando alunos + municipio + metas")

    df_integrado = df_alunos.merge(
        df_municipio[["id_municipio", "ano", "rede"]].drop_duplicates(),
        on=["id_municipio", "ano", "rede"], how="left", suffixes=("", "_mun")
    )

    df_integrado = df_integrado.merge(
        df_meta_municipio[["id_municipio", "ano", "rede", "taxa_alfabetizacao",
                            "meta_alfabetizacao_2024", "meta_alfabetizacao_2025"]],
        on=["id_municipio", "ano", "rede"], how="left"
    )

    log.info(f"[SILVER] Integração final: {len(df_integrado)} registros")
    return df_integrado

# ============================================================
# ESCRITA — Silver (particionado por ano)
# ============================================================

def salvar_silver(df, entidade):
    for ano in sorted(df["ano"].dropna().unique()):
        df_ano  = df[df["ano"] == ano].copy()
        df_ano["_silver_processed_at"] = PROCESSED_AT
        arquivo = f"/tmp/silver_{entidade}_{ano}.parquet"
        df_ano.to_parquet(arquivo, index=False)

        destino = f"silver/{entidade}/ano={int(ano)}/{entidade}.parquet"
        bucket.blob(destino).upload_from_filename(arquivo)
        log.info(f"[SILVER] {len(df_ano)} registros -> gs://{BUCKET_NAME}/{destino}")

# ============================================================
# EXECUÇÃO
# ============================================================

def main():
    log.info("=" * 60)
    log.info("INICIANDO TRANSFORMAÇÃO SILVER")
    log.info("=" * 60)

    dicionario     = ler_bronze("dicionario")
    df_alunos      = transformar_alunos(ler_bronze("alunos"), dicionario)
    df_municipio   = transformar_municipio(ler_bronze("municipio"), dicionario)

    df_alunos, df_quarentena_alunos = separar_quarentena_referencial(df_alunos, df_municipio)

    if len(df_quarentena_alunos) > 0:
        salvar_silver(df_quarentena_alunos.assign(ano=df_quarentena_alunos["ano"]), "alunos_quarentena")

    df_meta_mun    = transformar_meta_municipio(ler_bronze("meta_alfabetizacao_municipio"))
    
    salvar_silver(df_alunos, "alunos")
    salvar_silver(df_municipio, "municipio")
    salvar_silver(df_meta_mun, "meta_alfabetizacao_municipio")

    df_integrado = integrar(df_alunos, df_municipio, df_meta_mun)
    salvar_silver(df_integrado, "alunos_municipio_integrado")

    log.info("=" * 60)
    log.info("SILVER CONCLUÍDA")
    log.info("=" * 60)

if __name__ == "__main__":
    main()
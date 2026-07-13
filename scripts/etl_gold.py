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
# LEITURA
# ============================================================

def ler_camada(camada, entidade):
    """Lê todos os parquets de uma entidade em uma camada (bronze/silver), juntando anos."""
    prefixo = f"{camada}/{entidade}/"
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
    log.info(f"[GOLD] '{camada}/{entidade}' lido: {len(df)} registros")
    return df

# ============================================================
# VISÃO 1 — Indicador de alfabetização por município
# ============================================================

def construir_indicador_por_municipio(df_integrado):
    log.info("[GOLD] Construindo visao: indicador_por_municipio")

    grupo = df_integrado.groupby(["ano", "id_municipio"]).agg(
        total_alunos=("id_aluno", "count"),
        alfabetizados=("alfabetizado", lambda s: (s == "Sim").sum()),
        proficiencia_media=("proficiencia", "mean"),
        taxa_alfabetizacao_oficial=("taxa_alfabetizacao", "first"),
        meta_2024=("meta_alfabetizacao_2024", "first"),
        meta_2025=("meta_alfabetizacao_2025", "first"),
    ).reset_index()

    grupo["taxa_alfabetizacao_calculada"] = round(
        grupo["alfabetizados"] / grupo["total_alunos"] * 100, 2
    )
    grupo["proficiencia_media"] = round(grupo["proficiencia_media"], 2)

    return grupo

# ============================================================
# VISÃO 2 — Meta vs Resultado (agregado nacional, por ano)
# ============================================================

def construir_meta_vs_resultado_brasil(df_meta_brasil, df_indicador_municipio):
    log.info("[GOLD] Construindo visao: meta_vs_resultado_brasil")

    # Resultado real agregado nacionalmente a partir do indicador por município
    resultado_real = df_indicador_municipio.groupby("ano").agg(
        total_alunos=("total_alunos", "sum"),
        total_alfabetizados=("alfabetizados", "sum"),
    ).reset_index()
    resultado_real["taxa_alfabetizacao_real"] = round(
        resultado_real["total_alfabetizados"] / resultado_real["total_alunos"] * 100, 2
    )

    # Meta oficial (tabela nacional, direto da Bronze — dado pequeno, sem necessidade de Silver própria)
    df_meta_brasil["ano"] = df_meta_brasil["ano"].astype(int)
    metas = df_meta_brasil[[
        "ano", "meta_alfabetizacao_2024", "meta_alfabetizacao_2025",
        "meta_alfabetizacao_2026", "meta_alfabetizacao_2030"
    ]].drop_duplicates(subset=["ano"])

    comparativo = resultado_real.merge(metas, on="ano", how="left")
    comparativo["gap_para_meta_2030"] = round(
        comparativo["meta_alfabetizacao_2030"] - comparativo["taxa_alfabetizacao_real"], 2
    )
    return comparativo

# ============================================================
# VISÃO 3 — Evolução temporal por município (2023 vs 2024 lado a lado)
# ============================================================

def construir_evolucao_temporal(df_indicador_municipio):
    log.info("[GOLD] Construindo visao: evolucao_temporal")

    pivot = df_indicador_municipio.pivot_table(
        index="id_municipio",
        columns="ano",
        values="taxa_alfabetizacao_calculada"
    ).reset_index()

    pivot.columns = ["id_municipio"] + [f"taxa_{int(c)}" for c in pivot.columns[1:]]

    if "taxa_2023" in pivot.columns and "taxa_2024" in pivot.columns:
        pivot["variacao_2023_2024"] = round(pivot["taxa_2024"] - pivot["taxa_2023"], 2)

    return pivot

# ============================================================
# ESCRITA
# ============================================================

def salvar_gold(df, nome_visao, particionar_por_ano=True):
    if particionar_por_ano and "ano" in df.columns:
        for ano in sorted(df["ano"].dropna().unique()):
            df_ano = df[df["ano"] == ano].copy()
            df_ano["_gold_processed_at"] = PROCESSED_AT
            arquivo = f"/tmp/gold_{nome_visao}_{ano}.parquet"
            df_ano.to_parquet(arquivo, index=False)

            destino = f"gold/{nome_visao}/ano={int(ano)}/{nome_visao}.parquet"
            bucket.blob(destino).upload_from_filename(arquivo)
            log.info(f"[GOLD] {len(df_ano)} registros -> gs://{BUCKET_NAME}/{destino}")
    else:
        df = df.copy()
        df["_gold_processed_at"] = PROCESSED_AT
        arquivo = f"/tmp/gold_{nome_visao}.parquet"
        df.to_parquet(arquivo, index=False)

        destino = f"gold/{nome_visao}/{nome_visao}.parquet"
        bucket.blob(destino).upload_from_filename(arquivo)
        log.info(f"[GOLD] {len(df)} registros -> gs://{BUCKET_NAME}/{destino}")

# ============================================================
# EXECUÇÃO
# ============================================================

def main():
    log.info("=" * 60)
    log.info("INICIANDO CONSTRUÇÃO GOLD")
    log.info("=" * 60)

    df_integrado    = ler_camada("silver", "alunos_municipio_integrado")
    df_meta_brasil  = ler_camada("bronze", "meta_alfabetizacao_brasil")

    indicador_municipio = construir_indicador_por_municipio(df_integrado)
    salvar_gold(indicador_municipio, "indicador_por_municipio")

    meta_vs_resultado = construir_meta_vs_resultado_brasil(df_meta_brasil, indicador_municipio)
    salvar_gold(meta_vs_resultado, "meta_vs_resultado_brasil", particionar_por_ano=True)

    evolucao = construir_evolucao_temporal(indicador_municipio)
    salvar_gold(evolucao, "evolucao_temporal_municipio", particionar_por_ano=False)

    log.info("=" * 60)
    log.info("GOLD CONCLUÍDA")
    log.info("=" * 60)

if __name__ == "__main__":
    main()
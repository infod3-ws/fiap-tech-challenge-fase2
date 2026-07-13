import json
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
RUN_TS          = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

client = storage.Client(project=BILLING_PROJECT)
bucket = client.bucket(BUCKET_NAME)

# ============================================================
# LEITURA
# ============================================================

def ler_camada(camada, entidade):
    prefixo = f"{camada}/{entidade}/"
    blobs   = [b for b in bucket.list_blobs(prefix=prefixo) if b.name.endswith(".parquet")]
    if not blobs:
        raise FileNotFoundError(f"Nenhum arquivo encontrado em {prefixo}")

    dfs = []
    for blob in blobs:
        arquivo_local = f"/tmp/{blob.name.split('/')[-1]}"
        blob.download_to_filename(arquivo_local)
        dfs.append(pd.read_parquet(arquivo_local))

    df = pd.concat(dfs, ignore_index=True)
    log.info(f"[DQ] '{camada}/{entidade}' carregado: {len(df)} registros")
    return df

# ============================================================
# MOTOR DE REGRAS — genérico, mesmo padrão dos scripts de ETL
# ============================================================

resultados = []

def registrar(nome_check, camada, entidade, ok, critico, detalhe):
    status = "PASS" if ok else ("FAIL" if critico else "WARN")
    resultados.append({
        "check": nome_check, "camada": camada, "entidade": entidade,
        "status": status, "critico": critico, "detalhe": detalhe,
        "verificado_em": RUN_TS,
    })
    log_fn = log.info if ok else (log.error if critico else log.warning)
    log_fn(f"[DQ] {status} | {camada}/{entidade} | {nome_check} | {detalhe}")

def checar_nulos(df, colunas, camada, entidade, critico=True):
    for col in colunas:
        nulos = df[col].isnull().sum()
        registrar(f"not_null:{col}", camada, entidade, nulos == 0, critico,
                   f"{nulos} nulos de {len(df)} registros")

def checar_duplicidade(df, chave, camada, entidade, critico=True):
    dups = df.duplicated(subset=chave).sum()
    registrar(f"unique:{'+'.join(chave)}", camada, entidade, dups == 0, critico,
               f"{dups} duplicatas encontradas (chave={chave})")

def checar_integridade_referencial(df_filho, coluna_fk, df_pai, coluna_pk,
                                    camada, entidade, critico=False): # gap conhecido na fonte, ver README
    chaves_pai   = set(df_pai[coluna_pk].astype(str))
    chaves_filho = set(df_filho[coluna_fk].astype(str))
    orfaos       = chaves_filho - chaves_pai
    registrar(f"referential_integrity:{coluna_fk}", camada, entidade, len(orfaos) == 0, critico,
               f"{len(orfaos)} valores de '{coluna_fk}' sem correspondencia em '{coluna_pk}'")

def checar_consistencia_soma(valor_a, valor_b, nome_check, camada, entidade,
                              tolerancia=0, critico=True):
    diferenca = abs(valor_a - valor_b)
    registrar(nome_check, camada, entidade, diferenca <= tolerancia, critico,
               f"silver={valor_a} | gold={valor_b} | diferenca={diferenca}")

# ============================================================
# EXECUÇÃO DAS VALIDAÇÕES
# ============================================================

def main():
    log.info("=" * 60)
    log.info("INICIANDO VALIDAÇÃO DE QUALIDADE DE DADOS")
    log.info("=" * 60)

    # --- Carrega as tabelas necessárias ---
    df_alunos_silver    = ler_camada("silver", "alunos")
    df_municipio_silver = ler_camada("silver", "municipio")
    df_indicador_gold   = ler_camada("gold", "indicador_por_municipio")

    # --- 1. Nulos em colunas-chave ---
    checar_nulos(df_alunos_silver, ["id_aluno", "id_municipio", "ano"],
                 "silver", "alunos", critico=True)
    checar_nulos(df_municipio_silver, ["id_municipio", "ano"],
                 "silver", "municipio", critico=True)

    # --- 2. Duplicidade ---
    checar_duplicidade(df_alunos_silver, ["id_aluno", "ano"],
                        "silver", "alunos", critico=True)
    checar_duplicidade(df_municipio_silver, ["id_municipio", "ano", "rede"],
                        "silver", "municipio", critico=True)

    # --- 3. Integridade referencial: todo aluno pertence a um município cadastrado ---
    checar_integridade_referencial(
        df_alunos_silver, "id_municipio",
        df_municipio_silver, "id_municipio",
        "silver", "alunos_para_municipio", critico=False # gap conhecido na fonte, ver README
    )

    # --- 4. Consistência entre Silver e Gold ---
    total_alunos_silver = len(df_alunos_silver)
    total_alunos_gold   = df_indicador_gold["total_alunos"].sum()
    checar_consistencia_soma(
        total_alunos_silver, total_alunos_gold,
        "consistencia_total_alunos_silver_vs_gold",
        "gold", "indicador_por_municipio", tolerancia=0, critico=True
    )

    # ============================================================
    # RELATÓRIO FINAL
    # ============================================================

    df_relatorio = pd.DataFrame(resultados)
    total   = len(df_relatorio)
    passou  = (df_relatorio["status"] == "PASS").sum()
    falhou  = (df_relatorio["status"] == "FAIL").sum()
    alertou = (df_relatorio["status"] == "WARN").sum()
    score   = round(passou / total * 100, 1)

    log.info("=" * 60)
    log.info(f"SCORE DE QUALIDADE: {score}%  (PASS={passou} FAIL={falhou} WARN={alertou} TOTAL={total})")
    log.info("=" * 60)

    # Salva o relatório no bucket como evidência (JSON, fácil de anexar no README)
    arquivo_local = f"/tmp/quality_report_{RUN_TS}.json"
    with open(arquivo_local, "w") as f:
        json.dump({
            "executado_em": RUN_TS,
            "score_pct": score,
            "total_checks": total,
            "pass": int(passou),
            "fail": int(falhou),
            "warn": int(alertou),
            "detalhes": resultados,
        }, f, indent=2, default=str)

    destino = f"quality/reports/quality_report_{RUN_TS}.json"
    bucket.blob(destino).upload_from_filename(arquivo_local)
    log.info(f"[DQ] Relatorio salvo em gs://{BUCKET_NAME}/{destino}")

    if falhou > 0:
        raise Exception(f"[DQ] {falhou} verificacao(oes) critica(s) falharam. Revisar antes de prosseguir.")

if __name__ == "__main__":
    main()
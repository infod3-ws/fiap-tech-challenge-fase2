# Camada Bronze — Dados Brutos

Dados extraídos das fontes originais sem transformação, apenas com
metadados de auditoria adicionados. Histórico completo preservado.

**Localização real (Cloud Storage):**
`gs://fiap-tc2-datalake/bronze/{entidade}/ano={ano}/`

**Script responsável:** [`../scripts/etl_bronze.py`](../scripts/etl_bronze.py)

## Entidades

| Entidade | Origem | Particionamento |
|---|---|---|
| `uf` | `basedosdados.br_inep_avaliacao_alfabetizacao.uf` | ano |
| `meta_alfabetizacao_brasil` | idem `.meta_alfabetizacao_brasil` | ano |
| `meta_alfabetizacao_uf` | idem `.meta_alfabetizacao_uf` | ano |
| `meta_alfabetizacao_municipio` | idem `.meta_alfabetizacao_municipio` | ano |
| `municipio` | idem `.municipio` | ano |
| `alunos` | idem `.alunos` | ano |
| `dicionario` | idem `.dicionario` | sem partição |
| `alunos_streaming` | Simulação via Pub/Sub | data_ingestao |

## Metadados de auditoria adicionados

`_ingestion_timestamp`, `_ingestion_date`, `_source_entity`, `_source_query`, `_record_hash`

> Os arquivos Parquet reais não são versionados neste repositório — apenas
> o código que os gera. Isso evita inflar o histórico do Git com dados
> binários; a rastreabilidade acontece via metadados de auditoria em
> cada registro e via versionamento do código de ingestão.
# Camada Gold — Camada Analítica

Datasets agregados, prontos para consumo em dashboards, análises
estatísticas e treinamento de modelos de ML.

**Localização real (Cloud Storage):**
`gs://fiap-tc2-datalake/gold/{visao}/`

**Script responsável:** [`../scripts/etl_gold.py`](../scripts/etl_gold.py)

## Visões geradas

| Visão | Descrição | Particionamento |
|---|---|---|
| `indicador_por_municipio` | Taxa de alfabetização calculada, proficiência média, meta oficial, por município/ano | ano |
| `meta_vs_resultado_brasil` | Comparativo nacional entre taxa real e metas pactuadas até 2030, incluindo `gap_para_meta_2030` | ano |
| `evolucao_temporal_municipio` | Taxa de alfabetização 2023 vs. 2024 lado a lado, com `variacao_2023_2024`, por município | sem partição |

## Aplicações previstas

Ver seção 7 ("Aplicação em IA") do [README principal](../README.md).
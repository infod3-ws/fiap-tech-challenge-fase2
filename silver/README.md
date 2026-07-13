# Camada Silver — Dados Tratados e Integrados

Dados limpos, deduplicados, com tipos padronizados e códigos decodificados
via tabela de dicionário. Integração entre `alunos`, `municipio` e
`meta_alfabetizacao_municipio`.

**Localização real (Cloud Storage):**
`gs://fiap-tc2-datalake/silver/{entidade}/ano={ano}/`

**Script responsável:** [`../scripts/etl_silver.py`](../scripts/etl_silver.py)

## Entidades geradas

- `alunos` — decodificado, deduplicado por `id_aluno`+`ano`
- `municipio` — decodificado, deduplicado
- `meta_alfabetizacao_municipio` — deduplicado
- `alunos_municipio_integrado` — join final, base para a camada Gold
- `alunos_quarentena` — registros com `id_municipio` sem correspondência
  referencial (ver seção 8 do README principal)

## Transformações aplicadas

Tratamento de nulos, padronização de tipos, decodificação via dicionário,
deduplicação por chave natural, integração via join, isolamento em
quarentena de registros referencialmente inconsistentes.
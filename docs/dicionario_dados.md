# Dicionário de Dados

Baseado na tabela `dicionario` da fonte `br_inep_avaliacao_alfabetizacao`,
usada pelo `etl_silver.py` para decodificar campos categóricos.

## Entidade: alunos

| Coluna | Tipo | Descrição | Valores possíveis |
|---|---|---|---|
| `id_aluno` | string | Identificador único do aluno avaliado | — |
| `id_municipio` | string | Código IBGE do município (7 dígitos) | — |
| `ano` | integer | Ano de referência da avaliação | 2023, 2024 |
| `rede` | string | Rede de ensino do aluno | Federal, Estadual, Municipal, Privada |
| `serie` | string | Série/ano escolar do aluno avaliado | 2º ano do Ensino Fundamental (valor único na base) |
| `alfabetizado` | string | Se o aluno atingiu o ponto de corte de 743 pontos (Saeb) | Sim, Não |
| `proficiencia` | float | Pontuação obtida na escala de proficiência do Saeb | 0–1000 (aprox.) |
| `presenca` | string | Se o aluno esteve presente na avaliação | Ausente, Presente |
| `preenchimento_caderno` | string | Situação de preenchimento do caderno de respostas | Prova não preenchida, Prova preenchida |

### Decodificação — `alunos.rede`
| Código | Valor |
|---|---|
| 1 | Federal |
| 2 | Estadual |
| 3 | Municipal |
| 4 | Privada |

### Decodificação — `alunos.presenca`
| Código | Valor |
|---|---|
| 0 | Ausente |
| 1 | Presente |

### Decodificação — `alunos.preenchimento_caderno`
| Código | Valor |
|---|---|
| 0 | Prova não preenchida |
| 1 | Prova preenchida |

### Decodificação — `alunos.alfabetizado`
| Código | Valor |
|---|---|
| 0 | Não |
| 1 | Sim |

### Decodificação — `alunos.serie`
| Código | Valor |
|---|---|
| 2 | 2° ano do Ensino Fundamental |

> **Nota:** a base contém apenas um valor para `serie`, o que é esperado —
> o Compromisso Nacional Criança Alfabetizada e o Indicador Criança
> Alfabetizada são especificamente voltados à avaliação de alfabetização
> ao final do 2º ano do Ensino Fundamental (ver Seção 1 deste README).
> Essa coluna foi mantida no schema por consistência com a fonte original,
> mesmo sem variação de valores.

## Entidade: municipio / uf

| Coluna | Tipo | Descrição |
|---|---|---|
| `id_municipio` | string | Código IBGE do município |
| `sigla_uf` | string | Sigla da unidade federativa |
| `rede` | string | Rede de ensino (agregação usada nas metas/resultados por município) |
| `serie` | string | Série/ano escolar de referência |
| `taxa_alfabetizacao` | float | Percentual de alunos alfabetizados, agregado |
| `media_portugues` | float | Proficiência média em Língua Portuguesa |
| `proporcao_aluno_nivel_0` a `nivel_8` | float | Distribuição percentual dos alunos por nível de proficiência |

### Decodificação — `municipio.rede`
| Código | Valor |
|---|---|
| 0 | Total (Federal, Estadual, Municipal e Privada) |
| 1 | Federal |
| 2 | Estadual |
| 3 | Municipal |
| 4 | Privada |
| 5 | Pública (Estadual e Municipal) |
| 6 | Pública (Federal, Estadual e Municipal) |

> **Nota:** `municipio.rede` tem uma escala mais ampla que `alunos.rede`
> (inclui agregações como "Total" e "Pública"), pois representa resultados
> consolidados por município, não o registro individual de um aluno.
> Isso foi considerado na integração feita em `etl_silver.py`, que faz o
> join por `id_municipio + ano + rede` usando os códigos compatíveis
> entre as duas tabelas.

## Colunas de metadados de auditoria (adicionadas pelo pipeline)

| Coluna | Camada | Descrição |
|---|---|---|
| `_ingestion_timestamp` | Bronze | Timestamp UTC da extração |
| `_ingestion_date` | Bronze | Data da extração (formato YYYY-MM-DD) |
| `_source_entity` | Bronze | Nome da entidade de origem |
| `_source_query` | Bronze | Query SQL usada na extração via BigQuery |
| `_record_hash` | Bronze | Hash MD5 do registro, para auditoria de integridade |
| `_silver_processed_at` | Silver | Timestamp do processamento de limpeza |
| `_quarentena_motivo` | Silver | Motivo do isolamento (quando aplicável) |
| `_gold_processed_at` | Gold | Timestamp da agregação analítica |
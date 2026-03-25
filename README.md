# Arquitetura de Dados — YouTube Creators Pipeline

Pipeline para ingestão e atualização contínua de dados de criadores do YouTube e suas publicações, coletados a partir de APIs públicas e Wikipedia.

---

## Sumário

1. [Visão Geral](#visão-geral)
2. [Orquestrador](#orquestrador)
3. [Modelagem de Dados](#modelagem-de-dados)
4. [Extração de Dados](#extração-de-dados)
5. [Etapas do Pipeline](#etapas-do-pipeline)
6. [Monitoramento e Qualidade](#monitoramento-e-qualidade)
7. [Boas Práticas](#boas-práticas)

---

## Visão Geral

```
Wikipedia API ──┐
                ├──▶ [Bronze] ──▶ [Silver] ──▶ [Gold] ──▶ Consumo
YouTube API ────┘
```

A arquitetura segue o padrão **Medallion (Bronze / Silver / Gold)** no Databricks com tabelas Delta Lake, garantindo rastreabilidade completa desde a fonte até o dado analítico final. Por que? Porque a Bronze imutável é o seu seguro (se uma transformação Silver tiver bug, você reprocessa a partir dos dados brutos sem precisar chamar a API de novo).

| Camada | Descrição |
|--------|-----------|
| Bronze | Dados brutos das APIs, sem transformação, append-only |
| Silver | Dados limpos, tipados e enriquecidos |
| Gold   | Agregações analíticas prontas para consumo |

---

## Orquestrador

**Escolha: Databricks Workflows**

Por já estar no ecossistema Databricks, o Workflows elimina a necessidade de infraestrutura adicional e se integra nativamente com os notebooks e tabelas Delta do projeto.

Alternativas consideradas:

| Orquestrador | Motivo de não escolher |
|---|---|
| Apache Airflow | Exige infraestrutura separada; overhead desnecessário para este escopo |
| AWS Step Functions | Vendor lock-in AWS; menos integração nativa com Spark |
| Prefect / Dagster | Ótimas opções, mas requerem setup adicional fora do Databricks |

O pipeline roda em dois modos:

- **Carga inicial (backfill)**: execução única, coleta histórico completo
- **Atualização incremental**: agendado diariamente via cron no Databricks Workflows

---

## Modelagem de Dados

### Diagrama de Relacionamento

```
┌─────────────────────┐         ┌──────────────────────────┐
│   creators          │         │   posts                  │
├─────────────────────┤         ├──────────────────────────┤
│ PK  user_id         │────┐    │ PK  post_id              │
│     wiki_page        │    └───▶│ FK  user_id              │
│     channel_title    │         │     title                │
│     subscribers      │         │     published_at         │
│     total_views      │         │     likes                │
│     country          │         │     views                │
│     created_at       │         │     comments_count       │
│     updated_at       │         │     duration_seconds     │
└─────────────────────┘         │     ingested_at          │
                                 └──────────────────────────┘
          │
          │
          ▼
┌─────────────────────┐
│   creators_monthly  │
├─────────────────────┤
│ FK  user_id         │
│     year_month      │
│     num_posts       │
│     total_likes     │
│     total_views     │
└─────────────────────┘
```

### Documentação das Tabelas

#### `silver.creators`

Dados cadastrais de cada criador, atualizados a cada execução.

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `user_id` | string | Identificador do canal no YouTube (PK) |
| `wiki_page` | string | Nome da página na Wikipedia |
| `channel_title` | string | Nome do canal |
| `subscribers` | long | Número de inscritos |
| `total_views` | long | Total de visualizações do canal |
| `country` | string | País do criador |
| `created_at` | timestamp | Data de criação do canal |
| `updated_at` | timestamp | Última atualização do registro |

#### `silver.posts`

Publicações de cada criador. Append-only com snapshot diário de métricas.

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `post_id` | string | ID do vídeo no YouTube (PK) |
| `user_id` | string | FK para `creators` |
| `title` | string | Título do vídeo |
| `published_at` | timestamp | Data de publicação |
| `likes` | long | Número de likes |
| `views` | long | Número de visualizações |
| `comments_count` | long | Número de comentários |
| `duration_seconds` | integer | Duração em segundos |
| `ingested_at` | timestamp | Data de ingestão do registro |

#### `gold.creators_monthly`

Agregação mensal por criador, usada para análises de tendência.

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `user_id` | string | FK para `creators` |
| `year_month` | string | Mês no formato `yyyy-MM` |
| `num_posts` | integer | Número de publicações no mês |
| `total_likes` | long | Soma de likes do mês |
| `total_views` | long | Soma de visualizações do mês |

---

## Extração de Dados

### Fontes

| Fonte | Dados extraídos | Método |
|---|---|---|
| Wikipedia API | `user_id` do YouTube por criador | REST API (`action=parse`) |
| YouTube Data API v3 | Metadados do canal e vídeos | REST API |

### Carga Inicial

```
1. Lista de wiki_pages definida manualmente (seed)
        ↓
2. Wikipedia API → resolve wiki_page para youtube user_id
        ↓
3. YouTube API (channels) → metadados do canal
        ↓
4. YouTube API (search + videos) → histórico completo de vídeos
        ↓
5. Grava Bronze → transforma → grava Silver
```

A carga inicial busca todo o histórico disponível de vídeos de cada canal, iterando via `pageToken` até esgotar os resultados da API.

### Atualização Incremental

A cada execução diária, o pipeline busca apenas publicações novas:

```python
# Busca vídeos publicados após a última ingestão
published_after = last_ingested_at.strftime("%Y-%m-%dT%H:%M:%SZ")

params = {
    "part": "id",
    "channelId": channel_id,
    "publishedAfter": published_after,
    "order": "date",
    "maxResults": 50,
}
```

Métricas de vídeos já existentes (likes, views) são atualizadas via upsert com `MERGE INTO` do Delta Lake:

```sql
MERGE INTO silver.posts AS target
USING updates AS source
ON target.post_id = source.post_id
WHEN MATCHED THEN UPDATE SET
    target.likes    = source.likes,
    target.views    = source.views,
    target.comments_count = source.comments_count
WHEN NOT MATCHED THEN INSERT *
```

### Cotas da YouTube Data API v3

| Operação | Custo (unidades) |
|---|---|
| `channels.list` | 1 por canal |
| `search.list` | 100 por chamada |
| `videos.list` | 1 por chamada |
| **Cota diária gratuita** | **10.000 unidades** |

Para um volume de ~50 criadores com atualização diária, o consumo fica bem abaixo da cota gratuita.

---

## Etapas do Pipeline

```
┌──────────────────────────────────────────────────────────────┐
│                     Databricks Workflow                       │
│                                                              │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────────┐  │
│  │  notebook   │    │  notebook   │    │    notebook     │  │
│  │  1_extract  │───▶│ 2_transform │───▶│    3_load       │  │
│  │  _raw       │    │  _silver    │    │    _gold        │  │
│  └─────────────┘    └─────────────┘    └─────────────────┘  │
│         │                                       │            │
│         ▼                                       ▼            │
│    bronze.*                               gold.*             │
│    (raw JSON)                          (agregações)          │
└──────────────────────────────────────────────────────────────┘
```

### Notebook 1 — Extração (Bronze)

- Lê `creators_scrape_wiki` para obter a lista de criadores
- Chama Wikipedia API para resolver `wiki_page` → `user_id`
- Chama YouTube API para buscar metadados de canais e vídeos
- Grava resultado bruto em `bronze.creators_raw` e `bronze.posts_raw` (append com timestamp)

### Notebook 2 — Transformação (Silver)

- Lê Bronze, valida e limpa os dados
- Tipagem explícita de todas as colunas
- Remove duplicatas e registros inválidos
- Upsert via `MERGE INTO` nas tabelas Silver

### Notebook 3 — Agregação (Gold)

- Lê Silver e calcula agregações mensais por criador
- Garante que todos os meses do intervalo existam (cross join com calendário)
- Preenche meses sem posts com `num_posts = 0`
- Sobrescreve tabelas Gold com `overwrite`

---

## Monitoramento e Qualidade

### Qualidade dos Dados

Em cada notebook Silver, validações são executadas antes do upsert:

```python
# Exemplos de checks de qualidade
assert df.filter(F.col("user_id").isNull()).count() == 0, "user_id nulo encontrado"
assert df.filter(F.col("likes") < 0).count() == 0, "likes negativo encontrado"
assert df.filter(F.col("published_at") > F.current_timestamp()).count() == 0, "data futura encontrada"
```

Se qualquer check falhar, o notebook levanta uma exceção e o Workflow marca a execução como falha — sem gravar dados inconsistentes.

### Monitoramento do Pipeline

| O que monitorar | Como |
|---|---|
| Falha em qualquer notebook | Alerta por e-mail no Databricks Workflows |
| Volume de registros ingeridos | Log no notebook + métrica no Delta history |
| Erros 403 / 429 nas APIs | Contador de erros logado por execução |
| Drift de schema | `mergeSchema` desativado — schema fixo, falha explícita se mudar |

### Observabilidade

```python
# Ao final de cada notebook, loga um resumo da execução
print({
    "notebook": "2_transform_silver",
    "run_date": date.today().isoformat(),
    "creators_updated": creators_df.count(),
    "posts_inserted": new_posts,
    "posts_updated": updated_posts,
    "errors": error_count,
})
```

---

## Boas Práticas

### Gitflow

```
main
 └── develop
      ├── feature/ingestao-wikipedia
      ├── feature/youtube-api-integration
      └── hotfix/fix-403-user-agent
```

- `main`: código em produção, protegido — merge apenas via PR aprovado
- `develop`: branch de integração
- `feature/*`: uma branch por funcionalidade ou notebook
- `hotfix/*`: correções urgentes em produção

### Princípios de Engenharia

**Idempotência**: qualquer notebook pode ser reexecutado sem duplicar dados — garantido pelo `MERGE INTO` e pelo `overwrite` na Gold.

**Separação de responsabilidades**: cada notebook tem uma única responsabilidade (extrair, transformar ou agregar). Nenhum notebook lê e já grava Gold diretamente.

**Configuração centralizada**: credenciais (API keys) armazenadas em Databricks Secrets, nunca hardcoded. Parâmetros como datas e nomes de tabelas em um notebook de configuração importado pelos demais.

**Schema explícito**: todos os DataFrames têm schema definido com `StructType`. Nenhum `inferSchema=True` em produção.

**Bronze imutável**: a camada Bronze é append-only e nunca é modificada. Permite reprocessar Silver e Gold a qualquer momento a partir dos dados brutos originais.
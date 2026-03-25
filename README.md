# Arquitetura de Dados — YouTube Creators Pipeline

Pipeline para ingestão e atualização contínua de dados de criadores de conteúdo e suas publicações, coletados a partir de APIs e Wikipedia.

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

Vou utilizar como base o processo de Wikipedia e Youtube realizado anteriormente mas sabendo que podemos ter diversas origens, para complemento das informações: Instagram, TikTok, além de outras Wikis de Fandom, Google Trends, etc, que serão consideradas abaixo como 'Outras Origens'.

```
Wikipedia e YouTube ───┐
                       ─▶ [Bronze] ──▶ [Silver] ──▶ [Gold] ──▶ Consumo
Outras origens ────────┘
```

A arquitetura segue o padrão **Medalhão (Bronze / Silver / Gold)** no Databricks com tabelas Delta Lake.

A camada Bronze teria os dados brutos e mantidos imutáveis, garantindo dados não manipulados: se uma transformação Silver tiver bug, posso reprocessar a partir dos dados brutos sem precisar chamar a API / relizar o scrapping de novo.

No caso do Wikipedia via scraping, eu guardaria o HTML bruto (uma coluna como string) com metadados de rastreabilidade:
```
{
    "wiki_page": "Felipe_Neto",
    "url": "https://en.wikipedia.org/wiki/Felipe_Neto",
    "html": "<html>...</html>",
    "ingested_at": "2024-04-15T10:23:00"
}
```

Na camada Silver, os dados de cada origem seriam tratados separadamente: extração informações interessantes a partir do HTML com regex, deduplicação das linhas, padronização das colunas, validações das urls extraídas (como a url do YouTube extraída do Wikipedia), alguns joins de enriquecimento (com os dados extraídos do Wikipedia).

Além disso, teria duas estratégias para essa camada: os dados cadastrais seriam atualizados com MERGE INTO (UPSERT), já que nesse caso o estado atual é suficiente; e os dados de métricas de engajamento (likes, views, etc) com snapshot diário (append), para conseguir acessar as variações ao longo do tempo e construir boas soluções enquanto os vídeos estão em alta.

Na camada Gold, os dados seriam agredados e modelados: por exemplo, dados de Youtube unidos com de outras redes sociais, os groupby realizados anteriormente, etc.


| Camada | Descrição |
|--------|-----------|
| Bronze | Dados brutos, sem transformação, append para manter histórico |
| Silver | Dados limpos, tipados e enriquecidos |
| Gold   | Agregações analíticas prontas para consumo |

---

## Orquestrador

**Escolha: Databricks Workflows**

Por já estar no ecossistema Databricks, o Workflows elimina a necessidade de infraestrutura adicional e se integra nativamente com os notebooks e tabelas Delta do projeto.

Alternativas consideradas:

| Orquestrador | Motivo de não escolher |
|---|---|
| Apache Airflow | Exige infraestrutura separada; overhead desnecessário |
| AWS Step Functions | Da AWS; menos integração nativa |

O pipeline roda em dois modos:

- **Carga inicial**: execução única, coleta de histórico
- **Atualização incremental**: agendado diariamente via cron no Databricks Workflows

---

## Modelagem de Dados

### Diagrama de Relacionamento

```
SILVER
─────────────────────────────────────────────────────────────────────────────

┌──────────────────────────┐         ┌──────────────────────────────┐
│   creators               │         │   posts                      │
│   (MERGE INTO)           │         │   (MERGE INTO)               │
├──────────────────────────┤         ├──────────────────────────────┤
│ PK  creator_id           │────┐    │ PK  post_id                  │
│     wiki_page            │    └───▶│ FK  creator_id               │
│     channel_title        │         │     title                    │
│     country              │         │     published_at             │
│     created_at           │         │     duration_seconds         │
│     updated_at           │         │     ingested_at              │
└──────────────────────────┘         └──────────────────────────────┘
           │                                        │
           ▼                                        ▼
┌─────────────────────────────┐         ┌─────────────────────────────┐
│      creators_metrics       │         │   posts_metrics             │
│      (APPEND diário)        │         │   (APPEND diário)           │
├─────────────────────────────┤         ├─────────────────────────────┤
│ FK  creator_id              │─────▶   │ FK  post_id                 │
│     subscribers             │         │ FK  creator_id              │
│     total_views             │         │     likes                   │
│     snapshot_date           │         │     views                   │
└─────────────────────────────┘         │     comments_count          │
           │                            │     snapshot_date           │
           │                            └─────────────────────────────┘
           │                                        │
           └────────────────┤───────────────────────┘
                            │
GOLD                        ▼
─────────────────────────────────────────────────────────────────────────────

                ┌─────────────────────────┐
                │   creators_monthly      │
                │   (OVERWRITE)           │
                ├─────────────────────────┤
                │ FK  creator_id          │
                │     year_month          │
                │     num_posts           │
                │     total_likes         │
                │     total_views         │
                └─────────────────────────┘
```

### Documentação das Tabelas

#### `silver.creators`

Dados cadastrais de cada criador, atualizados a cada execução. Podemos criar uma tabela dessa para cada rede social de origem ou uma tabela completa apontando para a todas as redes desse mesmo criador. Para simplificar, fiz com um exemplo do Youtube:

| Coluna | Tipo | Descrição |
|---|---|---|
| creator_id | string | Identificador do criador (PK) |
| yt_user | string | Identificador do canal no YouTube (PK) |
| channel_title | string | Nome do canal no YouTube |
| wiki_page | string | Nome da página na Wikipedia |
| country | string | País do criador |
| created_at | timestamp | Data de criação do canal |
| updated_at | timestamp | Última atualização do registro |


#### `silver.creators_metrics`

Dados com métricas de cada rede social, para acompanhar os números de cada canal.

| Coluna | Tipo | Descrição |
|---|---|---|
| yt_user | string | Identificador do canal no YouTube (FK) |
| subscribers | long | Número de inscritos no YouTube |
| total_views | long | Total de visualizações do canal |
| snapshot_date | timestamp | Data do último snapshot |

#### `silver.posts`

Dados cadastrais de cada publicação, atualizados a cada execução.

| Coluna | Tipo | Descrição |
|---|---|---|
| creator_id | string | Identificador do criador (FK) |
| yt_user | string | Identificador do canal no YouTube (FK) |
| post_id | string | ID do vídeo no YouTube (PK) |
| title | string | Título do vídeo |
| published_at | timestamp | Data de publicação |
| ingested_at | timestamp | Data de ingestão do registro |

#### `silver.posts_metrics`

Dados com métricas de cada post, para acompanhar os números ao longo do tempo.

| Coluna | Tipo | Descrição |
|---|---|---|
| post_id | string | Identificador do post (FK) |
| likes | long | Número de likes |
| views | long | Número de visualizações |
| tags | long | Tags utilizadas |
| comments | long | Número de comentários |
| snapshot_date | timestamp | Data do último snapshot |

#### `gold.creators_monthly`

Agregação mensal por criador, usada para análises de tendência.

| Coluna | Tipo | Descrição |
|---|---|---|
| creator_id | string | Identificador do criador (PK) |
| yt_user | string | Identificador do canal no YouTube (PK) |
| year_month | string | Mês no formato yyyy-MM |
| num_posts | integer | Número de publicações no mês |
| total_likes | long | Soma de likes do mês |
| total_views | long | Soma de visualizações do mês |

---

## Extração de Dados

### Fontes

| Fonte | Dados extraídos | Método |
|---|---|---|
| Wikipedia | `creator_id` do YouTube por criador | Webscrapping / REST API (`action=parse`) |
| YouTube Data API v3 | Metadados do canal e vídeos | REST API |

### Carga Inicial

A carga inicial seria realizada a partir da lista de wiki_pages, buscando a partir disso os ids do YouTube. Com a API do Youtube eu buscaria os metadados do canal e o histórico completo de vídeos e suas informações: a carga inicial busca todo o histórico disponível de vídeos de cada canal, paginando até esgotar os resultados da API.

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

### Cotas da [API do YouTube](https://developers.google.com/youtube/v3/getting-started)

| Operação | Custo (unidades) |
|---|---|
| `channels.list` | 1 por canal |
| `search.list` | 100 por chamada |
| `videos.list` | 1 por chamada |
| Cota diária gratuita | 10.000 unidades |

---

## Etapas do Pipeline

Notebook 1 (extração e salva na Bronze) -> Notebook 3 (lê da Bronze, transforma e salva na Silver) -> Notebook 3 (lê da Silver, agrega e salva na Gold)

### Notebook 1 — Extração (Bronze)

- Lê `creators_scrape_wiki` para obter a lista de criadores
- Chama Wikipedia API para resolver `wiki_page` → `creator_id`
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
assert df.filter(F.col("creator_id").isNull()).count() == 0, "creator_id encontrado"
assert df.filter(F.col("likes") < 0).count() == 0, "likes negativos encontrado"
assert df.filter(F.col("published_at") > F.current_timestamp()).count() == 0, "data futura encontrada"
```

Se qualquer check falhar, o notebook levanta uma exceção e o Workflow marca a execução como falha — sem gravar dados inconsistentes.

### Monitoramento do Pipeline

| O que monitorar | Como |
|---|---|
| Falha em qualquer notebook | Alerta por e-mail no Databricks Workflows |
| Volume de registros ingeridos | Log no notebook + métrica no Delta history |   -> que que é Delta history?????
| Erros nas APIs | Contador de erros logado por execução |
| Schema | Schema fixo, falha explícita se mudar |

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

- `main`: código em produção, protegido — merge apenas via PR aprovado
- `develop`: branch de integração
- `homolog`: branch de testes, se necessário
- `feature/*`: uma branch por funcionalidade ou notebook
- `hotfix/*`: correções urgentes em produção

### Princípios de Engenharia

**Idempotência**: qualquer notebook pode ser reexecutado sem duplicar dados — garantido pelo `MERGE INTO` e pelo `overwrite` na Gold.

**Separação de responsabilidades**: cada notebook tem uma única responsabilidade (extrair, transformar ou agregar). Nenhum notebook lê e já grava Gold diretamente.

**Configuração centralizada**: credenciais (API keys) armazenadas em Databricks Secrets, nunca hardcoded. Parâmetros como datas e nomes de tabelas em um notebook de configuração importado pelos demais.

**Schema explícito**: todos os DataFrames têm schema definido com `StructType`. Nenhum `inferSchema=True` em produção.

**Bronze imutável**: a camada Bronze é append-only e nunca é modificada. Permite reprocessar Silver e Gold a qualquer momento a partir dos dados brutos originais.

**SOLID**: ?????
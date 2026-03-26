# Arquitetura de Dados — Creators Pipeline

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

Vou utilizar como base o processo de Wikipedia e Youtube realizado anteriormente mas sabendo que podemos ter diversas origens, para complemento das informações: Instagram, TikTok, além de outras Wikis de Fandom, Google Trends.

A arquitetura seguiria o padrão **Medalhão (Bronze / Silver / Gold)** no Databricks com tabelas Delta Lake.

![Arquietura](imagens/arquitetura.png)

A camada Bronze teria os dados brutos e mantidos imutáveis, garantindo dados não manipulados: se uma transformação Silver tiver bug, posso reprocessar a partir dos dados brutos sem precisar chamar a API / realizar o scrapping de novo. No caso dos resultados via scrapping e API, eu guardaria o HTML bruto ou o json de retorno do request como uma coluna (string) com metadados de rastreabilidade:
```
{
    "wiki_page": "Felipe_Neto",
    "url": "https://en.wikipedia.org/wiki/Felipe_Neto",
    "html": "<html>...</html>",
    "ingested_at": "2024-04-15T10:23:00"
}
```

O particionamento da bronze poderia ser por canal e data de ingestão, Já que, um reprocessamento é sempre por canal: se o parser do Instagram tiver bug, dá para reprocessar source=instagram/ inteiro, não vasculhar todas as datas misturadas com outros canais. Não acredito que 'criador' seja uma boa partição: se temos 300 criadores, partições de alta cardinalidade fragmentam demais os arquivos e degradam performance.
```
bronze/
├── source=youtube/
│   ├── ingestion_date=2024-04-15/
│   └── ingestion_date=2024-04-16/
├── source=wikipedia/
│   └── ingestion_date=2024-04-15/
└── source=instagram/
    └── ingestion_date=2024-04-15/
```

Na camada Silver, os dados da bronze seriam transformados em tabelas: extração informações / colunas a partir do HTML com regex, extração informações / colunas a partir do Json da API, tipagem dessas colunas, deduplicação das linhas, validações das urls extraídas (como a url do YouTube extraída do Wikipedia), alguns joins de enriquecimento (com os dados extraídos do Wikipedia), aplicação de dataquality (testes de unicidade e completude). Os dados seriam atualizados por Upsert.

O particionamento seria o mesmo da bronze:
```
silver/
├── source=youtube/
│   └── ingestion_date=2024-04-15/
|       └── silver.youtube_canais
|       └── silver.youtube_videos
├── source=wikipedia/
│   └── ingestion_date=2024-04-15/
|       └── silver.wikipedia_criadores
└── source=instagram/
    └── ingestion_date=2024-04-15/
        └── silver.instagram_contas
        └── silver.instagram_posts

```

Na camada Gold, os dados seriam agredados e modelados e teria duas estratégias para essa camada: os dados cadastrais seriam atualizados com MERGE INTO (UPSERT), já que nesse caso o estado atual é suficiente, e particionados por data de ingestão; e os dados de métricas de engajamento (likes, views, etc) com snapshot diário (append), para conseguir acessar as variações ao longo do tempo e construir boas soluções enquanto os vídeos estão em alta, e particionados pela data do snapshot.

---

## Orquestrador

**Escolha: Databricks Workflows**

Eu tenho bastante experiência na AWS e costumo utilizar o glue workflow para pipelines simples e que utilizam apenas o glue; ou step functions, para pipelines mais complexas (envonvendo muitos jobs, paralelismos e dependências) ou com outras ferramentas. O Airflow também é uma boa pedida em termos de orquestradores, principalmente quando o nível de complexidade da pipeline é alto, porém, nesse caso de arquitetura medalhão, o custo operacional de manter um Airflow separado não se justifica.

Porém, por estar no ecossistema Databricks, o Workflows é nativo e elimina a necessidade de infraestrutura adicional; se integra nativamente com os notebooks e as tabelas Delta do projeto, além de ter notificações, retry policies e dependências entre tasks, sendo assim a melhor opção.

O pipeline roda em dois modos:

- **Carga inicial**: execução única, coleta de histórico
- **Atualização incremental**: agendado diariamente via cron no Databricks Workflows

---

## Modelagem de Dados

![Gold](imagens/gold.png)

### `gold.fato`

Tabela com os ids (`id_criador`, `id_conta`, `id_publicacao`), usadas como links para as três tabelas dimensão e métricas importantes de serem acompanhadas por publicação de todas as plataformas: visualizações, likes, comentarios, compartilhamentos.

Data do snapshot, pois essa tabela terá armazenamento de histórico para acompanhar as métricas ao longo do tempo.

### `gold.criador`

Tabela com valores únicos de `id_criador` para registro de dados cadastrais de cada criador, atualizados eventualmente (upsert). Também teria campos como nome do criador (podendo ser de registro e/ou nome artítisco), país de origem do criador, página no wikipedia, nicho.

Data do cadastro do criador e data da última atualização (upsert).

### `gold.conta`

Tabela com valores únicos de `id_conta`, associada a url, para registro das diversas origens dos dados: youtube, instagram, tiktok, etc. Nela teremos dados da url completa, plataforma (youtube, instagram, tiktok, etc).

Data do cadastro da conta e data da última atualização (upsert).


### `gold.publicacao`

Tabela com valores únicos de `id_publicacao` com todas as publicações de todas as plataformas e criadores. Os dados seriam data da publicação, título da postagem, descrição, tags usadas, agregação de temas variados em um tema comum entre plataformas, `fonte_tema` (se aquele tema foi extraído de uma tag explícita ou inferido por NLP), relevância do tema com notas de 0 a 1.

Data do cadastro da publicação e data da última atualização (upsert).

---

## Extração de Dados

### Fontes

| Fonte | Dados extraídos | Método |
|---|---|---|
| Wikipedia | `creator_id` do YouTube por criador | Webscrapping |
| YouTube Data API | Metadados do canal e vídeos | REST API |
| Instagram Graph API | Metadados da conta e conteúdos | REST API |
Além de diversas outras possibilidades de API e Webscrapping

### Carga Inicial

A carga inicial seria realizada a partir da lista ids do YouTube, Instagram, etc. Com a API eu buscaria os metadados do canal / páginas e o histórico completo de vídeos / reels e suas informações: a carga inicial busca todo o histórico disponível de vídeos de cada canal, paginando até esgotar os resultados da API.

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

Notebook 1 (extração e salva na Bronze) -> Notebook 2 (lê da Bronze, transforma e salva na Silver) -> Notebook 3 (lê da Silver, agrega e salva na Gold)

### Notebook 1 — Extração (Bronze)

- lista ids do YouTube, Instagram, etc
- Busca nas API pelos dados e metadados
- Grava resultado bruto em `bronze.source` (append com timestamp)

### Notebook 2 — Transformação (Silver)

- Lê Bronze, cria colunas, valida e limpa os dados
- Tipagem explícita de todas as colunas
- Remove duplicatas e registros inválidos
- Data Quality
- Upsert via `MERGE INTO` nas tabelas Silver

### Notebook 3 — Agregação (Gold)

- Lê Silver e cria tabelas fato e dimensões
- Sobrescreve tabelas de cadastro / dimensão com `overwrite`
- Snapshot das tabelas de métricas

---

## Monitoramento e Qualidade

### Qualidade dos Dados

Em cada notebook Silver, validações são executadas antes do upsert, com a biblioteca de python [great expectations](https://community.databricks.com/t5/community-articles/data-quality-with-pyspark-and-great-expectations-on-databricks/td-p/128912):

Exemplos de checks de qualidade:
- validação da unicidade das chaves das tabelas dimensão: `id_criador`, `id_publicacao`, `id_conta`
- completude das chaves das tabelas dimensão: `id_criador`, `id_publicacao`, `id_conta`
- completude das colunas de data, `url`, `plataforma`
- O valor de `plataforma` deve estar contida em uma lista de valores válidos (youtube, instagram, tiktok, etc).
- A valor de `relevância` deve estar em um range de 0 a 1
- Valores de likes, compartilhamentos, views e comentários deve ser maior que 1

Se qualquer check falhar, o notebook levanta uma exceção e o Workflow marca a execução como falha — sem gravar dados inconsistentes.

### Monitoramento do Pipeline

| O que monitorar | Como |
|---|---|
| Falha em qualquer notebook | Alerta por e-mail no Databricks Workflows |
| Volume de registros ingeridos | Log no notebook + métrica no Delta history |
| Erros nas APIs | Contador de erros logado por execução |
| Schema | Schema fixo, falha explícita se mudar |

Obs: Delta history é o log de transações nativo do Delta Lake. Cada operação (write, merge, overwrite) gera uma entrada em DESCRIBE HISTORY nome_da_tabela, com timestamp, número de linhas afetadas, versão, etc. É para monitorar o volume de registros sem precisar de infraestrutura extra.

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

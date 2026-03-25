# Databricks notebook source
# MAGIC %md
# MAGIC Criar o notebook "3 - create_table_user_yt_from_wikipedia_api" para gerar a tabela delta default.users_yt
# MAGIC
# MAGIC Usar a tabela default.creators_scrape_wiki para buscar na api da wikipedia o user_id do youtube de cada wiki_name
# MAGIC
# MAGIC - dica 1: utilizar o endpoint https://en.wikipedia.org/w/api.php
# MAGIC - dica 2: utilizar parametros params = {"action": "parse","page": f"{page_name}","format": "json"}
# MAGIC
# MAGIC Campos da tabela default.users_yt: user_id(extraido da wikipedia) e o wiki_page(da tabela default.creators_scrape_wiki)
# MAGIC
# MAGIC Exemplo de 1 registro da tabela {'user_id': 'felipeneto', 'wiki_page': 'Felipe_Neto'}

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG winnin_teste;
# MAGIC USE SCHEMA default;

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from creators_scrape_wiki limit 3;

# COMMAND ----------

# MAGIC %sql 
# MAGIC select count(*) from creators_scrape_wiki;

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- Como em breve vou fazer um join com essa tabela:
# MAGIC select distinct yt_user from posts_creator;

# COMMAND ----------

import requests
import re
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType


# COMMAND ----------

df_creators = spark.table("default.creators_scrape_wiki")
wiki_pages = [row["wiki_page"] for row in df_creators.collect()]

print(f"Total de páginas a processar: {len(wiki_pages)}")
wiki_pages


# COMMAND ----------

# MAGIC %md
# MAGIC ### Testes

# COMMAND ----------

WIKIPEDIA_API_URL = "https://en.wikipedia.org/w/api.php"
HEADERS = {
    "User-Agent": "users-yt-pipeline/1.0 (databricks; educational project)"
}
params = {
        "action": "parse",
        "page": "Cocomelon",
        "format": "json"
    }
response = requests.get(WIKIPEDIA_API_URL, params=params, headers=HEADERS, timeout=10)
data = response.json()
html = data["parse"]["text"]["*"]
html

# COMMAND ----------

pattern = r'youtube\.com/(@|channel/|user/|c/)([\w][\w\.\-·]*[\w]|[\w])'
found = re.findall(pattern, html, re.IGNORECASE)
print(found)
# retorna lista de tuplas: [('@', 'felipeneto'), ('user/', 'felipeneto')]

urls = {f"https://www.youtube.com/{prefix}{handle}" for prefix, handle in found}
urls

# COMMAND ----------

df_posts = spark.table("default.posts_creator")
yt_users = [row["yt_user"].lower() for row in df_posts.select("yt_user").distinct().collect()]
yt_users

# COMMAND ----------

# MAGIC %md
# MAGIC ### Código

# COMMAND ----------

def request_wikipedia(page_name: str):
    """
    Chama a Wikipedia API com os parâmetros padrão (retorna HTML)
    """
    try:
        WIKIPEDIA_API_URL = "https://en.wikipedia.org/w/api.php"
        HEADERS = {
            "User-Agent": "users-yt-pipeline/1.0 (databricks; educational project)"
        }
        params = {
                "action": "parse",
                "page": page_name,
                "format": "json"
            }
        response = requests.get(WIKIPEDIA_API_URL, params=params, headers=HEADERS, timeout=10)

        if response.status_code == 200:
            data = response.json()
            html = data["parse"]["text"]["*"]
            return html
        else:
            print(f"[ERROR] Status da requisição diferente de 200: {response.status_code}")

    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Falha na requisição para {page_name}: {e}")

def get_youtube_id(html: str) -> str | None:
    """
    A partir do html do Wikipedia, busca todas as urls de youtube e retorna uma lista com os ids e com as urls
    """
    try:
        # Sabendo os padrões de url permitidos https://support.google.com/youtube/answer/6180214?hl=pt-BR
        # E os caracteres permitidos https://support.google.com/youtube/answer/11585688?hl=pt-Br
        """
        [\w] = procura caracteres, exceto os especiais
        [\w\.\-·]* = aceita também ., - e · no meio do id
        |[\w] = cobre o caso de identificador com apenas 1 caractere
        """
        pattern = r"youtube\.com/(@|channel/|user/|c/)([\w][\w\.\-·]*[\w]|[\w])"
        found = re.findall(pattern, html, re.IGNORECASE) # retorna lista de tuplas: [('@', 'felipeneto'), ('user/', 'felipeneto')]

        urls = {f"https://www.youtube.com/{prefix}{handle}" for prefix, handle in found}
        user_ids = {handle for prefix, handle in found}
        return list(user_ids), list(urls)

    except Exception as e:
        print(f"[ERROR] Falha na extração do id do Youtube: {e}")

def validation_check(page: str, urls: list):
    """
    Valida as listas de urls que ficaram vazias e atualiza os valores
    """
    # Se fosse um caso mais generalizável, dava para remover alguns itens que tornam o nome incorreto e rodar de novo
    # new_page = record["wiki_page"].replace('_(YouTuber)', '')
    # html = request_wikipedia(new_page)
    # new_user_id = get_youtube_id(html)

    # Porém, o nome é outro: Pirulla, então vou alterar manualmente para a consulta
    # Se for o caso, é importante alterar o valor de wiki_page também o valor na origem, optei por não fazer por não ter certeza se esse valor é utilizado em outros casos, outros joins, etc.
    new_wiki_page = 'Pirulla'
    print(f"[WARNING] Não foram encontradas urls na {page}, valor atualizado para {new_wiki_page}")
    html = request_wikipedia(new_wiki_page)
    new_user_ids, new_urls = get_youtube_id(html)
    return new_user_ids, new_urls
    
def check_youtube_channel(urls: list, api_key: str) -> bool:
    """
    Faz um request no Youtube para ter certeza de que as páginas encontradas realmente existem
    """
    for url in urls:
        try:
            handle = url.split("/")[-1].lstrip("@")
            
            response = requests.get(
                "https://www.googleapis.com/youtube/v3/channels",
                params={"part": "id", "forHandle": handle, "key": api_key},
                headers=HEADERS,
                timeout=10
            )
            data = response.json()
            return data.get("pageInfo", {}).get("totalResults", 0) > 0
        except requests.exceptions.RequestException as e:
            print(f"[ERROR] Falha na requisição para {url}: {e}")

def check_user_ids(user_ids: list):
    """
    Verifica se algum dos user_ids encontrados na Wikipedia está na lista de ids conhecidos (yt_users).
    Se sim, retorna o valor que teve match.
    Se não, retorna a lista completa de possibilidades.
    """
    print(user_ids)
    for user_id in user_ids:
        if user_id.lower() in yt_users:
            print(f"{user_id} in {yt_users}")
            return user_id
    
    print(f"{user_id} not in {yt_users}")
    user_id = ' | '.join(user_ids)
    return user_id

records = []
for page in wiki_pages:
    print(f"\nProcessando {page}...")
    html = request_wikipedia(page)
    user_ids, urls = get_youtube_id(html)
    if not urls:
        user_ids, urls = validation_check(page, urls)
    user_id = check_user_ids(user_ids)
    # data = check_youtube_channel(urls, api_key)
    
    records.append({"user_id": user_id, "wiki_page": page})
    print(f"{page} → {user_id}")

print(f"\nRegistros finais: {records}")


# COMMAND ----------

schema = StructType([
    StructField("user_id", StringType(), nullable=True),
    StructField("wiki_page", StringType(), nullable=False),
])

df_users_yt = spark.createDataFrame(records, schema=schema)
df_users_yt.show()

(
    df_users_yt.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("default.users_yt")
)

print("Tabela default.users_yt criada com sucesso.")

# COMMAND ----------


# Databricks notebook source
# MAGIC %md
# MAGIC Criar o notebook "4 - analyze_creators"
# MAGIC
# MAGIC Usar o join da default.users_yt com a default.posts_creator para gerar analises desses creators
# MAGIC
# MAGIC - Mostrar o top 3 posts ordenado por likes de cada creator nos últimos 6 meses (user_id, title, likes, rank)
# MAGIC - Mostrar o top 3 posts ordenado por views de cada creator nos últimos 6 meses (user_id, title, views, rank)
# MAGIC - Mostrar os yt_user que estão na tabela default.post_creator mas não estão na tabela default.users_yt
# MAGIC - Mostrar a quantidade de publicações por mês de cada creator.
# MAGIC
# MAGIC Extras
# MAGIC - Exercício Extra 1: mostrar 0 nos meses que não tem video
# MAGIC - Exercício Extra 2: transformar a tabela no formato que a primeira coluna é o user_id e temos uma coluna para cada mês. Ex:
# MAGIC   - user_id, 2024/01, 2024/02, 2024/03
# MAGIC   - felipeneto, 10, 20, 30
# MAGIC   - lucasneto, 5, 10, 15
# MAGIC - Exercício Extra 3: Mostrar as 3 tags mais utilizadas por criador de conteúdo
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG winnin_teste;
# MAGIC USE SCHEMA default;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from posts_creator
# MAGIC left join users_yt
# MAGIC on lower(users_yt.user_id) = lower(posts_creator.yt_user)
# MAGIC limit 10;

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import timedelta


# COMMAND ----------

# DBTITLE 1,Cell 5
posts_creator = spark.table("default.posts_creator")
users_yt = spark.table("default.users_yt")

df = posts_creator.join(
    F.broadcast(users_yt), 
    F.lower(posts_creator['yt_user']) == F.lower(users_yt['user_id']),
    'left'
)

df.show(1)

# COMMAND ----------

# Mais fácil de validar
df = df.withColumn("published_at_timestamp", F.from_unixtime("published_at").cast("timestamp"))
df.show(1)

# COMMAND ----------

# DBTITLE 1,Cell 8
min_date, max_date = df.agg(
    F.min("published_at_timestamp"),
    F.max("published_at_timestamp")
).collect()[0]

date_6_months_ago = max_date - timedelta(days=180)
print(f"Intervalo: {min_date} → {max_date}")
print(f"6 meses atrás: {date_6_months_ago}")

last_6_months = df.filter(F.col("published_at_timestamp") >= date_6_months_ago)

total, last_6 = df.agg(F.count("*")).collect()[0][0], last_6_months.count()
print(f"Total de posts: {total}")
print(f"Posts nos últimos 6 meses: {last_6}")

last_6_months.show(1)

# COMMAND ----------

last_6_months.filter(F.col('yt_user').isNull()).show(1)
last_6_months.filter(F.col('user_id').isNull()).show(1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mostrar o top 3 posts ordenado por likes de cada creator nos últimos 6 meses (user_id, title, likes, rank)

# COMMAND ----------

# DBTITLE 1,Cell 9
window_likes = Window.partitionBy('user_id').orderBy(F.desc('likes'))

# Por user_id, temos valores vazios, que vieram da outra tabela e não tiveram match
# Por isso vou remover os vazios
top3_likes = last_6_months.filter(F.col('user_id').isNotNull()) \
    .withColumn('rank', F.row_number().over(window_likes)) \
    .filter(F.col('rank') <= 3) \
    .select('user_id', 'title', 'likes', 'rank') \
    .orderBy('user_id', 'rank')

top3_likes.display()

# COMMAND ----------

window_likes = Window.partitionBy('yt_user').orderBy(F.desc('likes'))

# Por yt_user, não temos valores vazios, então temos todos os criadores:
top3_likes = last_6_months.withColumn('rank', F.row_number().over(window_likes)) \
    .filter(F.col('rank') <= 3) \
    .select('yt_user', 'title', 'likes', 'rank') \
    .orderBy('yt_user', 'rank')

top3_likes.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mostrar o top 3 posts ordenado por views de cada creator nos últimos 6 meses (user_id, title, views, rank)

# COMMAND ----------

# DBTITLE 1,Cell 10: Top 3 por Views
window_views = Window.partitionBy('user_id').orderBy(F.desc('views'))

# Por user_id, temos valores vazios, que vieram da outra tabela e não tiveram match
# Por isso vou remover os vazios
top3_views = last_6_months.filter(F.col('user_id').isNotNull()) \
    .withColumn('rank', F.row_number().over(window_views)) \
    .filter(F.col('rank') <= 3) \
    .select('user_id', 'title', 'views', 'rank') \
    .orderBy('user_id', 'rank')

top3_views.display()

# COMMAND ----------

window_views = Window.partitionBy('yt_user').orderBy(F.desc('views'))

# Como feito anteriormente, por yt_user, não temos valores vazios, então temos todos os criadores:
top3_views = last_6_months.withColumn('rank', F.row_number().over(window_views)) \
    .filter(F.col('rank') <= 3) \
    .select('yt_user', 'title', 'views', 'rank') \
    .orderBy('yt_user', 'rank')

top3_views.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mostrar os yt_user que estão na tabela default.post_creator mas não estão na tabela default.users_yt

# COMMAND ----------

# DBTITLE 1,Cell 11: Creators Não Encontrados
# Os creators que não tem correspondência são os que não tiveram match (ficaram com users_id nulos)
creators_not_found = df.filter(F.col('user_id').isNull()) \
    .select('yt_user') \
    .distinct()

print(f"Creators sem correspondência: {creators_not_found.count()}")
print("\nCreators em posts_creator que NÃO estão em users_yt:")
creators_not_found.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mostrar a quantidade de publicações por mês de cada creator.
# MAGIC

# COMMAND ----------

df = df.withColumn('year_month', F.date_format('published_at_timestamp', 'yyyy-MM'))
df.show(1)

# COMMAND ----------

# DBTITLE 1,Cell 12: Publicações por Mês
posts_per_month = df.groupBy('yt_user', 'year_month') \
    .agg(F.count('*').alias('total_posts')) \
    .orderBy('yt_user', 'year_month')

posts_per_month.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício Extra 1: mostrar 0 nos meses que não tem video

# COMMAND ----------

df.show(1)

# COMMAND ----------

# Vou criar uma tabela com todos os meses
start_date, end_date = df.agg(
    F.min("published_at_timestamp"),
    F.max("published_at_timestamp")
).collect()[0]

# Colocar dia = 1
start_date = start_date.replace(day=1)
end_date = end_date.replace(day=1)

months = []
current = start_date
while current <= end_date:
    months.append(current.strftime("%Y-%m"))
    current += relativedelta(months=1)

schema = StructType([
    StructField("year_month", StringType(), True)
])
df_year_month = spark.createDataFrame(months, schema)

# Cross join pra ter todas as combinações de meses com criadores
creators = df.select("yt_user").distinct()
df_year_month = creators.crossJoin(df_year_month)

posts_per_month_fill = posts_per_month.join(
        df_year_month,
        ['year_month', 'yt_user'],
        # (df_year_month['year_month'] == posts_per_month['year_month']) & (df_year_month['yt_user'] == posts_per_month['yt_user']),
        'right'
    ) \
    .fillna(0, subset=["total_posts"]) \
    .orderBy('yt_user', 'year_month')

posts_per_month_fill.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício Extra 2: transformar a tabela no formato que a primeira coluna é o user_id e temos uma coluna para cada mês. 
# MAGIC Ex:
# MAGIC user_id, 2024/01, 2024/02, 2024/03
# MAGIC felipeneto, 10, 20, 30
# MAGIC lucasneto, 5, 10, 15

# COMMAND ----------

# DBTITLE 1,Cell 26
# Pivot mantendo yt_user como primeira coluna
transposed_df = posts_per_month_fill.groupBy('yt_user').pivot('year_month').agg(F.sum('total_posts'))
transposed_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício Extra 3: Mostrar as 3 tags mais utilizadas por criador de conteúdo

# COMMAND ----------

df.select('yt_user', F.explode('tags').alias('tags')).show()

# COMMAND ----------

tags_df = df.select('yt_user', F.explode('tags').alias('tags')) \
    .groupBy('yt_user', 'tags') \
    .agg(F.count('*').alias('tag_count')) \
    .orderBy('yt_user', F.desc('tag_count'))

window = Window.partitionBy('yt_user').orderBy(F.desc('tag_count'))

tags_df = tags_df.withColumn('rank', F.row_number().over(window)) \
    .filter(F.col('rank') <= 3) \
    .select('yt_user', 'tags', 'tag_count') \
    .orderBy('yt_user', 'rank')

tags_df.display()
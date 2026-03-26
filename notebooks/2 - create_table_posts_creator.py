# Databricks notebook source
# MAGIC %md
# MAGIC Criar o notebook "2 - create_table_posts_creator" que le o arquivo 2-posts_creator.json.gz e cria a tabela delta default.posts_creator

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG winnin_teste;
# MAGIC USE SCHEMA default;
# MAGIC

# COMMAND ----------

## Ler o arquivo posts_creator.json.gz

file_path = "/Workspace/Users/crqr.naiara@gmail.com/teste_winnin/posts_creator.json.gz"

df = spark.read.json(file_path)

print(f"{df.count()} linhas carregadas")
df.show(1)


# COMMAND ----------

# Criar tabela
nome_tabela = "winnin_teste.default.posts_creator"

df.write.format("delta").mode("overwrite").saveAsTable(nome_tabela)

print(f"Tabela '{nome_tabela}' criada com sucesso!")


# COMMAND ----------


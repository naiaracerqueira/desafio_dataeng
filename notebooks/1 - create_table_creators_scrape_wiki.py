# Databricks notebook source
# MAGIC %md
# MAGIC Criar o notebook "1 - create_table_creators_scrape_wiki" que le o arquivo 1-wiki_pages.json.gz e cria a tabela delta default.creators_scrape_wiki

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG winnin_teste;
# MAGIC USE SCHEMA default;
# MAGIC
# MAGIC SELECT current_catalog(), current_schema();

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE SCHEMA EXTENDED winnin_teste.default;

# COMMAND ----------

## Ler o arquivo wiki_pages.json.gz

file_path = "/Workspace/Users/crqr.naiara@gmail.com/teste_winnin/wiki_pages.json.gz"

df = spark.read.json(file_path)

print(f"{df.count()} linhas carregadas")
df.show(1)


# COMMAND ----------

# Criar tabela
nome_tabela = "winnin_teste.default.creators_scrape_wiki"

df.write.format("delta").mode("overwrite").saveAsTable(nome_tabela)

print(f"Tabela '{nome_tabela}' criada com sucesso!")


# COMMAND ----------


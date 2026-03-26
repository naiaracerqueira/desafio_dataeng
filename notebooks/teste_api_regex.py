# Databricks notebook source
import requests
import re

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
found
# retorna lista de tuplas: [('@', 'felipeneto'), ('user/', 'felipeneto')]


# COMMAND ----------

urls = {f"https://www.youtube.com/{prefix}{handle}" for prefix, handle in found}
urls

# COMMAND ----------


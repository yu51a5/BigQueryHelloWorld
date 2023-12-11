#The purpose of this basic project is to select the shortest StackOverflow questions containing "hello" and "world", and also the most ancient and the most recent questions in the dataset

# Authentication: create a service account, generate a key in JSON format 
# Next save its contents (everything except opening `{` and closing `}`) as google_key secret.

# how-to: https://docs.aws.amazon.com/dms/latest/sbs/bigquery-redshift-migration-step-1.html (steps 1.-10. on one web page)
# thanks: https://stackoverflow.com/questions/73195754/python-google-bigquery-how-to-authenticate-without-json-file

import os

from google.cloud import bigquery
from google.oauth2 import service_account

assert 'google_key' in os.environ, "Please define a secret called google_key; its value must be the contents of access key in JSON format"

cred = service_account.Credentials.from_service_account_info(eval("{" + os.environ['google_key'] +"}"))
client = bigquery.Client(credentials=cred, project=cred.project_id)
# running a dummy query to make sure that the access key is valid
try:
  query_job = client.query("SELECT * FROM `bigquery-public-data.stackoverflow.posts_questions` LIMIT 1")
  query_job.result()
except:
  raise Exception("Error: your google_key secret is not valid")

# SQL dialect reference: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions
query_str = ["""
            SELECT
              CONCAT(
                'https://stackoverflow.com/questions/',
                CAST(id as STRING)) as url,
              view_count, body, title, creation_date
            FROM `bigquery-public-data.stackoverflow.posts_questions`""" + clauses
             for clauses in (["""WHERE LOWER(body) like '%hello%world%' 
                                 ORDER BY CHAR_LENGTH(body) 
                                 LIMIT 3"""] \
                           + [f"""ORDER BY creation_date {sort_order} 
                                  LIMIT 1""" for sort_order in ("", " DESC")])]

results = [client.query(qs).result() for qs in query_str]

for i, r in enumerate(results):
  print(f'\nQuery {i}')
  for row in r:
    print(f"\nQuestion `{row.title}` created on {row.creation_date}, viewed {row.view_count} time(s):\n`URL {row.url}`\nContents `{row.body}`\n")

print("all done!")

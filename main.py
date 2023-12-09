import os

from google.cloud import bigquery
from google.oauth2 import service_account

#The purpose of this basic project is to  select the shortest StackOverflow questions containing "hello" and "world".

# Authentication: create a service account, generate a key in JSON format 
# Next save its contents (everything except opening `{` and closing `}`) as google_key secret.

# how-to: https://docs.aws.amazon.com/dms/latest/sbs/bigquery-redshift-migration-step-1.html (steps 1.-10. on one web page)
# thanks: https://stackoverflow.com/questions/73195754/python-google-bigquery-how-to-authenticate-without-json-file
cred = service_account.Credentials.from_service_account_info(eval("{" + os.environ['google_key'] +"}"))
client = bigquery.Client(credentials=cred, project=cred.project_id)

# SQL dialect reference: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions
query_job = client.query(
    """
    SELECT
      CONCAT(
        'https://stackoverflow.com/questions/',
        CAST(id as STRING)) as url,
      view_count, body, title
    FROM `bigquery-public-data.stackoverflow.posts_questions`
    WHERE LOWER(body) like '%hello%world%'
    ORDER BY CHAR_LENGTH(body)
    LIMIT 10"""
)

results = query_job.result()

for row in results:
  print(f"\n{row.url} : {row.view_count} views for question\n{row.title}\n{row.body}\n")

print("all done!")

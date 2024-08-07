CREATE OR REPLACE MODEL
`sofia-data-305018.cadena_cuidado.embedding-multilingual-text-vertex`
REMOTE WITH CONNECTION `projects/sofia-data-305018/locations/us-central1/connections/gemini-pro-llm-vertex-connection`
OPTIONS (ENDPOINT = 'text-multilingual-embedding-002')

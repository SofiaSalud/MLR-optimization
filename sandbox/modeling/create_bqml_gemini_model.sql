CREATE OR REPLACE MODEL
`sofia-data-305018.cadena_cuidado.gemini-pro-text-vertex`
REMOTE WITH CONNECTION `projects/sofia-data-305018/locations/us-central1/connections/gemini-pro-llm-vertex-connection`
OPTIONS (ENDPOINT = 'gemini-1.5-pro-001')

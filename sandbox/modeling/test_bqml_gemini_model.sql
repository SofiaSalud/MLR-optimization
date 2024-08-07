SELECT *
FROM
  ML.GENERATE_TEXT(
    MODEL `sofia-data-305018.cadena_cuidado.gemini-pro-text-vertex`,
    (
      SELECT CONCAT('Who was Caesar Augustus?') AS prompt
    ),
    STRUCT(
      0.2 AS temperature, 650 AS max_output_tokens, TRUE AS flatten_json_output)
)

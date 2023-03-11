{{ config(materialized='incremental') }}
 




 
WITH staged_data AS (
  SELECT *
  FROM {{ ref('staging_table') }}
),

changed_data AS (
  SELECT *
  FROM staged_data
  WHERE NOT EXISTS (
    SELECT *
    FROM {{ ref('main_table') }}
    WHERE staged_data.id = main_table.id
  )
  OR EXISTS (
    SELECT *
    FROM {{ ref('main_table') }}
    WHERE staged_data.id = main_table.id
    AND staged_data.updated_at > main_table.updated_at
  )
)

MERGE INTO {{ ref('main_table') }} main
USING changed_data staged
ON main.id = staged.id
WHEN MATCHED AND staged.updated_at > main.updated_at THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *

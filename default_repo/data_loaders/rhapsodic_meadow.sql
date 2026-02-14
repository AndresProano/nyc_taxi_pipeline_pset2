-- Docs: https://docs.mage.ai/guides/sql-blocks
SELECT schema_name 
FROM information_schema.schemata
WHERE schema_name IN ('bronze', 'silver', 'gold');
Use DATABASE DE_2;
USE SCHEMA RAW;

-- 02_create_network_rule_access.sql
CREATE OR REPLACE NETWORK RULE open_meteo_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('archive-api.open-meteo.com:443');

  CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION open_meteo_eai
  ALLOWED_NETWORK_RULES = (open_meteo_rule)
  ENABLED = TRUE;

  GRANT USAGE ON INTEGRATION open_meteo_eai TO ROLE DE_2;

-- EXERCISED_EVENTS, example projection table
CREATE TABLE "exercised_events"
(
  "contract_id"     TEXT     NOT NULL,
  "event_id"        TEXT     NOT NULL,
  "acting_parties"  TEXT     NOT NULL,
  "witness_parties" TEXT     NOT NULL, 
  "event_offset"    TEXT     NOT NULL
);

-- Adding some constraints and indexes, no big difference.
-- ALTER TABLE "exercised_events"
--  ADD CONSTRAINT "exercised_events_id" PRIMARY KEY ("id");
-- CREATE INDEX event_offset_ix ON "exercised_events" ("event_offset");

-- if only inserting, switching off vacuum could be faster
--ALTER TABLE "exercised_events" SET (
--  autovacuum_enabled = false, toast.autovacuum_enabled = false
--);

CREATE TABLE ious
(
  contract_id     TEXT     NOT NULL,
  event_id        TEXT     NOT NULL,
  amount          DECIMAL  NOT NULL,
  currency        TEXT     NOT NULL,
  witness_parties TEXT     NULL
);

CREATE TABLE "java_api_exercised_events"
(
  "contract_id"                 TEXT     NOT NULL,
  "transfer_result_contract_id" TEXT     NOT NULL,
  "event_id"                    TEXT     NOT NULL,
  "acting_parties"              TEXT     NOT NULL,
  "witness_parties"             TEXT     NOT NULL,
  "event_offset"                TEXT     NOT NULL
);

CREATE TABLE "java_api_tree_events"
(
  "contract_id"                 TEXT     NOT NULL,
  "transfer_result_contract_id" TEXT     NOT NULL,
  "event_id"                    TEXT     NOT NULL,
  "acting_parties"              TEXT     NOT NULL,
  "witness_parties"             TEXT     NOT NULL,
  "event_offset"                TEXT     NOT NULL
);

CREATE TABLE "column_types_table"
(
  "contract_id"        TEXT      NULL,
  "stakeholder"        TEXT      NOT NULL,
  "long_column"        BIGINT    NULL,
  "int_column"         INT       NULL,
  "short_column"       SMALLINT  NULL,
  "boolean_column"     BOOL      NULL,
  "double_column"      FLOAT8    NULL,
  "float_column"       FLOAT4    NULL,
  "event_offset"       TEXT      NULL,
  "projection_id"      TEXT      NULL,
  "bigdecimal_column"  DECIMAL   NULL,
  "date_column"        DATE      NULL,
  "timestamp_column"   TIMESTAMPTZ NULL
);

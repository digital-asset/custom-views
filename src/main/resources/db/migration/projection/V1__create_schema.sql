-- PROJECTION
CREATE TABLE "projection"
(
  "id"                TEXT        NOT NULL,
  "projection_table"  TEXT        NOT NULL,
  "data"              JSON        NOT NULL,
  "projection_type"   TEXT        NOT NULL,
  "projection_offset" TEXT        NULL
);

ALTER TABLE "projection"
  ADD CONSTRAINT "projection_id" PRIMARY KEY ("id");

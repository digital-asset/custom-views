-- PROJECTION
CREATE TABLE ${projection_table_name}
(
  "id"                TEXT        NOT NULL,
  "projection_offset" TEXT        NULL
);

ALTER TABLE ${projection_table_name}
  ADD CONSTRAINT "projection_id" PRIMARY KEY ("id");

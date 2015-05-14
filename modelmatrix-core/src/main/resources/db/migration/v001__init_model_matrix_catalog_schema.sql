-- Model Matrix Control definition

CREATE SEQUENCE mmc_definition_seq;

CREATE TABLE mmc_definition (
    id         INT NOT NULL DEFAULT nextval('mmc_definition_seq')
  , source     TEXT NOT NULL
  , created_by TEXT NOT NULL
  , created_at TIMESTAMP
  , comment    TEXT
);

-- Model Matrix Control definition

CREATE SEQUENCE mmc_definition_seq;
CREATE SEQUENCE mmc_definition_feature_seq;
CREATE SEQUENCE mmc_definition_feature_top_param_seq;
CREATE SEQUENCE mmc_definition_feature_index_param_seq;

CREATE TABLE mmc_definition (
    id         INT NOT NULL UNIQUE DEFAULT nextval('mmc_definition_seq')
  , name       TEXT
  , source     TEXT NOT NULL
  , created_by TEXT NOT NULL
  , created_at TIMESTAMP
  , comment    TEXT
);

CREATE TABLE mmc_definition_feature (
    id                    INT NOT NULL UNIQUE DEFAULT nextval('mmc_definition_feature_seq')
  , model_definition_id   INT REFERENCES mmc_definition (id)
  , active                BOOLEAN NOT NULL
  , grp                   TEXT NOT NULL
  , feature               TEXT NOT NULL
  , extrct                TEXT NOT NULL
  , transform             TEXT NOT NULL
);

CREATE TABLE mmc_definition_feature_top_param (
    id                    INT NOT NULL UNIQUE DEFAULT nextval('mmc_definition_feature_top_param_seq')
  , feature_definition_id INT REFERENCES mmc_definition_feature (id)
  , percentage            DECIMAL NOT NULL
  , all_other             BOOLEAN NOT NULL
);

CREATE TABLE mmc_definition_feature_index_param (
    id                    INT NOT NULL UNIQUE DEFAULT nextval('mmc_definition_feature_index_param_seq')
  , feature_definition_id INT REFERENCES mmc_definition_feature (id)
  , percentage            DECIMAL NOT NULL
  , all_other             BOOLEAN NOT NULL
);

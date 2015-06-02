-- Model Matrix definition

CREATE SEQUENCE mmc_definition_seq;
CREATE SEQUENCE mmc_definition_feature_seq;
CREATE SEQUENCE mmc_definition_feature_top_param_seq;
CREATE SEQUENCE mmc_definition_feature_index_param_seq;
CREATE SEQUENCE mmc_definition_feature_bins_param_seq;

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
  , cover                 DECIMAL NOT NULL
  , all_other             BOOLEAN NOT NULL
);

CREATE TABLE mmc_definition_feature_index_param (
    id                    INT NOT NULL UNIQUE DEFAULT nextval('mmc_definition_feature_index_param_seq')
  , feature_definition_id INT REFERENCES mmc_definition_feature (id)
  , support               DECIMAL NOT NULL
  , all_other             BOOLEAN NOT NULL
);

CREATE TABLE mmc_definition_feature_bins_param (
    id                    INT NOT NULL UNIQUE DEFAULT nextval('mmc_definition_feature_bins_param_seq')
  , feature_definition_id INT REFERENCES mmc_definition_feature (id)
  , nbins                 INT NOT NULL
  , min_points            INT NOT NULL
  , min_pct               INT NOT NULL
);

-- Model Matrix Instances

CREATE SEQUENCE mmc_instance_seq;
CREATE SEQUENCE mmc_instance_feature_seq;
CREATE SEQUENCE mmc_instance_feature_identity_column_seq;
CREATE SEQUENCE mmc_instance_feature_top_column_seq;
CREATE SEQUENCE mmc_instance_feature_index_column_seq;
CREATE SEQUENCE mmc_instance_feature_bins_column_seq;

CREATE TABLE mmc_instance (
    id                  INT NOT NULL UNIQUE DEFAULT nextval('mmc_instance_seq')
  , model_definition_id INT REFERENCES mmc_definition (id)
  , name                TEXT
  , created_by          TEXT NOT NULL
  , created_at          TIMESTAMP
  , comment             TEXT
);

CREATE TABLE mmc_instance_feature (
    id                    INT NOT NULL UNIQUE DEFAULT nextval('mmc_instance_feature_seq')
  , model_instance_id     INT REFERENCES mmc_instance (id)
  , feature_definition_id INT REFERENCES mmc_definition_feature (id)
  , extract_type          TEXT NOT NULL
);

CREATE TABLE mmc_instance_feature_identity_column (
    id                    INT NOT NULL UNIQUE DEFAULT nextval('mmc_instance_feature_identity_column_seq')
  , feature_instance_id   INT REFERENCES mmc_instance_feature (id)
  , column_id             INT NOT NULL
);

CREATE TABLE mmc_instance_feature_top_column (
    id                    INT NOT NULL UNIQUE DEFAULT nextval('mmc_instance_feature_top_column_seq')
  , feature_instance_id   INT REFERENCES mmc_instance_feature (id)
  , column_id             INT NOT NULL
  , source_name           TEXT
  , source_value          BYTEA
  , cnt                   INT NOT NULL
  , cumulative_cnt        INT NOT NULL
);

CREATE TABLE mmc_instance_feature_index_column (
    id                    INT NOT NULL UNIQUE DEFAULT nextval('mmc_instance_feature_index_column_seq')
  , feature_instance_id   INT REFERENCES mmc_instance_feature (id)
  , column_id             INT NOT NULL
  , source_name           TEXT
  , source_value          BYTEA
  , cnt                   INT NOT NULL
  , cumulative_cnt        INT NOT NULL
);

CREATE TABLE mmc_instance_feature_bins_column (
    id                    INT NOT NULL UNIQUE DEFAULT nextval('mmc_instance_feature_bins_column_seq')
  , feature_instance_id   INT REFERENCES mmc_instance_feature (id)
  , column_id             INT NOT NULL
  , low                   NUMERIC NOT NULL
  , high                  NUMERIC NOT NULL
  , cnt                   INT NOT NULL
  , sample_size           INT NOT NULL
);

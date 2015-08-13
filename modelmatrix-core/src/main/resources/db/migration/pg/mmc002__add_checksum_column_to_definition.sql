-- Model Matrix definition

ALTER TABLE mmc_definition ADD checksum VARCHAR(32) NOT NULL UNIQUE;

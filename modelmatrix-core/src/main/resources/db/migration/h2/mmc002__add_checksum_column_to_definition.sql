-- Model Matrix definition

ALTER TABLE mmc_definition ADD IF NOT EXISTS checksum VARCHAR(32);
ALTER TABLE mmc_definition ALTER COLUMN checksum SET NOT NULL;
ALTER TABLE mmc_definition ADD CONSTRAINT mmc_definition_checksum_unique UNIQUE(checksum);
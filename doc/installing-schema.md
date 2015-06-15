---
layout: default
title: Installing Schema
---

# Installing Schema

Model Matrix use PostgreSQL database to store model matrix definitions and all associated metadata.

## #1 Create Model Matrix database

Default catalog database configuration defined in `modelmatrix-cli/src/main/resources/reference.conf`.

    url      = "jdbc:postgresql://localhost/modelmatrix"  
    user     = "modelmatrix"  
    password = "modelmatrix"
    

## #2 Install Model Matrix schema

Schema migrations managed by [Flyway](http://flywaydb.org), 
schema DDL and migrations located in: `modelmatrix-core/src/main/resources/db/migration`

First it's required to package CLI distribution. In root directory run:

    sbt universal:packageBin
    
The cli zip distribution will be placed in:
  - `modelmatrix/modelmatrix-cli/target/universal/model-matrix-cli-0.0.2.zip`

> If you want to create schema in non-default database check [CLI documentation](cli.html#specify-database) on how to 
> update database config.

Install Model Matrix Catalog schema:

    bin/modelmatrix-cli install-schema
    

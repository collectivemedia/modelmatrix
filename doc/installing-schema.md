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
    
However you can create any database name/user/password you like, you'll be able to provide custom configuration later for
all command line tools and Scala API.

## #2 Install Model Matrix schema

Schema migrations managed by [Flyway](http://flywaydb.org), 
schema DDL and migrations located in: `modelmatrix-core/src/main/resources/db/migration`

Install schema in default database:

    sbt> project modelmatrix-core  
    sbt> flywayMigrate 
    
If you need to install schema into custom database, you have to provide flyway properties at sbt startup

    sbt -Dflyway.url=myUrl \
        -Dflyway.user=myUser \
        -Dflyway.password=mySecretPwd \
        -Dflyway.schemas=schema1,schema2,schema3 \
        -Dflyway.placeholders.keyABC=valueXYZ \
        -Dflyway.placeholders.otherplaceholder=value123

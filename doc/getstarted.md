---
layout: default
title: Getting Started
---

# Getting Started

Here is a very simple sample to get started with Model Matrix.

## Requirements

- SBT 0.13.x
- Scala 2.10.4
- PostgreSQL database


## #1 Create Model Matrix Catalog database in PostreSQL

Model Matrix store it's [catalog](doc/philosophy.html#model-matrix-catalog) in PostgreSQL database, default catalog db configuration 
defined in `modelmatrix-cli/src/main/resources/reference.conf`. 

    url      = "jdbc:postgresql://localhost/modelmatrix"  
    user     = "modelmatrix"  
    password = "modelmatrix"

## #2 Install Model Matrix schema in catalog db

Schema migrations managed by [Flyway](http://flywaydb.org), 
Schema DDL and migrations located in: `modelmatrix-core/src/main/resources/db/migration`

Install schema for development:

    sbt> project modelmatrix-core  
    sbt> flywayMigrate 
    
If you need to install schema into different database, you have to provide flyway properties at sbt startup

    sbt -Dflyway.url=myUrl \
        -Dflyway.user=myUser \
        -Dflyway.password=mySecretPwd \
        -Dflyway.schemas=schema1,schema2,schema3 \
        -Dflyway.placeholders.keyABC=valueXYZ \
        -Dflyway.placeholders.otherplaceholder=value123

## #3 Assembly application jar file

First it's required to prepare assembled jar file. In root directory run:

    sbt assembly
    
The cli assembled jar will be placed in:
  - `modelmatrix/modelmatrix-cli/target/scala-2.10/model-matrix-cli.jar`

## #4 Check that Schema installed successfully

You run simple cli command to ensure that schema installed successfully
   
    # List available model matrix definitions
    mm definitions list
    
## #5 Use custom database config
    
If you want to use custom database settings you can provide them in 
separate database configuration and pass it to CLI with db options:
    
    # datasci-db.conf
    datasci-db {
     url = "jdbc:postgresql://localhost/datasci_mm?user=datasci&password=datasci"
     driver = org.postgresql.Driver
     keepAliveConnection = true      
    }    
    
and later pass additional options to CLI:
    
    # List available model matrix definitions inside DataSci database
    mm definitions list --dbConf ./datasci-db.conf --dbName datasci-db
   

## And much more…

More documentation on Command Line Interface is [here](doc/cli.html).

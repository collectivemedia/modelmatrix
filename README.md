# Model Matrix

Model Matrix (feature engineering tools)

Model Matrix Configuration catalog is stored in PostgreSQL database

## Developing

Local installation of PostgreSQL required for integration tests
 
#### Default PostrgeSQL database config

    url      = "jdbc:postgresql://localhost/modelmatrix"  
    user     = "modelmatrix"  
    password = "modelmatrix"  

#### Install schema

Schema migrations managed by [Flyway](http://flywaydb.org), 
schema DDL and migrations located in: `modelmatrix-core/src/main/resources/db/migration`

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

## Testing

All tests that require Spark or Postgres are running as a part of integration tests

    sbt test
    sbt it:test

## Git Workflow

This repository workflow is based on [A successful Git branching model](http://nvie.com/posts/a-successful-git-branching-model/) with two main branches with an infinite lifetime:

* master
* develop

The **master** branch at origin should be familiar to every Git user. Parallel to the master branch, another branch exists called **develop**.

We consider **origin/master** to be the main branch where the source code of HEAD always reflects a production-ready state.

We consider **origin/develop** to be the main branch where the source code of HEAD always reflects a state with the latest delivered development changes for the next release. Some would call this the “integration branch”. This is where any automatic nightly builds are built from.

Further details are available in [A successful Git branching model](http://nvie.com/posts/a-successful-git-branching-model/) blog post.

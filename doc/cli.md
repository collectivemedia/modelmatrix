---
layout: default
title: Command Line Interface
---

# <a name="command-line-interface">Command Line Interface</a>

First you need to build Model Matrix CLI application distribution file:

    sbt> universal:packageBin
    
CLI will be packaged as zip file and should be available at:
    
    modelmatrix/modelmatrix-cli/target/universal/

To get list of all available commands simply run `bin/modelmatrix-cli` without arguments:

    modelmatrix-cli-0.0.2/bin/modelmatrix-cli

## <a name="specify-database">Specify Model Matrix Database</a>
    
If you want to use custom database settings you should update `application.conf` 
that is located at `modelmatrix-cli-0.0.2/conf/application.conf`
    
    ## Provide database configuration for modelmatrix catalog db
    # modelmatrix {
    #   catalog.db {
    #     url = "jdbc:postgresql://postgrestest001/modelmatrix?user=ezhulenev"
    #     driver = org.postgresql.Driver
    #     keepAliveConnection = true
    #   }
    # }
    
## <a name="mmc-definition">Model Matrix definitions</a>

#### List available model matrix definitions

    bin/modelmatrix-cli definition list [--name <name>]

#### View model matrix definition

    bin/modelmatrix-cli definition view features --definition-id <model-definition-id>    
    bin/modelmatrix-cli definition view source --definition-id <model-definition-id>
         
#### Validate model matrix definition config

    bin/modelmatrix-cli definition validate --config <model-config>
        
#### Add model matrix definition

    bin/modelmatrix-cli definition add [options] --config <model-config>
    
| Option      | Example                       | Description                            |
| ----------- | ----------------------------- | -------------------------------------- |
| n, name     | -n "v123"                     | matrix model definition name           |
| c, comment  | -c "testing new model"        | matrix model definition comment        |
| f, features | -f "features"                 | features definition property in config |
    
### Examples    

    # Validate config
    bin/modelmatrix-cli definition validate --config ./model-matrix-v123.conf
    
    # Create new definition if it's valid
    bin/modelmatrix-cli definition add \
          --name "v123" \
          --comment "testing" \
          --config ./model-matrix-v123.conf
    
    # Check that it appears in list of all definitions
    bin/modelmatrix-cli definition list
    
    # Find it by name
    bin/modelmatrix-cli definition list --name v123
    
    # View feature definitions
    bin/modelmatrix-cli definitions view --definition-id <model-definition-id>

## <a name="mmc-instance">Model Matrix instances</a>

#### List available model matrix instances

    bin/modelmatrix-cli instance list [--name <name>] [--definition <model-definition-id>]

#### View model matrix instance features

    bin/modelmatrix-cli instance view features --instance-id <model-instance-id>    
    
#### View model matrix instance columns
    
    bin/modelmatrix-cli instance view columns [options] --instance-id <model-instance-id>
    
| Option      | Example                       | Description                            |
| ----------- | ----------------------------- | -------------------------------------- |
| f, feature  | -f ad_position                | filter by feature name                 |
| g, group    | -g geographic                 | filter by feature group                |
    
         
#### Validate model matrix definition against input data

Check that input data compatible with model definition and can be used to build model instance

    bin/modelmatrix-cli instance validate --definition-id <model-definition-id> --source <input-source>
        
#### Create model matrix instance from input data

    bin/modelmatrix-cli instance create [options] --definition-id <model-definition-id> --source <input-source>
    
| Option         | Example                       | Description                                       |
| -------------- | ----------------------------- | --------------------------------------------------|
| n, name        | -n "v123"                     | matrix model instance name                        |
| c, comment     | -c "test on new data"         | matrix model instance comment                     |
| concurrency    | --concurrency 10              | number of concurrent transformations (Spark jobs) |
| repartition    | --repartition 1000            | repartition input source                          |
| cache          | --cache true                  | cache input source                                |
    
### Examples    

    # Validate Hive table source
    bin/modelmatrix-cli instance validate \
          --definition-id 1 \
          --source hive://mm.clicks_2015_05_05
    
    # Create new instance if it's valid
    bin/modelmatrix-cli instance create \
          --name "2015-05-05" \
          --comment "latest clicks model" \
          --concurrency 10 \
          --cache true \
          --definition-id 1 \
          --source hive://mm.clicks_2015_05_05
    
    # Check that it appears in list of all instances
    bin/modelmatrix-cli instance list
    
    # Find it by name
    bin/modelmatrix-cli instance list --name 2015-05
    
    # View features 
    bin/modelmatrix-cli instance view features \
          --instance-id <id> # id returned from 'create' command
    
    # View columns
    bin/modelmatrix-cli instance view columns \
           --instance-id <id> # id returned from 'create' command
    
    # View ony geographic columns
    bin/modelmatrix-cli instance view columns \
          --group geographic \
          --instance-id <id> # id returned from 'create' command

## <a name="feature-extraction">Feature extraction</a>

#### Validate model matrix instance against input data

Check that input data compatible with model instance and can be "featurized"

    bin/modelmatrix-cli featurize validate --instance-id <model-instance-id> --source <input-source>
        
#### Build sparse feature matrix from input data

    bin/modelmatrix-cli featurize sparse \
          --instance id <model-instance-id> \
          --source <input-source> \
          --target <output-sink> \
          --id-column <id-column>

`<id-column>` - the name of a column that will be used as row id
    
### Examples    

    # Validate Hive table source
    bin/modelmatrix-cli featurize validate \
          --instance-id 1 \
          --source hive://mm.clicks_2015_05_05
    
    # Create sparse feature matrix in Hive
    bin/modelmatrix-cli featurize sparse \
          --instance-id 1 \
          --source hive://mm.clicks_2015_05_05 \
          --target hive://mm.clicks_features_2015_05_05 \
          --id-column auction_id

## <a name="source-sink">Source and Sink</a>

HDFS and Hive can be used as source or sink for Model Matrix CLI

    bin/modelmatrix-cli ... hive://mm.clicks_2015_05_05
    bin/modelmatrix-cli ... parquet://file:///Users/mm/clicks_2015_05_05.parquet
    bin/modelmatrix-cli ... parquet://hdfs:///Users/mm/clicks_2015_05_05.parquet

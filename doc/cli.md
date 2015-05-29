---
layout: default
title: Command Line Interface
---

# <a name="command-line-interface">Command Line Interface</a>

To get list of all available commands simply run `mm` without arguments:

    modelmatrix/mm

## <a name="specify-database">Specify Model Matrix Database</a>
    
If you want to use custom database settings you can provide them in 
separate database configuration and pass it to CLI with db options:
    
    # Inside datasci-db.conf
    datasci-db {
     url = "jdbc:postgresql://localhost/datasci?user=datasci&password=datasci"
     driver = org.postgresql.Driver
     keepAliveConnection = true      
    }    
    
and later pass additional options to CLI:
    
    ./mm <cli-command> --dbConf ./datasci-db.conf --dbName datasci-db

## <a name="mmc-definition">Model Matrix definitions</a>

#### List available model matrix definitions

    ./mm definition list

#### Find model matrix definition by name

    ./mm definition find <name>
    
#### View model matrix definition

    ./mm definition view <model-definition-id>    
         
#### Validate model matrix definition config

    ./mm definition validate <model-config>
        
#### Add model matrix definition

    ./mm definition add [options] <model-config>
    
| Option      | Example                       | Description                            |
| ----------- | ----------------------------- | -------------------------------------- |
| n, name     | -n "v123"                     | matrix model definition name           |
| c, comment  | -c "testing new model"        | matrix model definition comment        |
| f, features | -f "features"                 | features definition property in config |
    
### Examples    

    # Validate config
    ./mm definition validate ./model-matrix-v123.conf
    
    # Create new definition if it's valid
    ./mm definition add --name "v123" --comment "testing new model" ./model-matrix-v123.conf
    
    # Check that it appears in list of all definitions
    ./mm definition list
    
    # Find it by name
    ./mm definition find v123
    
    # View feature definitions
    ./mm definitions view <model-definition-id> # id returned from 'add' command

## <a name="mmc-instance">Model Matrix instances</a>

#### List available model matrix instances

    ./mm instance list

#### Find model matrix instance by name

    ./mm instance find <name>
    
#### View model matrix instance features

    ./mm instance view features <model-instance-id>    
    
#### View model matrix instance columns
    
    ./mm instance view columns <model-instance-id>
    
| Option      | Example                       | Description                            |
| ----------- | ----------------------------- | -------------------------------------- |
| f, feature  | -f ad_position                | filter by feature name                 |
| g, group    | -g geographic                 | filter by feature group                |
    
         
#### Validate model matrix definition against input data

Check that input data compatible with model definition and can be used to build model instance

    ./mm instance validate <model-definition-id> <input-source>
        
#### Create model matrix instance from input data

    ./mm instance create [options] <model-definition-id> <input-source>
    
| Option         | Example                       | Description                                       |
| -------------- | ----------------------------- | --------------------------------------------------|
| n, name        | -n "v123"                     | matrix model instance name                        |
| c, comment     | -c "test on new data"         | matrix model instance comment                     |
| t, concurrency | -t 10                         | number of concurrent transformations (Spark jobs) |
    
### Examples    

    # Validate Hive table source
    ./mm instance validate 1 hive://mm.clicks_2015_05_05
    
    # Create new instance if it's valid
    ./mm instance create --name "2015-05-05" --comment "latest clicks model" --concurrency 10 1 hive://mm.clicks_2015_05_05
    
    # Check that it appears in list of all instances
    ./mm instance list
    
    # Find it by name
    ./mm instance find 2015-05
    
    # View features 
    ./mm instance view features <id> # id returned from 'create' command
    
    # View columns
    ./mm instance view columns <id> # id returned from 'create' command
    
    # View ony geographic columns
    ./mm instance view columns --group geographic <id> # id returned from 'create' command

## <a name="feature-extraction">Feature extraction</a>

#### Validate model matrix instance against input data

Check that input data compatible with model instance and can be "featurized"

    ./mm featurize validate <model-instance-id> <input-source>
        
#### Build sparse feature matrix from input data

    ./mm featurize sparse <model-instance-id> <input-source> <output-sink> <id-column>

`<id-column>` - the name of a column that will be used as row id
    
### Examples    

    # Validate Hive table source
    ./mm featurize validate 1 hive://mm.clicks_2015_05_05
    
    # Create sparse feature matrix in Hive
    ./mm featurize sparse 1 hive://mm.clicks_2015_05_05 hive://mm.clicks_features_2015_05_05 auction_id

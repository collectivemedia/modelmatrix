---
layout: default
title: Getting Started
---

# Getting Started

Here is a very simple sample to get started with Model Matrix. It's assumed that you already installed
Model Matrix schema in your PostgreSQL database, if not yet check [installing schema](installing-schema.html) documentation.

## Requirements

- SBT 0.13.x
- Scala 2.10.4

## #1 Assembly application jar file

First it's required to prepare assembled jar file. In root directory run:

    sbt assembly
    
The cli assembled jar will be placed in:
  - `modelmatrix/modelmatrix-cli/target/scala-2.10/model-matrix-cli.jar`

## #2 Specify database configuration
    
If you store you model matrix data in non-default database refer [CLI documentation](cli.html#specify-database) on
how to provide custom database config.  
  

## #3 Check that Schema installed successfully

Run simple cli command to ensure that schema installed successfully
   
    # List available model matrix definitions
    ./mm definition list [--dbConf your.conf --dbName your-db-name]
    
For next steps I'll skip custom database config, just be aware that `dbConf` and `dbName` options supported by all CLI commands.       
    
## #4 Add new model matrix definition from config file

Using [example configuration](feature-matrix-config.html#example-configuration) stored in features.conf:
   
    ./mm definition add ./model.conf
    
This command will return model definition id, going forward I'm assuming that it is `1`    
                
## #5 View new model definition

    ./mm definition view 1
    
## #6 Check that definition can be used to build model instance from Hive
    
    ./mm instance validate 1 hive://mm.clicks_2015_05_05
     
## #7 Create model instance by applying definition to input data
     
It will calculate categorial and continuous features transformations based on shape of input data
     
     ./ mm instance create 1 hive://mm.clicks_2015_05_05 --name clicks --comment "getting started"
     
This command will return model instance id, going forward I'm assuming that it is `123`

## #8 View instance feature transformations

You can view what categorial and continuous transformations were computed from input data.

    ./mm instance view features 123
    ./mm instance view columns 123
    
## #9 Check that instance can be used for feature extraction

Check that model instance computed at previous step compatible with next day input data
    
    ./mm featurize validate 123 hive://mm.clicks_2015_05_06
    
## #10 Build sparse feature table in Hive
   
Apply model instance transformation to input data and build "featurized" sparse table in Hive
   
    ./mm featurize sparse 123 hive://mm.clicks_2015_05_06 hive://mm.clicks_sparse_features_2015_05_06 AUCTION_ID
   

## And much more…

More documentation on Command Line Interface is [here](cli.html).

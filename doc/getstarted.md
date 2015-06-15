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

## #1 Package Model Matrix CLI

First it's required to package CLI distribution. In root directory run:

    sbt universal:packageBin
    
The cli zip distribution will be placed in:
  - `modelmatrix/modelmatrix-cli/target/universal/model-matrix-cli-0.0.2.zip`
  
Unzip this file to start using Model Matrix  

## #2 Specify database configuration
    
If you store you model matrix data in non-default database check [CLI documentation](cli.html#specify-database) to read
how to provide custom database config.    

## #3 Check that Schema installed successfully

Run simple cli command to ensure that schema installed successfully
   
    # List available model matrix definitions
    bin/modelmatrix-cli definition list

## #4 Add new model matrix definition from config file

Using [example configuration](feature-matrix-config.html#example-configuration) stored in features.conf:
   
    bin/modelmatrix-cli definition add --config ./model.conf
    
This command will return model definition id, going forward I'm assuming that it is `1`    
                
## #5 View new model definition

    bin/modelmatrix-cli definition view --definition-id 1
    
## #6 Check that definition can be used to build model instance from Hive
    
    bin/modelmatrix-cli instance validate --definition-id 1 --source hive://mm.clicks_2015_05_05
     
## #7 Create model instance by applying definition to input data
     
It will calculate categorial and continuous features transformations based on shape of input data
     
     bin/modelmatrix-cli instance create \
           --definition-id 1 \
           --source hive://mm.clicks_2015_05_05 \
           --name clicks \
           --comment "getting started"
     
This command will return model instance id, going forward I'm assuming that it is `123`

## #8 View instance feature transformations

You can view what categorial and continuous transformations were computed from input data.

    bin/modelmatrix-cli instance view features --instance-id 123
    bin/modelmatrix-cli instance view columns --instance-id 123
    
## #9 Check that instance can be used for feature extraction

Check that model instance computed at previous step compatible with next day input data
    
    bin/modelmatrix-cli featurize validate \
          --instance-id 123 \
          --source hive://mm.clicks_2015_05_06
    
## #10 Build sparse feature table in Hive
   
Apply model instance transformation to input data and build "featurized" sparse table in Hive
   
    bin/modelmatrix-cli featurize sparse \
          --instance-id 123 \
          --source hive://mm.clicks_2015_05_06 \
          --target hive://mm.clicks_sparse_features_2015_05_06 \
          --id-column AUCTION_ID
   

## And much more…

More documentation on Command Line Interface is [here](cli.html).

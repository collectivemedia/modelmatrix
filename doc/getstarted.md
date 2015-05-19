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
    
## #4 Add new model matrix definition from config file

Using [example configuration](features-configuration.html#example-configuration) stored in features.conf:
   
    ./mm definition add features.conf
        
## #5 View new model definition
       
At previous step you'll get id of newly created model definition, you can check what is stored in model matrix
database
       
       ./mm definition view <model-definition-id>
    

## And much more…

More documentation on Command Line Interface is [here](cli.html).
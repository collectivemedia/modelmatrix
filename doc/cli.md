---
layout: default
title: Command Line Interface
---

# <a name="command-line-interface">Command Line Interface</a>

## <a name="specify-database">Specify Catalog Database</a>
    
If you want to use custom database settings you can provide them in 
separate database configuration and pass it to CLI with db options:
    
    # Inside datasci-db.conf
    datasci-db {
     url = "jdbc:postgresql://localhost/datasci?user=datasci&password=datasci"
     driver = org.postgresql.Driver
     keepAliveConnection = true      
    }    
    
and later pass additional options to CLI:
    
    mm <cli-command> --dbConf ./datasci-db.conf --dbName datasci-db

## <a name="mmc-definition">Model Matrix definitions</a>

##### List available model matrix definitions
    mm definition list

##### Find model matrix definition by name (like '%name%' under the hood)
    mm definition find "viewability optimization"
         
##### Validate model matrix definition config
    mm definition validate ./model-matrix-v123.conf
        
##### Add model matrix definition
    mm definition add ./model-matrix-v123.conf

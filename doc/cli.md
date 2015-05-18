---
layout: default
title: Command Line Interface
---

# <a name="command-line-interface">Command Line Interface</a>

## <a name="mmc-definition">Model Matrix definitions</a>

    # List available model matrix definitions
    mm definition list
     
    # List available model matrix definitions from non-default database
    mm definition list --dbName=staging-db --dbConfig=./staging-db.conf
    
    # Find model matrix definition by name (like '%name%' under the hood)
    mm definition find "viewability optimization"
     
    # Validate model matrix definition config
    mm definition validate ./model-matrix-v123.conf
    
    # Add model matrix definition
    mm definition add ./model-matrix-v123.conf

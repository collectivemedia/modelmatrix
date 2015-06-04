---
layout: default
title: Features Configuration
---

# Features Configuration

Model Matrix use [Typesafe Config](https://github.com/typesafehub/config) library for 
defining model matrix features and transformations.

Feature transformations documentations available in [Model Matrix philosophy](philosophy.html#feature-matrix) page.

## Extract expressions

Any valid [Spark DataFrame](https://spark.apache.org/docs/1.3.0/sql-programming-guide.html) 
(very limited subset of SQL) expression is supported as extract function for feature. 

    ad_performance = ${bins} {
      ...
      extract = cast(ctr as double)
      extract = ctr * 100
      extract = nvl(ctr, 0)
      ...
    }
    
Additional Model Matrix UDFs:
    
    os = ${top} {
      ...
      extract = concat('_', os_system, os_family)
      ...    
    }
    
    ad_day_of_week = ${top} {
      ...
      extract = day_of_week(ad_epoch_millis, 'UTC')
      ...
    }
    
    ad_hour_of_day = ${top} {
      ...
      extract = hour_of_day(ad_epoch_millis, 'UTC')
      ...
    }    
    
## Feature types

### <a name="identity-feature">Identity feature</a>

    identity_feature_name = {
      active = <active status>
      group = <feature group>
      extract = <extract expression>
      transform = "identity"
    }
   
### <a name="top-feature">Top feature</a>

    top_feature_name = {
      active = <active status>
      group = <feature group>
      extract = <extract expression>
      transform = "top"
      cover = <cover percentage>
      allOther = <include all other>
    }
      
### <a name="index-feature">Index feature</a>

    index_feature_name = {
      active = <active status>
      group = <feature group>
      extract = <extract expression>
      transform = "index"
      support = <support percentage>
      allOther = <include all other>
    }
    
### <a name="binning-feature">Binning feature</a>

    binning_feature_name = {
      active = <active status>
      group = <feature group>
      extract = <extract expression>
      transform = "bins"
      nbins = <target number of bins>
    }    
   

## <a name="example-configuration">Example Configuration</a>

    # Abstract Model Matrix features
    feature = { active = true }
    
    identity = ${feature} { 
      transform = "identity"
    }
    
    top = ${feature} { 
      transform = "top",
      cover = 95.0
      allOther = true
    }
    
    index = ${feature} { 
      transform = "index"
      support = 0.5
      allOther = true
    }
    
    bins = ${feature} {
      transform = "bins"
      nbins = 10
    }

    # Define concrete model matrix features
    features {

      ad_network = ${identity} {
        group = "advertisement"
        extract = "network"
      }

      ad_type = ${top} {
        group = "advertisement"
        extract = "type"
      }

      ad_size = ${index} {
        group = "advertisement"
        extract = "size"
      }

      ad_visibility = ${top} {
        active = false
        group = "advertisement"
        extract = "visibility"
      }
      
      ad_performance = ${bins} {
        active = true
        group = "performance"
        extract = "nvl(ctr, 0)"
        nbins = 5
      }
    }

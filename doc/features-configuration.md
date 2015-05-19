---
layout: default
title: Features Configuration
---

# Features Configuration

Model Matrix use [Typesafe Config](https://github.com/typesafehub/config) library for 
defining model matrix features and transformations.

Feature transformations documentations available in [Model Matrix philosophy](philosophy.html#feature-transformations) page.

## <a name="identity-feature">Identity feature</a>

    identity_feature_name = {
      active = <active status>
      group = <feature group>
      extract = <extract column>
      transform = "identity"
    }
   
## <a name="top-feature">Top feature</a>

    top_feature_name = {
      active = <active status>
      group = <feature group>
      extract = <extract column>
      transform = "top"
      percentage = <percentage>
      allOther = <include all other>
    }
      
## <a name="index-feature">Index feature</a>

    index_feature_name = {
      active = <active status>
      group = <feature group>
      extract = <extract column>
      transform = "index"
      percentage = <percentage>
      allOther = <include all other>
    }
   

## <a name="example-configuration">Example Configuration</a>

    # Abstract Model Matrix features
    feature = { active = true }
    identity = ${feature} { 
      transform = "identity"
    }
    top = ${feature} { 
      transform = "top",
      percentage = 95.0
      allOther = true
    }
    index = ${feature} { 
      transform = "index"
      percentage = 0.5
      allOther = true
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

    }
---
layout: default
title: Philosophy
---

# <a name="philosophy">Philosophy</a>

## <a name="feature-matrix">Feature Matrix</a>

Preparing the data for use as predictors in modeling has its own challenges. Dimensionality of 
the data needs to be reduced to address both the extreme sparsity of the data. The time structure of the data 
requires that the features defined at one point in time could be used for constructing the data 
set at another point in time. The features should be defined, added, and removed without 
having to modify the source code.

## <a name="feature-transformations">Feature Transformations</a>

### <a name="continuous-features">Continuous Features: Binning</a>

Continuous features may need to be transformed to binary format using binning to account for nonlinearity. 
In general, binning attempts to break a set of ordered values into evenly distributed groups, 
such that each group contains approximately the same number of values from the sample. 

### <a name="categorical-features">Categorial Features</a>
 
Categorical features must be transformed to binary format by creating a binary variable
for each categorical value. However, high-cardinality features, such as web pages where ads were delivered, 
would then translate into millions of predictor variables, the vast majority of which are extremely sparse.

#### <a name="top-coverage">Top Coverage</a>

Top coverage, is selecting categorical values by computing the count of distinct 
users for each value, sorting the values in descending order by the count of users, and choosing the 
top values from the resulting list such that the sum of the distinct user counts over these values 
covers c percent of all users, for example, selecting top geographic locations covering 99% of users. 

#### <a name="minimum-support">Minumum Support</a>

The minimum support method is selecting categorical values such that at least c percent of users have this value, 
for example, web sites that account for at least c percent of traffic. 

## <a name="model-matrix-catalog">Model Matrix Catalog</a>

Model Matrix catalog - is a persistent metadata storage where you can keep all your model matrix definitions, keep history of them, use
definition to build model matrix instance by applying it to input data, apply model matrix instance to input data for "featurization".

For model matrix instances it stores all categorical and binning transformation settings for each feature.



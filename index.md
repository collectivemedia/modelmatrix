---
layout: default
title: Model Matrix
---

# Machine Learning at Scale: Feature Engineering

#### **News** : [v{{ site.latestrelease }}](doc/releases.html#release) released

> For the impatient, head directly to [Getting Started](doc/getstarted.html)

Model Matrix is a framework/tool for solving large scale feature engineering
problem: building model features for machine learning. 

It's build on top [Spark DataFrames](https://spark.apache.org/docs/1.3.0/sql-programming-guide.html) and can 
read input data, and write 'featurized' from/to HDFS (CSV, Parquet) and Hive.

## Model Matrix Philosophy


#### <a href="doc/philosophy.html#feature-matrix" class="icon-circle-arrow-right"><span class="space5"><span class="spec-font-small">Feature Matrix</span></span></a>

#### <a href="doc/philosophy.html#feature-transformations" class="icon-circle-arrow-right"><span class="space5"><span class="spec-font-small">Feature Transformations</span></span></a>

#### <a href="doc/philosophy.html#model-matrix-catalog" class="icon-circle-arrow-right"><span class="space5"><span class="spec-font-small">Model Matrix Catalog</span></span></a>

<br/>

## Model Matrix CLI

With **Model Matrix CLI** you can control all lifecycle of Model Matrix:
 
 - adding MM definition from external configuration 
 - building MM instance based on features definition and input data 
 - applying MM transformations to input data for building "featurized" dataset
 
#### <a href="doc/cli.html#specify-database" class="icon-circle-arrow-right"><span class="space5"><span class="spec-font">Specify model matrix database</span></span></a>

#### <a href="doc/cli.html#mmc-definition" class="icon-circle-arrow-right"><span class="space5"><span class="spec-font">Work with model matrix definitions</span></span></a>

#### <a href="doc/cli.html#mmc-instance" class="icon-circle-arrow-right"><span class="space5"><span class="spec-font">Work with model matrix instances</span></span></a>

#### <a href="doc/cli.html#feature-extraction" class="icon-circle-arrow-right"><span class="space5"><span class="spec-font">Feature extraction</span></span></a>

<br/>

# Why Model Matrix?
   
Doing machine learning is fun and cool. Feature engineering (process of using domain knowledge of the data 
to create features that make machine learning algorithms work better) is tedious and boring. However good 
feature selection is bedrock to good models, none of machine learning techniques can produce predictive model 
if input features are bad.
   
Take for example this data set:

| visitor_id  | ad_campaign     | ad_id | ad_ctr     | pub_site            | state | city         | price | timestamp     | 
| ----------- | --------------- | ----- | ---------- | ------------------- | ----- | ------------ | ----- | ------------- |
| bob         | Nike_Sport      | 1     | 0.01       | http://bbc.com      | NY    | New York     | 0.17  | 1431032702135 |  
| bill        | Burgers_Co      | 2     | 0.005      | http://cnn.com      | CA    | Los Angeles  | 0.42  | 1431032705167 |
| mary        | Macys           | 3     | 0.015      | http://fashion.com  | CA    | Los Angeles  | 0.19  | 1431032708384 |

Producing a feature vector for every visitor (cookie) row and every piece of information about a 
visitor as an p-size vector, where p is the number of predictor variables multiplied by cardinality 
of each variable (number of states in US, number of unique websites, etc ...). It is impractical 
both from the data processing standpoint and because the resulting vector would only have 
about 1 in 100,000 non-zero elements.
   
| visitor_id  | Nike_Sport | Burgers_Co | Macys | NY  | CA  | ... | 
| ----------- | ---------- | ---------- | ----- | --- | --- | --- |
| bob         | 1.0        |            |       | 1.0 |     | ... |  
| bill        |            | 1.0        |       |     | 1.0 | ... |
| mary        |            |            | 1.0   |     | 1.0 | ... |
   
Model Matrix uses feature transformations (top, index, binning) to reduce dimensionality to arrive 
at between one and two thousand predictor variables, with data sparsity of about 1 in 10. It removes 
irrelevant and low frequency predictor values from the model, and transforms continuous 
variable into bins of the same size.
   
| visitor_id  | Nike | OtherAd | NY  | OtherState | price ∈ [0.01, 0.20) | price ∈ [0.20, 0.90) | ... |
| ----------- | ---- | ------- | --- | ---------- | -------------------- | -------------------- | --- | 
| bob         | 1.0  |         | 1.0 |            | 1.0                  |                      | ... |
| bill        |      | 1.0     |     | 1.0        |                      | 1.0                  | ... |
| mary        |      | 1.0     |     | 1.0        |                      | 1.0                  | ... |
   

You can rad more about motivation and modeling approach in [Machine Learning at Scale](http://arxiv.org/abs/1402.6076) paper.

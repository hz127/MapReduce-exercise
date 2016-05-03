## MapReduce implementation of Random Forest

library(randomForest)
library(rmr2)

# Create RF directory at HDFS
bin/hadoop dfs -mkdir /RF
# Uploading file training.csv at HDFS
bin/hadoop dfs -put temp/training.csv /RF/

#generate a new sample from the current block of data
RF.subsample <- function(k, input) {
  generate.sample <- function(i) {
    # generate N Poisson variables
    draws <- rpois(n=nrow(input), lambda=0.1)
    indices <- rep((1:nrow(input)), draws)
    # emit the rows and rbinding the data frames from different mappers together
    keyval(i, input[indices, ])
  }
  c.keyval(lapply(1:100, generate.sample))
}

fit.trees <- function(k, v) {
  rf <- randomForest(formula=model.formula, data=v, na.action=na.roughfix, ntree=10, do.trace=FALSE);
  # rf is a list so wrap it in another list to ensure that only
  # one object gets emitted. this is because keyval is vectorized
  keyval(k, list(forest=rf))
}

mapreduce(input="/RF/training.csv", map=RF.subsample, reduce=fit.trees, output="/RF/output")

mraw.forests <- values(from.dfs("/RF/output"))
forest <- do.call(combine, raw.forests)
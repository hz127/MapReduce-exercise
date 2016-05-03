## Gradient descent of logistic regression using MapReduce.

library(rmr2)

data_train <- data[1:8000,]
data_test <- data[8001:10000,]
nTarget <- ncol(data_train)
testdata <- to.dfs(as.matrix(data_train))

logistic.regression = function(input, iterations, dims, gamma){
  
  g = function(z) 1/(1 + exp(-z))
  
  lr.map = function(k,M){
    Y = M[ ,nTarget]
    X = M[ ,-nTarget]
    keyval(1, t(t(X)%*%(g(X%*%t(beta))-Y)))
  }
  ## The result is a list of 1*p vectors
  
  lr.reduce = function(k,Z)
    keyval(k, t(as.matrix(apply(Z, 2, sum))))
  ## sum over columns to give a 1*p vector, then transpose to a p*1 column
  
  beta = t(runif(dims, 0, 1))
  
  for(i in 1:iterations){
    gradient = values(
      from.dfs(
        mapreduce(input, map = lr.map, reduce =lr.reduce, combine = T)
      )
    )
    beta = beta - gamma*gradient
  }
  beta
}
## Output the beta vector
beta <- as.matrix(logistic.regression(testdata, iterations = 50, dims = nTarget-1, gamma = 0.05))

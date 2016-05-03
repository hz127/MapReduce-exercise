## MapReduce implimentation of K-means

library(rmr2)

kmeans.mr = function(P, num.clusters, num.iter) {
  ## if k clusters, C is k by 2 matrix, calculated the distance between point and k centers of clusters, 
  ## we get a 1 by k distances. there are 100 points, we get 100 by k matrix.
  dist.fun = function(C, P) {
    apply(C, 1, function(x) colSums((t(P) - x)^2))
  }
  
  kmeans.map = function(., P) {
    if(is.null(C)) {
      nearest = sample(1:num.clusters, nrow(P), replace = TRUE)
    }
    else {
      D = dist.fun(C, P)
      nearest = max.col(-D)
    }
    keyval(nearest, P) 
  }
  
  kmeans.reduce = function(., P) {
    t(as.matrix(apply(P, 2, mean)))
  }
  
  C = NULL
  for(i in 1:num.iter ) {
    C = values(
      from.dfs(
        mapreduce(P, map = kmeans.map, reduce = kmeans.reduce)))
  } 
  ## keep the number of centers the desired one (when centers are nearest to no points, they are lost). 
  ## we generate a point and rbind with the previous C to give desired k by 2 matrix
  if(nrow(C) < num.clusters) {
    C = 
      rbind(
        C,
        matrix(
          rnorm(
            (num.clusters - nrow(C)) * nrow(C)), 
          ncol = nrow(C)) %*% C) 
  }
  C
}

set.seed(0)

P = do.call(
  rbind, 
  rep(
    list(
      matrix(rnorm(10, sd = 10), ncol=2)), 20)) 
    + matrix(rnorm(200), ncol =2)

out = kmeans.mr(to.dfs(P), num.clusters = 3, num.iter = 5)


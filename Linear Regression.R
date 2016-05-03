## MapReduce implementation of linear regression

X <- matrix(rnorm(2000), ncol = 10)
X.index <- to.dfs(cbind(1:nrow(X), X))
y <- as.matrix(rnorm(200))

Sum <- function(., YY)
  keyval(1, list(Reduce('+', YY)))

XtXmap <- function(., Xi) {
  Xi = Xi[,-1]
  keyval(1, list(t(Xi) %*% Xi))
}

XtX <- values(from.dfs(
  mapreduce(input = X.index,
            map = XtXmap,
            reduce = Sum,
            combine = TRUE)))[[1]]

Xtymap <- function(., Xi) { 
  yi = y[Xi[,1],] 
  Xi = Xi[,-1] 
  keyval(1, list(t(Xi) %*% yi))} 
} 

Xty <- values(from.dfs(
    mapreduce(input = X.index, 
              map = Xtymap, 
              reduce = Sum, 
              combine = TRUE)))[[1]] 

solve(XtX, Xty)
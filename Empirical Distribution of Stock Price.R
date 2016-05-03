## Ploting the empirical distribution of stock price in the MapReduce framework.

library(rmr2)

data <- read.csv("project1.csv", header = TRUE, sep = ",")
data1<- to.dfs(data)

# Method 1
# map function: return = (close - open) / open
# map fuction
map <- function(k,v) {
  Return1 <- round(((v[[5]]-v[[2]]) / v[[2]]), 5)
  return(keyval(Return1, 1))
}

# Method 2
## map function: return = close2 - close1 / close1
Return2 <- NULL
map <- function(k,v) {
  n <- sum(!is.na(v[[5]]))
  for(i in 2:n)
    Return2[i-1] <- round(((v[i,"close"]-v[i-1,"close"]) / v[i,"close"]), 5)
  return(keyval(Return2, 1))
}

## reduce function
reduce <- function(Return1, counts) { 
  keyval(Return1, sum(counts))
}

ReturnFreq <- function (input, output=NULL) { 
  mapreduce(input=data1, output=output,map=map, reduce=reduce)
}

hdfs.root = ''
hdfs.data = file.path(hdfs.root, 'data1') 
hdfs.out = file.path(hdfs.root, 'output') 
out = ReturnFreq(hdfs.data, hdfs.out)

results = from.dfs(out)
results.df = as.data.frame(results, stringsAsFactors=F) 
colnames(results.df) = c('return1', 'count')
plot(results.df, type="h")

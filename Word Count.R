# Word Count

library(rmr2) 

## map function
map = function(k,lines) {
  words.list = strsplit(lines, '\\s') 
  words = unlist(words.list)
  return(keyval(words, 1))
}

## reduce function
reduce = function(word, counts) { 
  keyval(word, sum(counts))
}

wordcount = function (input, output=NULL) { 
  mapreduce(input=input, output=output, input.format="text", 
            map=map, reduce=reduce)
}

## delete previous result if any
system("/Users/hadoop/hadoop-2.7.1/bin/hadoop fs -rm -r wordcount/out")

## Submit job
hdfs.root = ''
hdfs.data = file.path(hdfs.root, 'input') 
hdfs.out = file.path(hdfs.root, 'output') 
out = wordcount(hdfs.data, hdfs.out)

## Fetch results from HDFS
results = from.dfs(out)

## check top 30 frequent words
results.df = as.data.frame(results, stringsAsFactors=F) 
colnames(results.df) = c('word', 'count') 
head(results.df[order(results.df$count, decreasing=T), ], 30)

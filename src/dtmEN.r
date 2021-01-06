library(dplyr) 
library(sparklyr)
Sys.setenv(SPARK_HOME="/usr/local/spark/spark-2.4.5-bin-hadoop2.7")
config <- spark_config()
config$spark.executor.memory = "32G"
config$spark.driver.memory = "32G"
config$spark.cores.max = "96"
config$spark.yarn.executor.memoryOverhead = "4096"
sc <- spark_connect(master = "spark://hnamenode:7077", config = config)

# tokens = spark_read_parquet(sc, "tokens", "/home/sum2020/yelp/dtm/tfidf")
# tokens
# count(tokens)
# 
# token_en = filter(tokens, !doc_id %in% notEN)
# spark_write_parquet(token_en, "/home/sum2020/yelp14/pq/token_en")

tEN = spark_read_parquet(sc, "ten", "/home/sum2020/yelp14/pq/token_en")
tEN %>% summarise_at(vars(doc_id,token), n_distinct)
#  doc_id   token
# 8021122 1284591
terms = tEN %>% group_by(token) %>% summarise(
  t_freq = sum(freq),
  a_tfidf = mean(tfidf),
  n_docs = n() 
  )
A = collect(terms)

library(stringr)
D = A %>% filter(
  str_detect(token, "^[A-Za-z]{2,}$"),
  t_freq >= 30, n_docs >=10, a_tfidf <= 1,
  )
D = subset(D, a_tfidf > median(a_tfidf))
save(A, D, file='Terms.rdata')
  
x = D$token
dtmEN = filter(tEN, token %in% x)
spark_write_parquet(dtmEN, "/home/sum2020/yelp14/pq/dtmEN")
count(dtmEN)
dtmEN %>% summarise_at(vars(doc_id,token), n_distinct)

spark_disconnect(sc)


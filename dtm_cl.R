pacman::p_load(tm)
source("/home/tonychuo/_sparklyr.R")
tfidf = spark_read_parquet(sc, "tfidf", "hdfs:///home/sum2020/yelp/dtm/tfidf_2")

# 1. clean the dtm more clearly ----
tfidf %>%
  summarise_at(
    vars(doc_id,lemma), n_distinct
  ) 
# doc_id   lemma
# <dbl>   <dbl>
# 8021122 1205321


# token 1284591
tfidf %>% count #572843001

terms = tfidf %>% filter(rlike(lemma, "[A-Za-z]")) %>% 
  group_by(lemma) %>% summarise(
    a_tfidf = mean(tfidf),
    t_freq = sum(freq),
    n_doc = n()
  )
count(terms) #1123545

TS = terms %>% collect

summary(TS$a_tfidf)
quantile(TS$t_freq, probs = c(seq(0.75, 0.95, 0.05),0.99))

#use TS to subset important token
TS %>% filter(
  #a_tfidf >= quantile(a_tfidf,0.75),
  t_freq >= 200,
  !lemma %in% stopwords()
) %>% arrange(desc(a_tfidf)) %>% 
  count # 7402
# 32403



stopwd <- stopwords()

dtm <- inner_join(
  tfidf,
  dplyr::select(terms,-n_doc) %>% filter(
    t_freq >= 200,
    !lemma %in% stopwd
  )) 
count(dtm) 
# 359754154

dtm %>% summarise_at(
  vars(doc_id,lemma), n_distinct
) 
# doc_id lemma
# <dbl> <dbl>
#  8019270 32403

dtm <- dtm %>% select(doc_id, lemma, freq, n_docs, tfidf)

spark_write_parquet(dtm, path = "/home/sum2020/yelp/matrix", mode = "overwrite")


# 2. read dtm ----
dtm <- spark_read_parquet(sc, "dtm", "/home/sum2020/yelp/matrix")
head(dtm)

dtm %>%
  summarise_at(
    vars(doc_id,lemma), n_distinct
  )
# Source: spark<?> [?? x 2]
# doc_id lemma
# <dbl> <dbl>
#   1 8019270 32403
library(stringr)
stopwd <- c("go",
            "come",
            
            tm::stopwords())
stopwd <- c(stopwd, str_to_upper(stopwd), str_to_title(stopwd), str_to_sentence(stopwd))

dtm <- dtm %>% 
  filter(!lemma %in% stopwd)
dtm %>% count()

spark_write_parquet(dtm, path = "/home/sum2020/yelp/matrix", mode = "overwrite")

rid <- rev_biz_id %>%
  filter(lubridate::year(date)==2018,
         business_id %in% idx
         ) %>%
  pull(rid)

dtm %>% 
  select(doc_id, lemma, freq) %>% 
  filter(as.integer(doc_id) %in% rid) %>% 
  group_by(lemma) %>% 
  summarise(freq = sum(freq))


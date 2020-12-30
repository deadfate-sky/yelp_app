source('/home/b056090027/sparkLink.R') #sc

### load spark ----
dtmEN = spark_read_parquet(sc, "/home/sum2020/yelp14/pq/dtmEN")
count(dtmEN)
dtmEN %>% summarise_at(vars(doc_id,token), n_distinct)


### choose two restaurants ----


source("/home/b056090027/yelp14/wc/load_rev.R")

# do filter
head(dtmEN)
rid = as.character(rev$rid)
token_ice_juice = dtmEN %>% 
  filter(doc_id %in% rid)
count(token_ice_juice) # 510995
head(token_ice_juice)
# doc_id  token      freq n_tokens      tf n_docs   idf  tfidf
# 1189423 Ahwatukee     2      426 0.00469   3555  7.72 0.0363
# 4218900 Ahwatukee     1       68 0.0147    3555  7.72 0.114 
# 450722  Ahwatukee     1       47 0.0213    3555  7.72 0.164 
# 4673672 Ahwatukee     1       32 0.0312    3555  7.72 0.241 
# 3564554 Ahwatukee     1      345 0.00290   3555  7.72 0.0224
# 1683337 Ahwatukee     1       35 0.0286    3555  7.72 0.221 

token_ice_juice %>% summarise_at(vars(doc_id,token), n_distinct)
# doc_id token
# 212755 27117


# try collect
token_ice_juice1 <- token_ice_juice %>%
  select(doc_id:freq) %>% 
  mutate(doc_id = as.numeric(doc_id)) %>% 
  filter(doc_id < 4013221)

token_ice_juice_1 <- collect(token_ice_juice1)
save(token_ice_juice_1, file = '/home/b056090027/yelp14/wc/data/ice_juice_1.RData')


token_ice_juice2 <- token_ice_juice %>%
  select(doc_id:freq) %>% 
  mutate(doc_id = as.numeric(doc_id)) %>% 
  filter(doc_id > 4013221)
token_ice_juice_2 <- collect(token_ice_juice2)
save(token_ice_juice_2, file = '/home/b056090027/yelp14/wc/data/ice_juice_2.RData')

spark_disconnect(sc)

# save the files

load(file = '/home/b056090027/yelp14/wc/data/ice_juice_1.RData')
load(file = '/home/b056090027/yelp14/wc/data/ice_juice_2.RData')
token_ice_juice <- rbind(token_ice_juice_1, token_ice_juice_2)
token_ice_juice <- token_ice_juice %>% mutate(doc_id = as.character(doc_id))
save(token_ice_juice, file = '/home/b056090027/yelp14/wc/data/ice_juice.RData')



########################################################################
### draw the wc ----
#load and clean data
pacman::p_load(stringr, dplyr, wordcloud)
load('/home/b056090027/yelp14/wc/data/ice_juice.RData')
source("/home/b056090027/yelp14/wc/load_rev.R")

# count the y: tw avg of years
token_ice_juice <- token_ice_juice %>% 
  left_join(select(rev, -business_id), by = c('doc_id' = 'rid'))

token_wt <- token_ice_juice %>%
  group_by(token) %>%
  mutate(size = n()) %>%
  # count each token size
  
  group_by(token, year) %>%
  summarise(n = n(), size = first(size), wt = n / first(size)) %>%
  # count each year weight of the token
  ungroup() %>%
  
  mutate(wt_yr = year * wt) %>%
  group_by(token) %>%
  summarise(yr = sum(wt_yr), size = first(size))
  # sum each token weight avg of year
  # result
token_wt

Y_ice <- token_ice_juice %>% 
  filter(ice) %>% 
  group_by(token) %>% 
  summarise(ice_freq = sum(freq))

Y_juice <- token_ice_juice %>% 
  filter(juice) %>% 
  group_by(token) %>% 
  summarise(juice_freq = sum(freq))

Y <- token_wt %>% 
  left_join(Y_ice) %>% 
  left_join(Y_juice) %>% 
  mutate(freq = ice_freq / (ice_freq + juice_freq)) %>% 
  as.data.frame()

head(Y)
Y[is.na(Y)] = 0

# choose the amount of words
head(Y)
summary(Y)
hist(Y$yr)
hist(Y$size)
hist(log(Y$size, 50))

count(Y, size, sort = TRUE) %>% top_n(30)

Y %>% arrange(desc(size)) %>%
  head(100) %>%   
  slice(5:100) %>% 
  pull(size) %>% 
  scale(center = FALSE) %>% 
  plot(type = 'l')

# wc data
Y_wc = Y %>% 
  arrange(desc(size)) %>% 
  slice(5:100) 

# render plot

summary(Y_wc)
hist(scale(Y_wc$size, center = FALSE))
plot(scale(Y_wc$size, center = FALSE), type = 'l')

textplot(Y_wc$freq, 
         Y_wc$yr, 
         Y_wc$token, 
         cex = scale(Y_wc$size, center = FALSE),
         show.lines = FALSE)

png('wc2.png', width = 2400, height = 1600, res = 240)
textplot(Y_wc$freq, 
         Y_wc$yr, 
         Y_wc$token, 
         cex = scale(Y_wc$size, center = FALSE),
         show.lines = FALSE)

dev.off()


# Ice Cream & Frozen Yogurt vs Juice Bars & Smoothies
# y := weighted average of year 
#   data %>% count(term, year) %>%  group_by(term) %>% summarise(year*n/sum(n))?
# x := freq1/(freq1+freq2) 
# size = freq
# hist(x); hist(y)); hist(size);




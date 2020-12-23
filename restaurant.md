```R
source('/home/b056090027/sparkLink.R') #sc

### load spark ----
dtmEN = spark_read_parquet(sc, "/home/sum2020/yelp14/pq/dtmEN")
count(dtmEN)
dtmEN %>% summarise_at(vars(doc_id,token), n_distinct)


### choose two restaurants ----
```

```R
# load
pacman::p_load(stringr)
load("/home/sum2020/yelp14/data/biz.rdata")
load("/home/sum2020/yelp14/data/rev.rdata")
rm(A3,B3,biz)
gc()

# find Thai and Vietnamese restaurants rid
str_subset(colnames(Bmx),"Viet")
str_subset(colnames(Bmx),"Thai")
sum(Bmx[,"Restaurants"] & Bmx[,"Vietnamese"])


thai = rownames(Bmx)[ Bmx[,"Restaurants"] & Bmx[,"Thai"] ]
viet = rownames(Bmx)[ Bmx[,"Restaurants"] & Bmx[,"Vietnamese"] ]
intersect(thai, viet) %>% length

rev = rev[,c('rid', "business_id")]
rev = filter(rev, business_id %in% union(thai, viet))
rev$thai = rev$business_id %in% thai
rev$viet = rev$business_id %in% viet
```
這邊的rev在後面會用到

```R
# do filter
head(dtmEN)
rid = as.character(rev$rid)
token_rest = dtmEN %>% 
  filter(doc_id %in% rid)
count(token_rest) # 464866
token_rest %>% summarise_at(vars(doc_id,token), n_distinct)
# doc_id token
# 189362 21770

rest = collect(token_rest)
spark_disconnect(sc)
#save(rest, file = 'data/restaurant.RData')
```

這邊是做完spark的filter之後，開始用選出來的token，計算各自在泰國跟越南店被提到的次數
然後畫成帶座標的文字雲
目前x軸是泰國詞頻減越南詞頻，y軸是總詞頻。所以往左泰國，往右越南
y軸則是可以確認這個token的總聲量
```R
### draw the wc
# clean data
load(file = 'data/restaurant.RData')
pacman::p_load(stringr, dplyr, wordcloud)

Y_thai = rest %>% 
  filter(doc_id %in% rev$rid[rev$thai]) %>% 
  group_by(token) %>% 
  summarise(thai_freq = sum(freq),
            thai_avg_tfidf = mean(tfidf))

Y_viet = rest %>% 
  filter(doc_id %in% rev$rid[rev$viet]) %>% 
  group_by(token) %>% 
  summarise(viet_freq = sum(freq),
            viet_avg_tfidf = mean(tfidf))

Y = rest %>% 
  group_by(token) %>% 
  summarise(y_freq = sum(freq),
            avg_tfidf = mean(tfidf)) %>% 
  left_join(Y_thai) %>% 
  left_join(Y_viet)

Y = as.data.frame(Y)
Y[is.na(Y)] = 0
Y$x_freq = Y$thai_freq - Y$viet_freq

# choose the amount of words
Y_wc = Y %>% 
  arrange(desc(y_freq)) %>% 
  slice(6:100) 

Y %>% 
  arrange(desc(y_freq)) %>% 
  head(20)
# render plot

summary(Y_wc)
hist(sqrt(Y_wc$y_freq))
hist(Y_wc$x_freq)

png('wc.png', width = 1600, height = 1600, res = 240)
textplot(Y_wc$x_freq, Y_wc$y_freq, Y_wc$token,cex = 0.6 ,show.lines = FALSE)
dev.off()
```

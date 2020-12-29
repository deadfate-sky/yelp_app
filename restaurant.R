# source('/home/b056090027/sparkLink.R') #sc

### load spark ----
# dtmEN = spark_read_parquet(sc, "/home/sum2020/yelp14/pq/dtmEN")
# count(dtmEN)
# dtmEN %>% summarise_at(vars(doc_id,token), n_distinct)


### choose two restaurants ----

# load
pacman::p_load(stringr, dplyr, wordcloud)

load("data/Bmx.RData")
load("data/rev_id.RData")
rev <- rev_biz_id
rm(rev_biz_id)
gc()

# find Thai and Vietnamese restaurants rid
str_subset(colnames(Bmx), "Thai")
str_subset(colnames(Bmx), "Viet")
sum(Bmx[, "Ice Cream & Frozen Yogurt"] & Bmx[, "Juice Bars & Smoothies"])
thai <- rownames(Bmx)[Bmx[, "Restaurants"] & Bmx[, "Thai"]]
viet <- rownames(Bmx)[Bmx[, "Restaurants"] & Bmx[, "Vietnamese"]]
intersect(thai, viet) %>% length()


rev <- filter(rev, business_id %in% union(thai, viet))
rev$thai <- rev$business_id %in% thai
rev$viet <- rev$business_id %in% viet
rev$year <- lubridate::year(rev$date)
rev$rid <- as.character(rev$rid)


# # do filter
# head(dtmEN)
# rid = as.character(rev$rid)
# token_rest = dtmEN %>%
#   filter(doc_id %in% rid)
# count(token_rest) # 510995
# token_rest %>% summarise_at(vars(doc_id,token), n_distinct)
# # doc_id token
# # 212755 27117
#
# spark_write_parquet(token_rest, "/home/sum2020/yelp14/pq/rest")




# rest = collect(token_rest)
# spark_disconnect(sc)
# save(rest, file = 'data/restaurant.RData')

### draw the wc
# clean data
load(file = "data/restaurant.RData")
pacman::p_load(stringr, dplyr, wordcloud)

# Ice Cream & Frozen Yogurt vs Juice Bars & Smoothies
# y := weighted average of year
#   y在下方最後的結果是`yr`，算法是算出token在每年的比例，再乘上年份
#   換句話說就是年份的加權平均，越大代表近年討論較多

# x := freq1/(freq1+freq2)
#   x就是freq

# size = sum(下面用`sum`表示)
# hist(x); hist(y)); hist(size);

load(file = "data/restaurant.RData")
head(rest)

token_sum <- rest %>%
  select(-(n_tokens:tfidf)) %>%
  left_join(select(rev, -business_id), by = c("doc_id" = "rid")) %>%
  count(token, name = "sum")

token_wt <- rest %>%
  select(-(n_tokens:tfidf)) %>%
  left_join(select(rev, -business_id), by = c("doc_id" = "rid")) %>%
  left_join(token_sum) %>%
  group_by(token, year) %>%
  summarise(n = n(), sum = first(sum), wt = n / sum) %>%
  ungroup()

token_wt <- token_wt %>%
  mutate(yr_wt = year * wt) %>%
  group_by(token) %>%
  summarise(yr = sum(yr_wt))
# 這邊是算出個token的年份加權，也就是拿來畫圖的y


Y_thai <- rest %>%
  filter(doc_id %in% rev$rid[rev$thai]) %>%
  group_by(token) %>%
  summarise(thai_freq = sum(freq))

Y_viet <- rest %>%
  filter(doc_id %in% rev$rid[rev$viet]) %>%
  group_by(token) %>%
  summarise(viet_freq = sum(freq))
# 這兩個是要計算token在泰國跟越南餐廳各自提到的次數

Y <- token_wt %>%
  left_join(Y_thai) %>%
  left_join(Y_viet) %>%
  as.data.frame()

Y[is.na(Y)] <- 0
Y$freq <- Y$thai_freq / (Y$thai_freq + Y$viet_freq)
Y <- left_join(Y, token_sum)

# choose the amount of words
head(Y)
summary(Y)
hist(Y$yr)
hist(Y$freq)
hist(log(Y$sum, 50))
count(Y, sum, sort = TRUE) %>% top_n(30)

# 先選個100個詞瞧瞧
Y_wc <- Y %>%
  arrange(desc(sum)) %>%
  slice(6:100)
head(Y_wc)

plot(scale(Y_wc$sum, center = FALSE), type = "l")


# render plot
textplot(
  Y_wc$freq,
  Y_wc$yr,
  Y_wc$token,
  cex = scale(Y_wc$sum, center = FALSE),
  show.lines = FALSE
)


# save the image
png("wc.png", width = 2400, height = 1200, res = 160)
textplot(
  Y_wc$freq,
  Y_wc$yr,
  Y_wc$token,
  cex = scale(Y_wc$sum, center = FALSE),
  show.lines = FALSE
)
dev.off()

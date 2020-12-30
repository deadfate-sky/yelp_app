# load
pacman::p_load(dplyr)
load("/home/sum2020/yelp14/data/biz.rdata")
load("/home/sum2020/yelp14/data/rev.rdata")
rm(A3,B3,biz)
gc()

# find Thai and Vietnamese restaurants rid
str_subset(colnames(Bmx),"Men")
str_subset(colnames(Bmx),"Wom")
sum(Bmx[,"Ice Cream & Frozen Yogurt"] & Bmx[,"Juice Bars & Smoothies"])


ice = rownames(Bmx)[ Bmx[,"Ice Cream & Frozen Yogurt"]==1 ]
juice = rownames(Bmx)[ Bmx[,"Juice Bars & Smoothies"]==1 ]
intersect(ice, juice) %>% length


rev = rev[,c('rid', "business_id", "date")]
rev = filter(rev, business_id %in% union(ice, juice))
rev$rid = as.character(rev$rid)
rev$ice = rev$business_id %in% ice
rev$juice = rev$business_id %in% juice
rev$year = lubridate::year(rev$date)

rm(Bmx)
cat("finish loading")
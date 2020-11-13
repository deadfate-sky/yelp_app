load("/home/sum2020/yelp14/data/biz.rdata")
load("/home/sum2020/yelp14/data/rev.rdata")


pacman::p_load(dplyr, stringr, lubridate)
rev_biz_id<- rev[,c("rid", "business_id", "date")]
# save(rev_biz_id, file = "/home/b056090027/yelp14/data/rev_id.RData")
# save(Bmx, file = "/home/b056090027/yelp14/Bmx.RData")

###############################

# How to filter ----

lubridate::year(rev_biz_id$date)==2018
x <- unique(lubridate::year(rev_biz_id$date))
sort(x)==2004:2019
rownames(Bmx[1:5,1:5])

cat <- colnames(Bmx)

hist(rowSums(Bmx))

tmp <- sample(cat, 2) #select

as.matrix(Bmx[,which(cat %in% tmp)])

idx <- rownames(Bmx)[rowSums(as.matrix(Bmx[,which(cat %in% tmp)]))>0]
rev_biz_id %>% select(business_id)
dplyr::filter(rev_biz_id, business_id %in% idx) %>% 
  pull(rid)


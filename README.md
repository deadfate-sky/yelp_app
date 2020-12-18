# yelp_app

20201214 
先把非英文的review清掉
把group by token的terms做出來，然後再用regex把term裡的符號、亂碼清理掉
看要用哪些條件（avg tfidf, N of docs, freq）篩選terms，已經篩選之後的效果。
把選好的terms存起來

拿這個去filter(tfidf)的token
* 接下來：

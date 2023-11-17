from pyspark import SparkContext
sc = SparkContext('local', 'test')


def header_to_map(header):
    list = header[1:-1].split('","')
    map = {}
    for i in range(len(list)):
        map[list[i]] = i
    return map


tweets = sc.textFile('file:///home/shakalyan/unik/4kurs/bigdata/5/tweets/ira_tweets_csv_hashed.csv')
tweets_header = tweets.first()
tweets_hmap = header_to_map(tweets_header)
tweets_rows = tweets.filter(lambda row: row != tweets_header)
tweets_records = tweets_rows.map(lambda r: r[1:-1].split('","'))

users = sc.textFile('file:///home/shakalyan/unik/4kurs/bigdata/5/tweets/ira_users_csv_hashed')
users_header = users.first()
users_hmap = header_to_map(users_header)
users_rows = users.filter(lambda row: row != users_header)
users_records = users_rows.map(lambda r: r[1:-1].split('","'))


tweets_tuple = tweets_records.map(lambda r: (r[tweets_hmap['userid']], [r[tweets_hmap['in_reply_to_tweetid']], r[tweets_hmap['reply_count']]]))
min_users_tweets_cnt = 10

twt_cnt     = tweets_tuple.aggregateByKey(0, lambda u, v: u + 1, lambda u1, u2: u1 + u2)\
                          .filter(lambda t: t[1] > min_users_tweets_cnt)
end_twt_cnt = tweets_tuple.filter(lambda t: t[1][0] and (t[1][1] == u'' or t[1][1] == u'0'))\
                          .aggregateByKey(0, lambda u, v: u + 1, lambda u1, u2: u1 + u2)

coeffs    = twt_cnt.join(end_twt_cnt).map(lambda t: (t[0], (float(t[1][1]) / t[1][0], t[1][0])))
max_coeff = coeffs.aggregate((0, 0), lambda u, t: max(u, t[1]), lambda u1, u2: max(u1, u2))[0]

result = coeffs.filter(lambda t: t[1][0] == max_coeff)
print(result)
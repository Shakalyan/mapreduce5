from pyspark.sql import SparkSession
spark = SparkSession.builder.master('local').appName('test').getOrCreate()

query = \
"""
WITH 
twt_cnt AS (
    SELECT userid, COUNT(*) as tcnt FROM global_temp.tweets
    GROUP BY userid
),
end_twt_cnt AS (
    SELECT userid, COUNT(*) as etcnt FROM global_temp.tweets
    WHERE in_reply_to_tweetid IS NOT NULL and reply_count = 0
    GROUP BY userid
),
coeffs AS (
    SELECT twt_cnt.userid, (etcnt / tcnt) as coeff FROM twt_cnt
    JOIN end_twt_cnt ON twt_cnt.userid = end_twt_cnt.userid
    WHERE twt_cnt.tcnt > 10
),
max_coeff AS (
    SELECT MAX(coeff) as coeff FROM coeffs
)
SELECT global_temp.users.userid, global_temp.users.user_display_name, global_temp.users.user_profile_description FROM global_temp.users
JOIN coeffs ON coeffs.userid = global_temp.users.userid
JOIN max_coeff ON max_coeff.coeff = coeffs.coeff
"""

users = spark.read.csv("file:///home/shakalyan/unik/4kurs/bigdata/5/tweets/ira_users_csv_hashed", inferSchema=True, header=True, sep=',', quote="\"", escape="\"")
users.createGlobalTempView("users")

tweets = spark.read.csv("file:///home/shakalyan/unik/4kurs/bigdata/5/tweets/ira_tweets_csv_hashed.csv", inferSchema=True, header=True, sep=',', quote="\"", escape="\"")
tweets.createGlobalTempView("tweets")

res = spark.sql(query.replace('\n', ' ').replace('\t', ' '))
res.write.csv('sql_out')
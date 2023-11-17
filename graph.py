from pyspark.sql import SparkSession
from pyspark import SparkContext
from graphframes import *


def header_to_map(header):
    list = header[1:-1].split('","')
    map = {}
    for i in range(len(list)):
        map[list[i]] = i
    return map


sc = SparkContext('local', 'test')
sc.setCheckpointDir("/tmp")
ss = SparkSession.builder.master('local').appName('test').getOrCreate()

tweets = sc.textFile('file:///home/shakalyan/unik/4kurs/bigdata/5/tweets/twsmall.csv')
tweets_header = tweets.first()
tweets_hmap = header_to_map(tweets_header)
tweets_rows = tweets.filter(lambda row: row != tweets_header)
tweets_records = tweets_rows.map(lambda r: r[1:-1].split('","'))

vertices = tweets_records.map(lambda r: (r[tweets_hmap['tweetid']], r[tweets_hmap['userid']]))
edges    = tweets_records.map(lambda r: (r[tweets_hmap['tweetid']], r[tweets_hmap['in_reply_to_tweetid']]))

vertices_df = ss.createDataFrame(vertices, ['id', 'userid'])
edges_df    = ss.createDataFrame(edges, ['src', 'dst'])

graph = GraphFrame(vertices_df, edges_df)

cc = graph.connectedComponents()
cc_sorted = cc.groupBy('component').count().sort('count', ascending=False)
N = 1
component = cc_sorted.take(1)[0].component

cc_vertices = cc.where("component = " + str(component))

cc_vertices.join(edges_df, cc_vertices.id == edges_df.src, 'leftouter').where("dst = ''").show()
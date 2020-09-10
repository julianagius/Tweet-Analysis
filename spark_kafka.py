import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import re
from pymongo import MongoClient

'''
Term Frequency using Spark Streaming

spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.5 spark_kafka.py 172.27.1.16:9092 covid some_mongo
spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.5 spark_kafka.py 172.27.1.16:9092 blm some_mongo
'''

# remove punctuation + numbers and convert text to lowercase
def prepro(text):
	text = re.sub(r'https?:\/\/.*[\r\n]*', '', text)
	text = re.sub(r'\\n', ' ', text)
	text = re.sub(r'\\u', ' ', text)

	punc_and_num='!"#$%&\'()*+,./:;<=>?@[\\]^_`{|}~-0123456789'
	lowercased_str = text.lower()
	for ch in punc_and_num:
		lowercased_str = lowercased_str.replace(ch, '')

	return lowercased_str

# insert record into MongoDB
def update_word_mongodb(my_tuple):
	client = MongoClient('localhost', 27017)
	db = client[db_name]
	collection_name = topic+'_wc'
	coll = db[collection_name]

	try:
		coll.update({"_id": my_tuple[0]}, {"$inc": {"count": my_tuple[1]} }, upsert=True)
		client.close()
	except:
		print("Could not update record in Mongo collection: " + topic + "_wc")


if __name__ == "__main__":
	sc = SparkContext(appName="StreamingDirectKafka")
	sc.setLogLevel("ERROR")
	ssc = StreamingContext(sc, 10)
 
	brokers, topic, db_name = sys.argv[1:]
	kvs = KafkaUtils.createDirectStream(ssc, [topic],{"metadata.broker.list": brokers})
	lines = kvs.map(lambda x: prepro(x[1]))
	counts = lines.flatMap(lambda line: line.split()) \
	              .map(lambda word: (word, 1)) \
	              .reduceByKey(lambda a, b: a+b)

	counts.pprint()
	counts.foreachRDD(lambda rdd: rdd.foreach(update_word_mongodb))

	ssc.start()
	ssc.awaitTermination()
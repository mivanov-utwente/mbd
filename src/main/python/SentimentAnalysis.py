#!/usr/bin/env python
import sys
import os
import json
import pyspark
import re

pypath = '/home/s1753398/ctit-spark/src/main/python'

def calculate_sentiment(row):
  if pypath not in sys.path:
    sys.path.insert(1, pypath)
  from pattern.text.nl import sentiment
  entry = row.asDict()
  entry['description_score'], entry['text_score'] = sentiment(entry['description']), sentiment(entry['text'])
  return entry

# add actual job
def doJob(rdd):
  return rdd.map(calculate_sentiment) \
            .map(json.dumps)

def main():
  script_path = os.path.abspath(os.path.dirname(__file__))
  # parse arguments 
  in_dir, out_dir = sys.argv[1:]
  
  conf = pyspark.SparkConf().setAppName("%s %s %s" % (os.path.basename(__file__), in_dir, out_dir))
  sc = pyspark.SparkContext(conf=conf)
  sqlContext = pyspark.sql.SQLContext(sc)
  
  # invoke job and put into output directory
  
  doJob(sqlContext.read.json(in_dir))\
	.saveAsTextFile(out_dir)

if __name__ == '__main__':
  main()

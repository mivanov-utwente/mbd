#!/usr/bin/env python
import sys
import os
import json
import pyspark

import re


pol_parties = {
  'VVD': {'short': 'VVD', 'full': 'Volkspartij voor Vrijheid en Democratie', 'regexes': []},
  'PvdA': {'short': 'PvdA', 'full': 'Partij van de Arbeid', 'regexes': []},
  'SP': {'short': 'SP', 'full': 'Socialistische Partij', 'regexes': []},
  'CDA': {'short': 'CDA', 'full': 'Christen Democratisch Appel', 'regexes': []},
  'PVV': {'short': 'PVV', 'full': 'Partij voor de vrijheid', 'regexes': []},
  'D66': {'short': 'D66', 'full': 'Democraten 66', 'regexes': []},
  'CU': {'short': 'CU', 'full': 'ChristenUnie', 'regexes': []},
  'GL': {'short': 'GL', 'full': 'GroenLinks', 'regexes': []},
  'SGP': {'short': 'SGP', 'full': 'Staatkundig Gereformeerde Partij', 'regexes': []},
  'PvdD': {'short': 'PvdD', 'full': 'Partij voor de Dieren', 'regexes': []},
  '50PLUS': {'full': '50Plus', 'regexes': []},
}

# build rules
for k, v in pol_parties.items():
  rules = v.setdefault('rules', [])
  if v.get('short'):
    rules.append(re.compile(r'\b%s\b' % v['short']))
  if v.get('full'):
    rules.append(re.compile(r'\b%s\b' % v['full'].replace(' ', r'\b'), re.I))

def partyFlatMap(x):
  result = []
  for k, party in pol_parties.items():
    if any(rule.search(x['text']) for rule in party['rules']):
      result.append(((k, x['year']), x['description_score'] +  x['text_score']))
  return result

def doJob(df):
  rdd = df.rdd.filter(lambda x: x['category'] == 'Politiek')
  rdd = rdd.flatMap(partyFlatMap)
  counts = rdd.countByKey()
  rdd = rdd.reduceByKey(lambda a, b: list(map(sum, zip(a, b)))) \
           .map(lambda x: (x[0],  tuple(_/counts[x[0]] for _ in x[1])))
  return rdd

def formatRdd(rdd):
  return rdd.map(lambda x: '"{}",{},{},{},{},{}'.format(unicode(x[0][0]).encode("utf8"), x[0][1], *x[1]))

def main():
  # parse arguments 
  in_dir, out_dir = sys.argv[1:3]
  
  conf = pyspark.SparkConf().setAppName("%s %s %s" % (os.path.basename(__file__), in_dir, out_dir))
  sc = pyspark.SparkContext(conf=conf)
  sqlContext = pyspark.sql.SQLContext(sc)
  df = sqlContext.read.json(in_dir)

  # invoke job and put into output directory
  rdd = doJob(df)
  formatRdd(rdd).saveAsTextFile(out_dir)

if __name__ == '__main__':
  main()

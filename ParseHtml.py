#!/usr/bin/env python
import sys
import os
import json
import pyspark
import re
import HTMLParser

html_parser = HTMLParser.HTMLParser()

description_re = re.compile(r'<p itemprop="description">(?P<descr>.+?)</p>')
description_meta_re = re.compile(r'<meta name="description" content="(?P<descr>.+?)\" /\s?>')
author_re = re.compile(r'<span itemprop="author" itemscope="" itemtype="http://schema\.org/Person">(?P<author>.+?)</span>')

def parse_description(html):
  m = description_re.search(html)
  if m:
    return m.group('descr')
  
  m = description_meta_re.search(html)
  if m:
    return m.group('descr')

  return ''

def parse_author(html):
  m = author_re.search(html)
  return m and m.group('author') or ''

def parse_html(row):
  entry = row.asDict()
  html = entry['full_html']
  entry['description'] = html_parser.unescape(parse_description(html))
  entry['author'] = html_parser.unescape(parse_author(html)).upper()
  del entry['full_html']
  return json.dumps(entry)

def doJob(rdd):
  return rdd.map(parse_html)

def main():
  # parse arguments 
  in_dir, out_dir = sys.argv[1:]
  
  conf = pyspark.SparkConf().setAppName("%s %s %s" % (os.path.basename(__file__), in_dir, out_dir))
  sc = pyspark.SparkContext(conf=conf)
  sqlContext = pyspark.sql.SQLContext(sc)
  
  # invoke job and put into output directory
  doJob(sqlContext.read.json(in_dir)).saveAsTextFile(out_dir)

if __name__ == '__main__':
  main()

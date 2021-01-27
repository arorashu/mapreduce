"""
read all files in directory and get word count
using single map and reduce functions
"""

import os
import ray
import re
import time
import wikipedia

from utils import Config

def map(filename):
  # read the file
  kv_list = []
  for word in words:
    kv_list.append((word, 1))

  return kv_list


def reduce(kv: list):
  tot = 0
  for x in kv:
    tot += x

  return tot



if __name__ == "__main__":
  
  words = []

  summary = get_wiki_articles()
  for w in summary.split():
    words.append(w)


  kv_list = map(words)
  # print(f'kv list: {kv_list}')

  coll = {}
  for pair in kv_list:
    if pair[0] not in coll:
      coll[pair[0]] = [pair[1]]
    else:
      coll[pair[0]].append(pair[1])


  for key, val in coll.items():
    count = reduce(val)
    print(f'word: {key}, count: {count}')



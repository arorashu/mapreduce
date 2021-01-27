"""
read all files in directory and get word count
"""

from collections import Counter
import os
import re
import time

from utils import Config

if __name__ == "__main__":
  start = time.time()
  word_count = {}
  files = os.listdir(Config.txt_dir)
  #print(files)
  for f in files:
    txt_path = os.path.join(Config.txt_dir, f)
    #print(txt_path)
    with open(txt_path, 'r') as f:
      txt = f.read()
      for w in re.findall(r'\w+', txt) :
        w = w.lower()
        if w not in word_count:
          word_count[w] = 1
        else:
          word_count[w] += 1

  end = time.time()
  print(f'time taken: {round(end-start, 3)}s')
  counts = Counter(word_count)
  too_common = counts.most_common(40)
  for key in too_common:
    print(key[0])
  #print(counts.most_common(20))
    

  

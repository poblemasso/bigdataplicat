import re
import sys
import io
import re
import nltk
import string
from unicodedata import normalize
nltk.download('stopwords', quiet=True)
from nltk.corpus import stopwords
punctuations = '''!()-[]{};:'"\,<>./?@#$%^&*_~'''

stop_words = set(stopwords.words('spanish'))
input_stream = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')

for line in input_stream:
  line = line.strip()
  line = re.sub(r'[^\w\s]', '',line)
  line = line.lower()
  for x in line:
    if x in punctuations:
      line=line.replace(x, " ") 

  words=line.split()
  for word in words: 
    if word not in stop_words:
        word = normalize( 'NFC', word) 
        letter = word[0:1]
        print(letter)

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
#Definim les stopwords pels idiomes que tenim en els llibres(en el meu cas, català, castellà, anglès i francès)
stop_words = stopwords.words('spanish') + stopwords.words('french') + stopwords.words('english')
stop_words = set(stop_words)
input_stream = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
for line in input_stream:
  line = line.strip()
  line = re.sub(r'[^\w\s]', '',line)
  #Optimització: pasam tot a minuscules, perque no ha de distingir de majúscules i minúscules.
  line = line.lower()
  for x in line:
    if x in punctuations:
      line=line.replace(x, " ") 

  words=line.split()
  for word in words: 
    if word not in stop_words:
      #Optimització: Perque la paraula no ha de distingir de si te la primera lletra accentuada o no, però deixam la ñ com a excepció.
        word = re.sub(
                r"([^n\u0300-\u036f]|n(?!\u0303(?![\u0300-\u036f])))[\u0300-\u036f]+", r"\1", 
                normalize( "NFD", word), 0, re.I
            )
        word = normalize( 'NFC', word)
      #Agafam la primera lletra de la paraula, per veure amb quina lletra comença.  
        letra = word[0:1]
      #Optimització: si la lletra es troba dins l'alfabet llatí i les lletres ç o ñ.
        if letra in list('abcdefghijklmnñopqrstuvwxyzç'):
          print('%s\t%s\t%s\t%s' % (letra, 1, len(word), len(word)))

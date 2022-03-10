REGISTER /usr/lib/pig/piggybank.jar;
-- Carregam les critiques cinematografiques a la variable extract_details
extract_details = LOAD '/user/cloudera/pig_analisis_opinions/critiquescinematografiques.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE')  AS (text:chararray, label:int, id:int);
-- Tokenitzam les paraules per procesarles
tokens = foreach extract_details generate id,label,text, FLATTEN(TOKENIZE(text)) As word;
dictionary = load '/user/cloudera/pig_analisis_opinions/AFINN.txt' using PigStorage('\t') AS(word:chararray,rating:int);
word_rating = join tokens by word left outer, dictionary by word using 'replicated';
describe word_rating;
rating = foreach word_rating generate tokens::id as id,tokens::text as text, dictionary::rating as rate;
word_group = group rating by (text, label,);
avg_rate = foreach word_group generate group, AVG(rating.rate) as AVG;
STORE avg_rate INTO '/user/cloudera/pig_analisis_opinions/resultat_analisis_opinions' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE');
avg_rate_group = GROUP avg_rate ALL;
avg_rate_group_max_min = FOREACH avg_rate_group GENERATE MAX(avg_rate.AVG),MIN(avg_rate.AVG);
STORE avg_rate_group_max_min INTO '/user/cloudera/pig_analisis_opinions/resultat_analisis_opinions_max_min' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE');

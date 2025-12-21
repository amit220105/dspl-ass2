Dvir dayan 324209055
Amit Ben Hemo 327522348

statistics

Hebrew 
(UnigramDecade)
 with combiner | without combiner | diffrence

Combine input records: (with) 96,852,220 | (without) 0 | (without) combiner disabled (→ 0)
Combine output records: (with) 16,522,549 | (without) 0 | (without) combiner disabled (→ 0)
Reduce input records (KV pairs sent mapper→reducer): (with) 8,471,309 | (without) 88,800,980 | +80,329,671 (×10.48)
Reduce shuffle bytes (bytes sent mapper→reducer): (with) 76,185,197 | (without) 361,840,887 | +285,655,690 (×4.75)

(BigramDecade)
Combine input records: (with) 118,653,455 | (without) 0 | (without) combiner disabled (→ 0)
Combine output records: (with) 26,821,222 | (without) 0 | (without) combiner disabled (→ 0)
Reduce input records (KV pairs sent mapper→reducer): (with) 26,821,222 | (without) 118,653,455 | +91,832,233 (×4.42)
Reduce shuffle bytes (bytes sent mapper→reducer): (with) 335,218,700 | (without) 749,169,855 | +413,951,155 (×2.23)

English
(UnigramDecade)

with combiner | without combiner | difference

Combine input records: (with) 419,703,973 | (without) 0 | (without) combiner disabled (→ 0)
Combine output records: (with) 85,426,372 | (without) 0 | (without) combiner disabled (→ 0)
Reduce input records (KV pairs sent mapper→reducer): (with) 43,043,317 | (without) 377,320,918 | +334,277,601 (×8.77)
Reduce shuffle bytes (bytes sent mapper→reducer): (with) 388,227,941 | (without) 1,519,516,464 | +1,131,288,523 (×3.91)

(BigramDecade)

with combiner | without combiner | difference

Combine input records: (with) 778,240,162 | (without) 0 | (without) combiner disabled (→ 0)
Combine output records: (with) 196,990,982 | (without) 0 | (without) combiner disabled (→ 0)
Reduce input records (KV pairs sent mapper→reducer): (with) 196,990,982 | (without) 875,046,737 | +678,055,755 (×4.44)
Reduce shuffle bytes (bytes sent mapper→reducer): (with) 2,440,320,682 | (without) 5,547,688,406 | +3,107,367,724 (×2.27)





Analysis
Hebrew
Bad Examples

1.אמר רבי
2.רבי שמעון
3.הרב קוק
4.הקדוש ברוך
5.אלא גם
example 1 is bad because its means rabi said, which is not realy a Collocations its just a formula opener where they start a story by rabi said etc.
example 2 and 3 are bad because they are title + name so the name apear relatively rarley, and when it apears it is overwhelmingly as part of the fixed formula of
rabi + name.
example 4 is bad because its not a collocation. its a fragment of a longer fixed phrase Bigram extraction “cuts” a longer expression into pieces; the piece still
looks extremely collocational because it’s repeated in a fixed way.
example 5 is bad because t’s not a “content collocation”, but it is a very strong functional pairing. t stays because your stopword list likely doesn’t include one/both of these function words.

Good Examples
1.תל אביב
2.ראש הממשלה
3.ראש השנה
4.בדרך כלל
5.לידי ביטוי

English
Bad Examples
1.copyright ©
2. ' t
3. / www 
4. war ii
5. chapter vi

Good Examples
1. mental health
2. human rights
3. world war
4. prime minister
5. climate change


How To Run
1. compile: mvn clean compile

2. upload stop words: 
aws s3 cp "C:\BGU\DSPLR\dspl-ass2\dspl-ass2\eng-stopwords.txt" s3://<Bucket>/conf/eng-stopwords.txt --region us-east-1
aws s3 cp "C:\BGU\DSPLR\dspl-ass2\dspl-ass2\heb-stopwords.txt" s3://<Bucket>/conf/heb-stopwords.txt --region us-east-1

3.upload jar: aws s3 cp target\hadoop-examples-1.0-SNAPSHOT.jar s3://<Bucket>/jars/hadoop-examples-1.0-SNAPSHOT.jar --region us-east-1
4. run:
mvn exec:java "-Dexec.mainClass=hadoop.examples.JobFlowNgram" "-Dexec.args=us-east-1 <Bucket> <link-to-1gram-data> <link-to-2gram-data>"

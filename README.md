# alanhau-sparkscripts

~/spark/bin/spark-submit ~/alanhau-sparkscripts/SimpleApp.py

put a file on HDFS
~/ephemeral-hdfs/bin/hadoop fs -put ~/spark/README.md README.md
~/ephemeral-hdfs/bin/hadoop fs -put ~/mydata/googlengrams/googlebooks-eng-all-1gram-20120701-other /googlengrams/googlebooks-eng-all-1gram-20120701-other

~/ephemeral-hdfs/bin/hadoop fs -put ~/mydata/googlengrams/googlebooks-eng-all-1gram-20120701-b /googlengrams/googlebooks-eng-all-1gram-20120701-b

if you see errors on putting files to hadoop:
./root/ephemeral-hdfs/bin/stop-all.sh
./root/ephemeral-hdfs/bin/start-all.sh

check root dir on HDFS
~/ephemeral-hdfs/bin/hadoop fs -ls /

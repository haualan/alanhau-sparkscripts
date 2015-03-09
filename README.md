# alanhau-sparkscripts

~/spark/bin/spark-submit ~/alanhau-sparkscripts/SimpleApp.py

put a file on HDFS
~/ephemeral-hdfs/bin/hadoop fs -put ~/spark/README.md README.md
~/ephemeral-hdfs/bin/hadoop fs -put ~/mydata/googlengrams/googlebooks-eng-all-1gram-20120701-other /googlengrams/googlebooks-eng-all-1gram-20120701-other

~/ephemeral-hdfs/bin/hadoop fs -put ~/mydata/googlengrams/googlebooks-eng-all-1gram-20120701-b /googlengrams/googlebooks-eng-all-1gram-20120701-b

tell spark to shut up:
# Step 1. Change config file on the master node
nano /root/ephemeral-hdfs/conf/log4j.properties

# Before
hadoop.root.logger=INFO,console
# After
hadoop.root.logger=WARN,console

# Step 2. Replicate this change to slaves
~/spark-ec2/copy-dir /root/ephemeral-hdfs/conf/

if you see errors on putting files to hadoop:
./root/ephemeral-hdfs/bin/stop-all.sh
./root/ephemeral-hdfs/bin/start-all.sh

check root dir on HDFS
~/ephemeral-hdfs/bin/hadoop fs -ls /

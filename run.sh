spark-submit --master spark://100.88.61.22:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,com.datastax.spark:spark-cassandra-connector_2.12:3.0.0 --executor-cores 1 --conf spark.executor.memory=1g --conf spark.cores.max=1 --conf spark.sql.shuffle.partitions=2 --conf spark.default.parallelism=2 comment_processing.py
spark-submit --master spark://100.88.61.22:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,com.datastax.spark:spark-cassandra-connector_2.12:3.0.0 --executor-cores 1 --conf spark.executor.memory=1g --conf spark.cores.max=1 --conf spark.sql.shuffle.partitions=2 --conf spark.default.parallelism=2 submission_processing.py

spark-submit --master spark://100.88.61.22:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,com.datastax.spark:spark-cassandra-connector_2.12:3.0.0 spark_to_cassandra.py

spark-class org.apache.spark.deploy.master.Master

spark-class org.apache.spark.deploy.worker.Worker spark://100.88.61.22:7077 --cores 2 --memory 2g
include config

help: ## Show this help.
	@sed -ne '/@sed/!s/## //p' $(MAKEFILE_LIST)

hdfs-cleanUp-stream: ## Clean up and create HDFS stream folder
	hdfs dfs -rm -f -R ${hdfs_stream}
	hdfs dfs -mkdir -p ${hdfs_stream}

ipynb2py: ## Convert .ipynb files to .py files
	jupyter nbconvert --to script aml_trainer.ipynb
	jupyter nbconvert --to script aml_kafka_consumer.ipynb
	jupyter nbconvert --to script aml_kafka_producer.ipynb
	jupyter nbconvert --to script aml_stats.ipynb 
	jupyter nbconvert --to script aml_benchmark.ipynb

pipeline-init: ## Initialize Pipeline
	spark-submit aml_trainer.py
	hdfs dfs -ls -R /trab/g12/model/rf_aml_pipeline

kafka-produce: ## Generate Fake Data to Kafka Topic g12in
	spark-submit aml_kafka_producer.py

kafka-consumer: ## Display content of Kafka Topic g12out
	spark-submit aml_kafka_consumer.py

stats: ## Display Metrics
	spark-submit aml_stats.py

benchmark: ## Display Benchmark
	spark-submit aml_benchmark.py
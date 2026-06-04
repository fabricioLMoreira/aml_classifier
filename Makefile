include config

help: ## Show this help.
	@sed -ne '/@sed/!s/## //p' $(MAKEFILE_LIST)

hdfs-cleanup: ## Clean up and create HDFS folders
	hdfs dfs -rm -f -R ${hdfs_model}
	hdfs dfs -rm -f -R ${hdfs_model_versions}
	hdfs dfs -rm -f -R ${hdfs_active_model}

	hdfs dfs -rm -f -R ${hdfs_stream}
	hdfs dfs -mkdir -p ${hdfs_stream}

	hdfs dfs -rm -f -R ${hdfs_incremental}
	hdfs dfs -rm -f -R ${hdfs_manifests}

	hdfs dfs -ls ${hdfs_path}

ipynb2py: ## Convert .ipynb files to .py files
	jupyter nbconvert --to script aml_trainer.ipynb
	jupyter nbconvert --to script aml_kafka_consumer.ipynb
	jupyter nbconvert --to script aml_kafka_producer.ipynb
	jupyter nbconvert --to script aml_stats.ipynb 
	jupyter nbconvert --to script aml_benchmark.ipynb

auto-trainer: ## Run automatic periodic retraining
	python aml_auto_trainer.py

kafka-produce: ## Generate Fake Data to Kafka Topic g12in
	spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 aml_kafka_producer.py

kafka-consumer: ## Display content of Kafka Topic g12out
	spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 aml_kafka_consumer.py

stats: ## Display Metrics
	spark-submit aml_stats.py

benchmark: ## Display Benchmark
	spark-submit aml_benchmark.py

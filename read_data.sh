#! /bin/sh
spark-submit --packages com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.7 \
data-ingestion.py s3a://openaq-fetches/realtime/

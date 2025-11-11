FROM apache/airflow:3.0.3
RUN pip install --no-cache-dir apache-airflow-providers-apache-kafka==1.10.4 
RUN pip install --no-cache-dir apache-airflow-providers-postgres
RUN pip install --no-cache-dir confluent_kafka
RUN pip install --no-cache-dir apache-airflow-providers-google
RUN pip install  astronomer-cosmos
RUN pip install pandas
RUN pip install openpyxl
RUN pip install openai
RUN pip install --timeout=200 --retries=5 dbt-core
RUN pip install --timeout=200 --retries=5 dbt-postgres


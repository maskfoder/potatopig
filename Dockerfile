FROM apache/airflow
RUN pip install --no-cache-dir feedparser
RUN pip install --no-cache-dir pandas

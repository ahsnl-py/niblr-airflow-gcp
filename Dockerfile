FROM apache/airflow:2.10.4

USER airflow  
COPY requirements.txt /requirements.txt  
RUN pip install --no-cache-dir -r /requirements.txt

USER root
# Copy include directory with configuration files
COPY include/ /opt/airflow/include/

# Create datasets directory and set permissions
RUN mkdir -p /opt/airflow/include/datasets && \
    chown -R airflow:root /opt/airflow/include && \
    chmod -R 755 /opt/airflow/include

USER airflow

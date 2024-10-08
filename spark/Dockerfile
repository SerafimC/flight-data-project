# docker build -t my-pyspark-notebook:3.5.0 .
# Use the official Jupyter PySpark notebook image as the base
FROM jupyter/pyspark-notebook:latest

# Set environment variables
ENV APACHE_SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark

# Switch to root user to install dependencies
USER root

RUN pip install --no-cache-dir \
    FlightRadarAPI \

RUN conda activate base && pip install hdfs

# Ensure necessary permissions
RUN chown -R $NB_UID:$NB_GID /opt/hadoop

# Remove the pre-installed Spark version
RUN rm -rf /opt/spark

# Download and install Spark 3.5.0
RUN curl -sL https://archive.apache.org/dist/spark/spark-${APACHE_SPARK_VERSION}/spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz | tar -xz -C /opt/ && \
    mv /opt/spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} $SPARK_HOME && \
    chown -R $NB_UID:$NB_GID $SPARK_HOME

# Configure environment variables for Spark
ENV PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Expose Jupyter Notebook port
EXPOSE 8888

# Set the entrypoint for the Jupyter notebook
CMD ["start-notebook.sh"]
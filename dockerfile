FROM apache/airflow:3.0.0

COPY my-sdk /opt/airflow/my-sdk

RUN pip install -e /opt/airflow/my-sdk

COPY requirements.txt .

RUN pip install --no-cache-dir --prefer-binary --timeout=10000 -r requirements.txt


USER root  

# Install git properly
RUN apt-get update && apt-get install -y git && \
    git config --system safe.directory '*' && \
    git config --system user.email "airflow@localhost" && \
    git config --system user.name "Airflow"

#  INSTALL SPARK 3.3.0 (MATCHES YOUR CLUSTER)
RUN apt-get update && apt-get install -y wget curl tar && rm -rf /var/lib/apt/lists/*

# Install Spark 3.3.0 (matches cluster)
RUN wget https://archive.apache.org/dist/spark/spark-3.3.0/spark-3.3.0-bin-hadoop3.tgz && \
    tar -xzf spark-3.3.0-bin-hadoop3.tgz -C /opt && \
    ln -s /opt/spark-3.3.0-bin-hadoop3 /opt/spark && \
    rm spark-3.3.0-bin-hadoop3.tgz

# Install JDK 8 (Temurin / AdoptOpenJDK)
RUN wget https://github.com/adoptium/temurin8-binaries/releases/download/jdk8u352-b08/OpenJDK8U-jdk_x64_linux_hotspot_8u352b08.tar.gz && \
    tar -xzf OpenJDK8U-jdk_x64_linux_hotspot_8u352b08.tar.gz -C /opt && \
    mv /opt/jdk8u352-b08 /opt/java-8 && \
    rm OpenJDK8U-jdk_x64_linux_hotspot_8u352b08.tar.gz

# Set JAVA_HOME to Java 8
ENV JAVA_HOME=/opt/java-8
ENV SPARK_HOME=/opt/spark
ENV PATH="${JAVA_HOME}/bin:${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"


# new hodoop client
RUN wget https://archive.apache.org/dist/hadoop/core/hadoop-2.7.4/hadoop-2.7.4.tar.gz -P /opt && \
    tar -xzf /opt/hadoop-2.7.4.tar.gz -C /opt && \
    ln -s /opt/hadoop-2.7.4 /opt/hadoop && \
    rm /opt/hadoop-2.7.4.tar.gz

# Set Hadoop environment variables
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=/etc/hadoop/conf
ENV PATH="${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin:${PATH}"


USER airflow
# setup dbt && we should add all plugins we need like snowflake, postgres, spark, or in the futcher big query
RUN pip install --no-cache-dir \
    dbt-core==1.4.0 \
    dbt-snowflake \
    dbt-postgres==1.4.0 \
    dbt-spark==1.4.0

# Verify dbt installation
RUN dbt --version   
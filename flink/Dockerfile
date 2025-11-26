FROM flink:2.0.0

RUN apt-get update -y && \
    apt-get install -y \
      default-jdk \
      python3 \
      python3-dev \
      python3-pip \
      && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

# Create a 'python' symlink
RUN ln -s /usr/bin/python3 /usr/bin/python

# Ensure PyFlink JAR is present
RUN wget -P /opt/flink/lib https://repo1.maven.org/maven2/org/apache/flink/flink-python/2.0.0/flink-python-2.0.0.jar

# ===== Download necessary Kafka & Python JARs =====
# SQL Kafka connector
RUN wget -P /opt/flink/lib \
    https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/4.0.0-2.0/flink-sql-connector-kafka-4.0.0-2.0.jar

# # PyFlink JAR
# RUN wget -P /opt/flink/lib \
#     https://repo1.maven.org/maven2/org/apache/flink/flink-python/2.0.0/flink-python-2.0.0.jar


# Install PyFlink
RUN pip3 install apache-flink==2.0.0
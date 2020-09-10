FROM ubuntu:18.04

LABEL maintainer="Julian Agius"
LABEL description="Environment for Big Data"

WORKDIR /data

COPY . /data

# Install Java, Scala, Git, wget, Python3 and pip3
RUN apt update && apt install default-jdk scala git wget python3.6 python3-pip -y

# Install Spark
RUN wget https://downloads.apache.org/spark/spark-2.4.5/spark-2.4.5-bin-hadoop2.7.tgz && \
	tar xvf spark-* && \
	mv spark-2.4.5-bin-hadoop2.7 /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV PYSPARK_PYTHON=/usr/bin/python3

# Install Graphframes jar
RUN wget http://dl.bintray.com/spark-packages/maven/graphframes/graphframes/0.8.0-spark2.4-s_2.11/graphframes-0.8.0-spark2.4-s_2.11.jar && \
	mv graphframes-0.8.0-spark2.4-s_2.11.jar /opt/spark/jars/

# Install Python libraries
RUN pip3 install numpy && \
	pip3 install pandas && \
	pip3 install webweb && \
	pip3 install torch==1.4.0 --no-cache-dir && \
	pip3 install stanford_openie --no-cache-dir

RUN	pip3 install jupyter && \
	pip3 install pymongo && \
	pip3 install pyspark && \
	pip3 install graphframes  && \
	pip3 install nltk  && \
	pip3 install spacy && \
	pip3 install kafka && \
	pip3 install tweepy

# Install spacy en_core_web_sm model
RUN python3 -m spacy download en_core_web_sm

CMD ["jupyter","notebook","--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]
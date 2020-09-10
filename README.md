# Tweet-Analysis

In this project, a pipeline used to analyze tweets is presented and described. The two topics analyzed relate to the COVID-19 pandemic and Black Lives Matter protests. Text mining and graph mining approaches were employed to identify and visualize what Twitter users are writing about present trending topics.

## Requirements:
The whole solution can be run without having to manually set up the required environment using Docker and Docker Compose.
Otherwise, the required software (Java, Git, Python 3, etc...), Python packages (tweepy, pyspark, stanford_openie, graphframes, etc...) and paths will have to be set up manually. In this latter case follow the instructions found in the `Dockerfile`.

* docker
* docker-compose

## Usage:
1. To set up the environment including zookeeper, kafka, mongoDB, spark and graphx run `docker-compose up`
1. To get a stream of tweets from Twitter run the `Producer.ipynb` notebook
    * list of hashtag files can be found in the hashtags folder
1. To perform Open Information Extraction and Named Entity Recognition on stream of tweets run `Consumer.ipynb` notebook
    * data is stored in mongoDB collections
1. To count term frequencies in tweets:
    1. Run `docker ps` to find container ID for image _mybdenv_
    1. Run `docker exec -it _<<container ID>>_ /bin/bash`
    1. Run `spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.5 spark_kafka.py 172.27.1.16:9092 blm analysisDB`
        * where `spark_kafka.py` is the python script that will count terms and store in mongoDB
        * `172.27.1.16:9092` is the kafka server
        * `blm` is the topic name
        * `analysisDB is the mongoDB database name`
1. After running the above notebooks and script you can use the notebooks in the analysis_notebooks folder
    1. `Frequent Named Entities.ipynb` - Identify frequently mentioned organisations, people, etc...
	  1. `Frequent Terms.ipynb` - Visualize frequently mentioned words using a word cloud
	  1. `GraphX_Label_Propagation.ipynb` - Perform community detection on knowledge graph
	  1. `GraphXPageRank.ipynb` - Run PageRank on knowledge graph
	  1. `GraphX_BLM_Covid.ipynb` - Find covid related entities within blm knowledge graph and vice versa
	  1. `Graph_Visualization.ipynb` - Visualize detected communities using pickled edge lists stored in the data folder within the analysis_notebooks folder

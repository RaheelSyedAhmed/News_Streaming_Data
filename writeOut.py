from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *
from pyspark.sql.functions import *
import sys
import string

bootstrapServers = sys.argv[1]
subscribeType = sys.argv[2]
topics = sys.argv[3]

import nltk
nltk.download('stopwords')
nltk.download('wordnet')
nltk.download('punkt')
nltk.download('maxent_ne_chunker')
nltk.download('words')
nltk.download('averaged_perceptron_tagger')

# User defined SQL function to extract named entities from a text
@udf
def NER(text):
    
    from nltk.corpus import stopwords
    from nltk.tokenize import word_tokenize
    from nltk.stem import PorterStemmer, WordNetLemmatizer
    from nltk import pos_tag, ne_chunk

    print("NER")
    if text is not None:
        # Preprocess string
        # Step 1: Remove Punctuations
        text = text.translate(str.maketrans("", "", string.punctuation))

        # Step 2: Lowercasing
        text = text.lower()

        # Step 3: Tokenization
        tokens = word_tokenize(text)

        # Step 4: Stopword Removal
        stop_words = set(stopwords.words('english'))
        filtered_tokens = [word for word in tokens if word.lower() not in stop_words]

        # Step 5: Stemming
        stemmer = PorterStemmer()
        stemmed_tokens = [stemmer.stem(word) for word in filtered_tokens]

        # Step 6: Lemmatization
        lemmatizer = WordNetLemmatizer()
        lemmatized_tokens = [lemmatizer.lemmatize(word) for word in filtered_tokens]

        preprocessed = lemmatized_tokens
        # Combine the list of words into a single string
        text = ' '.join(preprocessed)

        # Tokenize the text into words
        words = word_tokenize(text)

        # Perform part-of-speech tagging
        pos_tags = pos_tag(words)

        # Perform named entity recognition
        named_entities = ne_chunk(pos_tags)

        all_entities = [entity[0] for entity in named_entities.leaves()]

        return ' '.join(all_entities)

# Start up a spark session
spark = (
        SparkSession.builder.appName("Kafka Pyspark Streaming NER")
        .master("local[*]")
        .getOrCreate()
    )
spark.sparkContext.setLogLevel("ERROR")

# Specify a schema for the article data that we will parse
article_schema = StructType()\
    .add("author", "string")\
    .add("title", "string")\
    .add("description", "string")\
    .add("publishedAt", "string")\
    .add("content", "string")

# Read a multiline stream with 1 file read per trigger from a folder of article CSV files.
article_stream = spark \
    .readStream \
    .schema(article_schema) \
    .option("header", "true") \
    .option("maxFilesPerTrigger", 1) \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .option("ignoreLeadingWhiteSpace", "true") \
    .option("encoding", "UTF-8") \
    .option("multiline","true")\
    .csv("articles") \
    .select(col('title'), concat('author', lit(" "),            # Select all columns with usable info and combine into one column
                   'title', lit(" "),                           # NER is ran on this column to extract named entities
                   'description', lit(" "),                     # The final selection is to explode the named entities into multiple rows of countable words
                   'content').alias('all_info')) \
    .select(col('title'), NER(col("all_info")).alias("named_entities")) \
    .select(
        col('title'), 
        explode(
            split(col('named_entities'), ' ')
        ).alias('word')
    )\
    .groupBy('word').count()                                    # Groupby and count words to aggregate the count over time

# Write the streaming dataframe as a json compatible value entry
query = article_stream.select(to_json(struct(col('word'), col('count'))).alias("value"))\
    .selectExpr("CAST(value AS STRING)") \
    .writeStream \
    .format("kafka") \
    .outputMode("complete") \
    .option("kafka.bootstrap.servers", bootstrapServers) \
    .option("topic", topics) \
    .trigger(processingTime='15 seconds')\
    .option("checkpointLocation", "tmp/checkpoints")\
    .start()
## WARNING: IF YOU DO NOT DELETE CHECKPOINTS FOLDER EVERY RUN, IT WILL NOT WORK
import time
time.sleep(10)

query.awaitTermination()        # Await termination to allow for closing

# For console debugging
"""
.format("kafka") \
.option("topic", topics) \
.option("kafka.bootstrap.servers", bootstrapServers) \
.trigger(processingTime='1 seconds') \
.outputMode("complete") \
.option("checkpointLocation", "chckpoints") \
.option("includeHeaders", "true") \
.start() 
"""

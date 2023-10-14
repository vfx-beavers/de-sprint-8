from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType
from time import sleep

TOPIC_NAME_IN = 'cgbeavers_in'
TOPIC_NAME_OUT = 'cgbeavers_out'

# input message json schema 
incomming_message_schema = StructType([
    StructField("restaurant_id", StringType()),
    StructField("adv_campaign_id", StringType()),
    StructField("adv_campaign_content", StringType()),
    StructField("adv_campaign_owner", StringType()),
    StructField("adv_campaign_owner_contact", StringType()),
    StructField("adv_campaign_datetime_start", LongType()),
    StructField("adv_campaign_datetime_end", LongType()),
    StructField("datetime_created", LongType()),
])

columns = [
    'restaurant_id',
    'adv_campaign_id',
    'adv_campaign_content',
    'adv_campaign_owner',
    'adv_campaign_owner_contact',
    'adv_campaign_datetime_start',
    'adv_campaign_datetime_end',
    'datetime_created',
    'trigger_datetime_created',
    'client_id']

# Spark with Kafka и PostgreSQL settings
spark_jars_packages = ",".join(
    [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
        "org.postgresql:postgresql:42.4.0",
    ])

kafka_connection_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";',
    'kafka.bootstrap.servers': 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091',
}

pg_local_settings = {
    'url': 'jdbc:postgresql://localhost:5432/de',
    'driver': 'org.postgresql.Driver',
    'user': 'jovyan',
    'password': 'jovyan',
    'dbtable': 'public.subscribers_feedback'
}

pg_cloud_subscribers_connection = {
    'url': 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de',
    'driver': 'org.postgresql.Driver',
    'user': 'student',
    'password': 'de-student',
    'dbtable': 'public.subscribers_restaurants'
}

# spark session + spark_jars_packages
def spark_init(name: str) -> SparkSession:
    return (SparkSession.builder
            .appName(name)
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.jars.packages", spark_jars_packages)
            .getOrCreate())

# subscribers read
def restaurant_subs(spark: SparkSession, options: dict) -> DataFrame:
    return (spark.read
            .format("jdbc")
            .options(**options)
            .load()
            .dropDuplicates(['client_id', 'restaurant_id'])
            .select('client_id', 'restaurant_id'))

# kafka in topic read
def restaurant_event_stream(spark: SparkSession, options: dict) -> DataFrame:
    return (spark.readStream
            .format('kafka')
            .options(**options)
            .option('subscribe', TOPIC_NAME_IN)
            .load())
            
# data to PostgreSQL and Kafka targets
def foreach_batch_function(df: DataFrame, epoch_id):
    # сохраняем df в памяти, чтобы не создавать df заново перед отправкой в Kafka
    df.persist()
    # записываем df в PostgreSQL с полем feedback
    df \
        .select(columns) \
        .withColumn('feedback', F.lit('')) \
        .write.format("jdbc") \
        .mode('append') \
        .options(**pg_local_settings) \
        .save()

    # отправляем сообщения в результирующий топик Kafka без поля feedback
    df \
        .select(F.to_json(F.struct(columns)).alias('value')) \
        .write \
        .mode("append") \
        .format("kafka") \
        .options(**kafka_connection_options) \
        .option("topic", TOPIC_NAME_OUT) \
        .save()

    # очищаем память от df
    df.unpersist()


def filtered_read_stream(df: DataFrame, schema: StructType) -> DataFrame:
    return (df
            .withColumn('value', F.col('value').cast(StringType()))
            .withColumn('event', F.from_json(F.col('value'), schema))
            .selectExpr('event.*')
            .withColumn('timestamp', F.from_unixtime(F.col('datetime_created'), "yyyy-MM-dd' 'HH:mm:ss.SSS").cast(TimestampType()))
            .dropDuplicates(['restaurant_id', 'timestamp'])
            .withWatermark('timestamp', '1 minutes')
            .withColumn('trigger_datetime_created', F.unix_timestamp(F.current_timestamp()))
            .filter('trigger_datetime_created >= adv_campaign_datetime_start and adv_campaign_datetime_end >= trigger_datetime_created'))

# kafka data join with subscribers by restaurant_id (uuid)
def join(event_df: DataFrame, subscribers_df: DataFrame) -> DataFrame:
    return (event_df
            .join(subscribers_df, 'restaurant_id', how='inner'))


if __name__ == "__main__":
    spark = spark_init('join stream')

    restaurant_subs_df  = restaurant_subs(spark, pg_cloud_subscribers_connection)
    restaurant_event_stream_df = restaurant_event_stream(spark, kafka_connection_options)
    filtered_read_stream_df = filtered_read_stream(restaurant_event_stream_df, incomming_message_schema)
    joined_df = join(filtered_read_stream_df, restaurant_subs_df)

    query = joined_df \
        .writeStream \
        .option("checkpointLocation", "tmp_folder") \
        .trigger(processingTime="20 seconds") \
        .foreachBatch(foreach_batch_function) \
        .start()

    while query.isActive:
        print(f"query information: runId={query.runId}, "
              f"status is {query.status}, "
              f"recent progress={query.recentProgress}")
        sleep(60)

    query.awaitTermination()

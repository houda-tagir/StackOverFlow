# === spark_stream_to_hbase.py ===
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    ArrayType,
    BooleanType,
    LongType,
)
import happybase

HBASE_HOST = "localhost"
QUESTIONS_TABLE = "stackoverflow_questions"
ANSWERS_TABLE = "stackoverflow_answers"

answer_schema = StructType(
    [
        StructField("answer_id", LongType()),
        StructField("body", StringType()),
        StructField("score", IntegerType()),
        StructField("is_accepted", BooleanType()),
        StructField("owner_reputation", IntegerType()),
    ]
)

question_schema = StructType(
    [
        StructField("question_id", LongType()),
        StructField("title", StringType()),
        StructField("body", StringType()),
        StructField("creation_date", LongType()),
        StructField("score", IntegerType()),
        StructField("tags", ArrayType(StringType())),
        StructField("owner_reputation", IntegerType()),
        StructField("is_answered", BooleanType()),
        StructField("answers", ArrayType(answer_schema)),
    ]
)

trend_schema = StructType(
    [
        StructField("tag", StringType()),
        StructField("count", IntegerType()),
    ]
)


def save_question_to_hbase(row):
    try:
        connection = happybase.Connection(HBASE_HOST, port=9090)
        table = connection.table(QUESTIONS_TABLE)
        key = str(row.question_id)

        table.put(
            key,
            {
                b"question:title": row.title.encode("utf-8") if row.title else b"",
                b"question:body": row.body.encode("utf-8") if row.body else b"",
                b"question:creation_date": str(row.creation_date).encode("utf-8"),
                b"question:score": str(row.score).encode("utf-8"),
                b"question:tags": (
                    ".".join(row.tags).encode("utf-8") if row.tags else b""
                ),
                b"question:owner_reputation": str(row.owner_reputation).encode("utf-8"),
                b"question:is_answered": str(row.is_answered).encode("utf-8"),
            },
        )

        print(f"[INFO] Saved question ID {key}")
    except Exception as e:
        print(f"[ERROR] Failed to save question to HBase: {e}")
    finally:
        try:
            connection.close()
        except:
            pass


def save_answers_to_hbase(row):
    try:
        connection = happybase.Connection(HBASE_HOST, port=9090)
        table = connection.table(ANSWERS_TABLE)

        for ans in row.answers or []:
            row_key = f"{row.question_id}#{ans['answer_id']}"
            table.put(
                row_key,
                {
                    b"answer:body": ans["body"].encode("utf-8"),
                    b"answer:score": str(ans["score"]).encode("utf-8"),
                    b"answer:is_accepted": str(ans["is_accepted"]).encode("utf-8"),
                    b"answer:owner_reputation": str(ans["owner_reputation"]).encode(
                        "utf-8"
                    ),
                },
            )
        print(
            f"[INFO] Saved {len(row.answers or [])} answers for QID {row.question_id}"
        )
    except Exception as e:
        print(f"[ERROR] Failed to save answers to HBase: {e}")
    finally:
        try:
            connection.close()
        except:
            pass


def save_trend_to_hbase(row):
    try:
        connection = happybase.Connection(HBASE_HOST, port=9090)
        table = connection.table("stackoverflow_trends")
        tag_key = row.tag.encode("utf-8") if row.tag else b"unknown"
        table.put(tag_key, {b"trend:count": str(row.count).encode("utf-8")})
    except Exception as e:
        print(f"[ERROR] Failed to save trend to HBase: {e}")
    finally:
        try:
            connection.close()
        except:
            pass


def main():
    spark = (
        SparkSession.builder.appName("StackOverflowStreamProcessor")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.streaming.backpressure.enabled", "true")
        .getOrCreate()
    )

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:29092")
        .option("subscribe", "stackoverflow-questions")
        .option("startingOffsets", "earliest")
        .load()
    )

    json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")
    parsed_df = json_df.select(
        from_json(col("json_str"), question_schema).alias("data")
    ).select("data.*")

    def foreach_batch_function(batch_df, batch_id):
        for row in batch_df.collect():
            save_question_to_hbase(row)
            save_answers_to_hbase(row)

    query = parsed_df.writeStream.foreachBatch(foreach_batch_function).start()

    trend_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:29092")
        .option("subscribe", "stackoverflow-trends")
        .option("startingOffsets", "earliest")
        .load()
    )

    trend_json_df = trend_df.selectExpr("CAST(value AS STRING) as json_str")
    parsed_trend_df = trend_json_df.select(
        from_json(col("json_str"), trend_schema).alias("data")
    ).select("data.*")

    def trend_foreach_batch(batch_df, batch_id):
        for row in batch_df.collect():
            save_trend_to_hbase(row)

    trend_query = parsed_trend_df.writeStream.foreachBatch(trend_foreach_batch).start()

    query.awaitTermination()
    trend_query.awaitTermination()


if __name__ == "__main__":
    main()

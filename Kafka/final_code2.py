import pyspark
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *
import psycopg2
import time
from pyspark import StorageLevel
jdbc_driver_path = "/opt/spark/jars/postgresql-42.6.2.jar"  

# Create Spark session
spark = SparkSession.builder \
    .appName("Postgres to Spark").config("spark.jars", jdbc_driver_path) \
    .getOrCreate()

# postgres db connection
pg_url = "jdbc:postgresql://localhost:5432/postgres_capstone"
pg_properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}
table_name = "emp_time_data"

# Connect to PostgreSQL with psycopg2
conn = psycopg2.connect(
    dbname="postgres_capstone",
    user="postgres",
    password="postgres",
    host="localhost",
    port="5432"
)
cur = conn.cursor()

# Step 1: Read from kafka_messages table
staging_df = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres_capstone") \
    .option("user", "postgres") \
    .option("dbtable", "kafka_messages") \
    .option("password", "postgres") \
    .option("driver", "org.postgresql.Driver") \
    .load()

# Step 2: Filter flagged messages and process
filtered_df = staging_df.withColumn("timestamp", from_unixtime("timestamp"))
filtered_df = filtered_df.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))

# Step 3: Write into history table kafka_messages_history
filtered_df.write.format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres_capstone") \
    .option("user", "postgres") \
    .option("dbtable", "kafka_messages_history") \
    .option("password", "postgres") \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()

filtered_df = filtered_df.filter(col("flag") == True)

# Step 4: Reading Emp_time_data
normal_emp_timeframe_df = spark.read.jdbc(url=pg_url, table='emp_time_data', properties=pg_properties)

# putting data into backup table
try:
    cur.execute("BEGIN;")
    cur.execute("INSERT INTO emp_timeframe_df_backup (emp_id,start_date,end_date,designation,salary,status) SELECT emp_id,start_date,end_date,designation,salary,status FROM emp_time_data;")
    cur.execute("COMMIT;")
    
    print("emp_timeframe_df_backup Query executed successfully.")
    print(cur.statusmessage)

except Exception as e:
    cur.execute("ROLLBACK;")
    print("Error:", e)

# Reading from emp_timeframe_df_backup table
full_emp_timeframe_df = spark.read.jdbc(url=pg_url, table='emp_timeframe_df_backup', properties=pg_properties)
print("full emp ",full_emp_timeframe_df.count())

emp_timeframe_df = full_emp_timeframe_df.filter((col("status") == "ACTIVE") & col("end_date").isNull())
print("emp ",emp_timeframe_df.count())

inactive_emp_timeframe_df = full_emp_timeframe_df.filter((col("status") == "INACTIVE"))
print("inactive df", inactive_emp_timeframe_df.count())

# Step 5: Reading existing_strike_data
existing_strike_df = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres_capstone") \
    .option("dbtable", "strike_table") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .option("driver", "org.postgresql.Driver") \
    .load()

# Step 6: get new emp and add them into strike data
new_emps_df = emp_timeframe_df.alias("emp") \
    .join(existing_strike_df.alias("strike"), col("emp.emp_id") == col("strike.sender"), "left_anti")

strike_df = (new_emps_df.alias("t").select(
            col("t.emp_id").cast(LongType()).alias("sender"),
            col("t.salary").cast(LongType()).alias("actual_salary"), 
            lit(0.0).alias("strike1"),
            lit(0.0).alias("strike2"),
            lit(0.0).alias("strike3"),
            lit(0.0).alias("strike4"),
            lit(0.0).alias("strike5"),
            lit(0.0).alias("strike6"),
            lit(0.0).alias("strike7"),
            lit(0.0).alias("strike8"),
            lit(0.0).alias("strike9"),
            lit(0.0).alias("strike10"),
            col("t.salary").cast(LongType()).alias("current_salary"),  
            lit(0).alias("num_of_strikes"),
            current_timestamp().alias("load_time")
        )
    )
try:
    strike_df.write.format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/postgres_capstone") \
            .option("user", "postgres") \
            .option("dbtable", "strike_table") \
            .option("password", "postgres") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
    print("strike_df success")
except:
    print("fail")

# Step 7: reading finally updated strike table from db
normal_strike_df = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres_capstone") \
    .option("dbtable", "strike_table") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .option("driver", "org.postgresql.Driver") \
    .load()
print("normal_strike_df", normal_strike_df.show())

# Step 8: Backup the strike_data table
try:
    cur.execute("BEGIN;")
    cur.execute("""
        INSERT INTO strike_table_backup (sender,actual_salary,strike1,strike2,strike3,strike4,strike5,strike6,strike7,strike8,strike9,strike10,current_salary,num_of_strikes,load_time)
        SELECT
            sender,actual_salary,strike1,strike2,strike3,strike4,strike5,strike6,strike7,strike8,strike9,strike10,current_salary,num_of_strikes,load_time
        FROM strike_table;
    """)
    cur.execute("COMMIT;")
    
    print("Data copied successfully from strike_table to strike_table_backup.")
    print(cur.statusmessage)

except Exception as e:
    cur.execute("ROLLBACK;")
    print("Error:", e)

# Step 9: Read from backup (NOT from original table)
strike_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres_capstone") \
    .option("dbtable", "strike_table_backup") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .option("driver", "org.postgresql.Driver") \
    .load()

# Step 10: Creating sender,timestamp table
query = """
SELECT sender, timestamp, flag 
FROM kafka_messages_history
WHERE flag = TRUE 
AND timestamp >= CURRENT_DATE - INTERVAL '31 days'
"""

strike_date_df = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres_capstone") \
    .option("user", "postgres") \
    .option("dbtable", f"({query}) as filtered_data") \
    .option("password", "postgres") \
    .option("driver", "org.postgresql.Driver") \
    .load()

# Convert timestamp to proper timestamp format
strike_date_df = strike_date_df.withColumn("timestamp", to_timestamp(col("timestamp")))

# Drop flag column after loading
strike_date_df = strike_date_df.drop("flag")

active_strike_date_df = strike_date_df.join(emp_timeframe_df, strike_date_df.sender == emp_timeframe_df.emp_id, "inner") \
     .select("sender", "timestamp")

print("active_strike_date cnt",active_strike_date_df.count())

strike_count_df = active_strike_date_df.groupBy("sender").count()

# Join the two DataFrames on 'sender'
df = strike_df.join(strike_count_df, on='sender', how='left')

# 1. Define the first strike as 90% of the actual salary
df = df.withColumn(
    'strike1',
    when(col('count') >= 1, col('actual_salary') * 0.9).otherwise(None)
)
# Step 9: Read from backup (NOT from original table)
strike_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres_capstone") \
    .option("dbtable", "strike_table_backup") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .option("driver", "org.postgresql.Driver") \
    .load()

# Step 10: Creating sender,timestamp table
query = """
SELECT sender, timestamp, flag 
FROM kafka_messages_history
WHERE flag = TRUE 
AND timestamp >= CURRENT_DATE - INTERVAL '31 days'
"""

strike_date_df = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres_capstone") \
    .option("user", "postgres") \
    .option("dbtable", f"({query}) as filtered_data") \
    .option("password", "postgres") \
    .option("driver", "org.postgresql.Driver") \
    .load()

# Convert timestamp to proper timestamp format
strike_date_df = strike_date_df.withColumn("timestamp", to_timestamp(col("timestamp")))

# Drop flag column after loading
strike_date_df = strike_date_df.drop("flag")

active_strike_date_df = strike_date_df.join(emp_timeframe_df, strike_date_df.sender == emp_timeframe_df.emp_id, "inner") \
     .select("sender", "timestamp")

print("active_strike_date cnt",active_strike_date_df.count())

strike_count_df = active_strike_date_df.groupBy("sender").count()

# Join the two DataFrames on 'sender'
df = strike_df.join(strike_count_df, on='sender', how='left')

# 1. Define the first strike as 90% of the actual salary
df = df.withColumn(
    'strike1',
    when(col('count') >= 1, col('actual_salary') * 0.9).otherwise(None)
)
# 2. For each subsequent strike, calculate 90% of the previous strike value
for i in range(2, 11):
    strike_col = f"strike{i}"
    prev_strike_col = f"strike{i-1}"
    
    df = df.withColumn(
        strike_col,
        when(col('count') >= i, col(prev_strike_col) * 0.9).otherwise(None)
    )

# 3. Update the current salary based on the last non-null strike column
df = df.withColumn(
    'current_salary',
    coalesce(
        col('strike10'),
        col('strike9'),
        col('strike8'),
        col('strike7'),
        col('strike6'),
        col('strike5'),
        col('strike4'),
        col('strike3'),
        col('strike2'),
        col('strike1'),
        col('actual_salary')  # If all strikes are null, fall back to actual_salary
    )
)
df = df.withColumn(
    'num_of_strikes',
    when(col('count') > 10, 10).otherwise(coalesce(col('count'), lit(0)))
).drop('count')


# Assuming emp_timeframe_df and strike_df are already loaded    
# Join emp_timeframe_df with strike_df on emp_id and sender (assumed sender corresponds to emp_id)
xemp_timeframe_df = emp_timeframe_df.alias("emp") \
    .join(strike_df.alias("strike"), col("emp_id") == col("strike.sender"), "left") \
    .withColumn(
        "status",
        # If any of the strike columns is NULL, make status "ACTIVE"
        when(
            col('strike.strike1').isNotNull() & col('strike.strike2').isNotNull() & 
            col('strike.strike3').isNotNull() & col('strike.strike4').isNotNull() & 
            col('strike.strike5').isNotNull() & col('strike.strike6').isNotNull() &
            col('strike.strike7').isNotNull() & col('strike.strike8').isNotNull() & 
            col('strike.strike9').isNotNull() & col('strike.strike10').isNotNull(),
            "INACTIVE"  # All strike columns are not null, hence mark as inactive
        ).otherwise("ACTIVE")  # Otherwise mark as ACTIVE
    )\
    .withColumn(
        "end_date",
        when(
            col('strike.strike1').isNotNull() & col('strike.strike2').isNotNull() & 
            col('strike.strike3').isNotNull() & col('strike.strike4').isNotNull() & 
            col('strike.strike5').isNotNull() & col('strike.strike6').isNotNull() &
            col('strike.strike7').isNotNull() & col('strike.strike8').isNotNull() & 
            col('strike.strike9').isNotNull() & col('strike.strike10').isNotNull(),
            to_date(col("strike.load_time"))  # Convert timestamp to date
        ).otherwise(col("emp.end_date"))) \
        .select(
        col("emp.emp_id"),
        col("emp.designation"),
        col("emp.start_date"),
        col("end_date"),
        col("emp.salary"),
        col("status")
    )

print("saving emp_timeframe_df", xemp_timeframe_df.show())
print("naman", xemp_timeframe_df.filter(xemp_timeframe_df.emp_id=='5583010374').show())
xemp_timeframe_df = xemp_timeframe_df.cache()
print("abcd", xemp_timeframe_df.filter(xemp_timeframe_df.emp_id=='5583010374').show())

count_sender = df.filter(col("strike1").isNotNull()).select("sender").count()

print(f"Count of senders where strike1 is not null: {count_sender}")


try:
    if df.rdd.isEmpty():
        print("Transformed DataFrame is empty. Restoring original data from backup.")
        # Write backup data back
        normal_strike_df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/postgres_capstone") \
            .option("dbtable", "strike_table") \
            .option("user", "postgres") \
            .option("password", "postgres") \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
    else:
        print("Writing transformed data to strike_table...")
        print("strike_df", df.show())

        df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/postgres_capstone") \
            .option("dbtable", "strike_table") \
            .option("user", "postgres") \
            .option("password", "postgres") \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()

    # Step 4: Truncate backup only after successful write
    cur.execute("TRUNCATE TABLE strike_table_backup;")
    conn.commit()

except Exception as e:
    print("Write failed:", e)


try:
    if xemp_timeframe_df.rdd.isEmpty():
        print("Transformed DataFrame is empty. Restoring original data from backup.")
        # Write backup data back
        normal_emp_timeframe_df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/postgres_capstone") \
            .option("dbtable", "emp_time_data") \
            .option("user", "postgres") \
            .option("password", "postgres") \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
    else:
        print("Writing transformed data to emp_timeframe_df...")
        print("naman", xemp_timeframe_df.filter(xemp_timeframe_df.emp_id=='5583010374').show())
        print("naman", inactive_emp_timeframe_df.filter(inactive_emp_timeframe_df.emp_id=='5583010374').show())
        final_df = xemp_timeframe_df.unionByName(inactive_emp_timeframe_df)
        print("final df", final_df.filter(final_df.emp_id=='5583010374').show())
        final_df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/postgres_capstone") \
            .option("dbtable", "emp_time_data") \
            .option("user", "postgres") \
            .option("password", "postgres") \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()

    # Step 4: Drop backup only after successful write
    cur.execute("Truncate TABLE emp_timeframe_df_backup;")
    conn.commit()

except Exception as e:
    print("Write failed:", e)

cur.close()
conn.close()

# Step 4: Truncate the staging table using psycopg2
conn = psycopg2.connect(
    host="localhost",
    database="postgres_capstone",
    user="postgres",
    password="postgres"
)
cur = conn.cursor()
cur.execute("TRUNCATE TABLE kafka_messages")  # ⚠️ This clears all rows!
conn.commit()
cur.close()
conn.close()


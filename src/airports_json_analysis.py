# ============================================================
# 1. IMPORT LIBRARIES
# ============================================================
from pyspark.sql import SparkSession

from pyspark.sql.functions import (
    col,
    trim,
    upper,
    count,
    avg,
    min,
    max,
    round,
    when,
    desc,
    explode
)

import sys
from datetime import datetime

# ============================================================
# LOGGING - Save ALL terminal output to a text file
# ============================================================
log_filename = f"airports_output_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

class Logger:
    def __init__(self, filename):
        self.terminal = sys.stdout
        self.log = open(filename, "w", encoding="utf-8")

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
        self.log.flush()

    def flush(self):
        self.terminal.flush()
        self.log.flush()

sys.stdout = Logger(log_filename)
print(f"All terminal output is being saved to: {log_filename}")
print("=" * 70)


# ============================================================
# 2. CREATE SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("AirportDataAnalysis")
    .getOrCreate()
)

# Reduce Spark logs
spark.sparkContext.setLogLevel("WARN")


# ============================================================
# 3. DEFINE HDFS PATHS
# ============================================================

INPUT_PATH = (
    "hdfs://localhost:9000/"
    "user/hadoop/project/airports.json"
)

OUTPUT_BASE = (
    "hdfs://localhost:9000/"
    "user/hadoop/project/airport_output"
)


# ============================================================
# 4. READ JSON FILE
# ============================================================

print("=" * 60)
print("READING AIRPORT JSON DATA")
print("=" * 60)

raw_df = (
    spark.read
    .option("multiline", "true")
    .json(INPUT_PATH)
)

df = (
    raw_df
    .select(explode(col("airports")).alias("airport"))
    .select("airport.*")
)

print("\n========== RAW DATA ==========")
df.show(truncate=False)

print("\n========== SCHEMA ==========")
df.printSchema()

# ============================================================
# 5. DISPLAY RAW DATA
# ============================================================

print("\n========== RAW DATA ==========")

df.show(
    truncate=False
)


# ============================================================
# 6. DISPLAY SCHEMA
# ============================================================

print("\n========== DATA SCHEMA ==========")

df.printSchema()


# ============================================================
# 7. DATASET INFORMATION
# ============================================================

print("\n========== DATASET INFORMATION ==========")

total_airports = df.count()
total_columns = len(df.columns)

print(f"Number of airports : {total_airports}")
print(f"Number of columns : {total_columns}")

print("\nColumns:")
print(df.columns)


# ============================================================
# 8. DATA QUALITY CHECKS
# ============================================================

print("\n" + "=" * 60)
print("DATA QUALITY CHECKS")
print("=" * 60)


# ------------------------------------------------------------
# Missing airport codes
# ------------------------------------------------------------

missing_codes = df.filter(
    col("code").isNull()
).count()


# ------------------------------------------------------------
# Missing names
# ------------------------------------------------------------

missing_names = df.filter(
    col("name").isNull()
).count()


# ------------------------------------------------------------
# Missing cities
# ------------------------------------------------------------

missing_cities = df.filter(
    col("city").isNull()
).count()


# ------------------------------------------------------------
# Missing countries
# ------------------------------------------------------------

missing_countries = df.filter(
    col("country").isNull()
).count()


# ------------------------------------------------------------
# Missing latitude
# ------------------------------------------------------------

missing_latitude = df.filter(
    col("lat").isNull()
).count()


# ------------------------------------------------------------
# Missing longitude
# ------------------------------------------------------------

missing_longitude = df.filter(
    col("lon").isNull()
).count()


print(f"Missing airport codes : {missing_codes}")
print(f"Missing airport names : {missing_names}")
print(f"Missing cities        : {missing_cities}")
print(f"Missing countries     : {missing_countries}")
print(f"Missing latitude      : {missing_latitude}")
print(f"Missing longitude     : {missing_longitude}")


# ============================================================
# 9. CHECK DUPLICATE AIRPORT CODES
# ============================================================

print("\n========== DUPLICATE AIRPORT CHECK ==========")

duplicate_codes = (
    df.groupBy("code")
    .count()
    .filter(col("count") > 1)
)

duplicate_count = duplicate_codes.count()

print(
    f"Number of duplicated airport codes: "
    f"{duplicate_count}"
)


# ============================================================
# 10. CLEAN THE DATA
# ============================================================

print("\n========== CLEANING DATA ==========")

df_clean = (
    df
    .withColumn(
        "code",
        upper(trim(col("code")))
    )
    .withColumn(
        "name",
        trim(col("name"))
    )
    .withColumn(
        "city",
        trim(col("city"))
    )
    .withColumn(
        "country",
        upper(trim(col("country")))
    )
)


# ============================================================
# 11. REMOVE DUPLICATE AIRPORTS
# ============================================================

df_clean = df_clean.dropDuplicates(
    ["code"]
)

print(
    f"Airports after duplicate removal: "
    f"{df_clean.count()}"
)


# ============================================================
# 12. VALIDATE LATITUDE AND LONGITUDE
# ============================================================

print("\n========== LOCATION VALIDATION ==========")

df_clean = df_clean.withColumn(
    "location_valid",
    when(
        col("lat").between(-90, 90) &
        col("lon").between(-180, 180),
        True
    ).otherwise(False)
)


# ============================================================
# 13. CREATE HEMISPHERE
# ============================================================

df_clean = df_clean.withColumn(
    "hemisphere",
    when(
        col("lat") >= 0,
        "Northern"
    ).otherwise(
        "Southern"
    )
)


# ============================================================
# 14. CREATE EAST / WEST REGION
# ============================================================

df_clean = df_clean.withColumn(
    "longitude_region",
    when(
        col("lon") >= 0,
        "Eastern"
    ).otherwise(
        "Western"
    )
)


# ============================================================
# 15. CREATE LOCATION CATEGORY
# ============================================================

df_clean = df_clean.withColumn(
    "location_category",
    when(
        (col("lat") >= 0) &
        (col("lon") >= 0),
        "Northern & Eastern"
    )
    .when(
        (col("lat") >= 0) &
        (col("lon") < 0),
        "Northern & Western"
    )
    .when(
        (col("lat") < 0) &
        (col("lon") >= 0),
        "Southern & Eastern"
    )
    .otherwise(
        "Southern & Western"
    )
)


# ============================================================
# 16. DISPLAY CLEANED DATA
# ============================================================

print("\n========== CLEANED DATA ==========")

df_clean.show(
    truncate=False
)


# ============================================================
# 17. COUNTRY ANALYSIS
# ============================================================

print("\n" + "=" * 60)
print("AIRPORTS BY COUNTRY")
print("=" * 60)

country_analysis = (
    df_clean
    .groupBy("country")
    .agg(
        count("*").alias("airport_count")
    )
    .orderBy(
        desc("airport_count")
    )
)

country_analysis.show(
    truncate=False
)


# ============================================================
# 18. CITY ANALYSIS
# ============================================================

print("\n" + "=" * 60)
print("AIRPORTS BY CITY")
print("=" * 60)

city_analysis = (
    df_clean
    .groupBy("city")
    .agg(
        count("*").alias("airport_count")
    )
    .orderBy(
        desc("airport_count")
    )
)

city_analysis.show(
    truncate=False
)


# ============================================================
# 19. HEMISPHERE ANALYSIS
# ============================================================

print("\n" + "=" * 60)
print("AIRPORTS BY HEMISPHERE")
print("=" * 60)

hemisphere_analysis = (
    df_clean
    .groupBy("hemisphere")
    .agg(
        count("*").alias("airport_count")
    )
)

hemisphere_analysis.show()


# ============================================================
# 20. EAST / WEST ANALYSIS
# ============================================================

print("\n" + "=" * 60)
print("AIRPORTS BY LONGITUDE REGION")
print("=" * 60)

longitude_analysis = (
    df_clean
    .groupBy("longitude_region")
    .agg(
        count("*").alias("airport_count")
    )
)

longitude_analysis.show()


# ============================================================
# 21. LOCATION CATEGORY ANALYSIS
# ============================================================

print("\n" + "=" * 60)
print("AIRPORTS BY LOCATION CATEGORY")
print("=" * 60)

location_analysis = (
    df_clean
    .groupBy("location_category")
    .agg(
        count("*").alias("airport_count")
    )
    .orderBy(
        desc("airport_count")
    )
)

location_analysis.show(
    truncate=False
)


# ============================================================
# 22. LATITUDE / LONGITUDE STATISTICS
# ============================================================

print("\n" + "=" * 60)
print("GEOGRAPHICAL STATISTICS")
print("=" * 60)

location_statistics = (
    df_clean
    .select(
        round(avg("lat"), 2).alias(
            "average_latitude"
        ),
        round(avg("lon"), 2).alias(
            "average_longitude"
        ),
        round(min("lat"), 2).alias(
            "minimum_latitude"
        ),
        round(max("lat"), 2).alias(
            "maximum_latitude"
        ),
        round(min("lon"), 2).alias(
            "minimum_longitude"
        ),
        round(max("lon"), 2).alias(
            "maximum_longitude"
        )
    )
)

location_statistics.show(
    truncate=False
)


# ============================================================
# 23. NORTHERNMOST AIRPORT
# ============================================================

print("\n========== NORTHERNMOST AIRPORT ==========")

northernmost = (
    df_clean
    .orderBy(
        desc("lat")
    )
    .select(
        "code",
        "name",
        "city",
        "country",
        "lat",
        "lon"
    )
    .limit(1)
)

northernmost.show(
    truncate=False
)


# ============================================================
# 24. SOUTHERNMOST AIRPORT
# ============================================================

print("\n========== SOUTHERNMOST AIRPORT ==========")

southernmost = (
    df_clean
    .orderBy(
        "lat"
    )
    .select(
        "code",
        "name",
        "city",
        "country",
        "lat",
        "lon"
    )
    .limit(1)
)

southernmost.show(
    truncate=False
)


# ============================================================
# 25. EASTERNMOST AIRPORT
# ============================================================

print("\n========== EASTERNMOST AIRPORT ==========")

easternmost = (
    df_clean
    .orderBy(
        desc("lon")
    )
    .select(
        "code",
        "name",
        "city",
        "country",
        "lat",
        "lon"
    )
    .limit(1)
)

easternmost.show(
    truncate=False
)


# ============================================================
# 26. WESTERNMOST AIRPORT
# ============================================================

print("\n========== WESTERNMOST AIRPORT ==========")

westernmost = (
    df_clean
    .orderBy(
        "lon"
    )
    .select(
        "code",
        "name",
        "city",
        "country",
        "lat",
        "lon"
    )
    .limit(1)
)

westernmost.show(
    truncate=False
)


# ============================================================
# 27. VALID AIRPORT LOCATIONS
# ============================================================

print("\n========== LOCATION QUALITY ==========")

valid_locations = df_clean.filter(
    col("location_valid") == True
).count()

invalid_locations = df_clean.filter(
    col("location_valid") == False
).count()

print(f"Valid locations   : {valid_locations}")
print(f"Invalid locations : {invalid_locations}")


# ============================================================
# 28. CREATE FINAL CLEAN DATASET
# ============================================================

final_df = df_clean.select(
    "code",
    "name",
    "city",
    "country",
    "lat",
    "lon",
    "location_valid",
    "hemisphere",
    "longitude_region",
    "location_category"
)


# ============================================================
# 29. WRITE CLEAN DATA AS PARQUET
# ============================================================

print("\n" + "=" * 60)
print("WRITING CLEAN PARQUET DATA")
print("=" * 60)

(
    final_df
    .write
    .mode("overwrite")
    .format("parquet")
    .option("compression", "snappy")
    .save(
        f"{OUTPUT_BASE}/clean_airports"
    )
)

print("Clean Parquet data written successfully.")


# ============================================================
# 30. WRITE COUNTRY ANALYSIS
# ============================================================

print("\nWriting country analysis...")

(
    country_analysis
    .write
    .mode("overwrite")
    .format("parquet")
    .save(
        f"{OUTPUT_BASE}/country_analysis"
    )
)


# ============================================================
# 31. WRITE CITY ANALYSIS
# ============================================================

print("Writing city analysis...")

(
    city_analysis
    .write
    .mode("overwrite")
    .format("parquet")
    .save(
        f"{OUTPUT_BASE}/city_analysis"
    )
)


# ============================================================
# 32. WRITE HEMISPHERE ANALYSIS
# ============================================================

print("Writing hemisphere analysis...")

(
    hemisphere_analysis
    .write
    .mode("overwrite")
    .format("parquet")
    .save(
        f"{OUTPUT_BASE}/hemisphere_analysis"
    )
)


# ============================================================
# 33. WRITE LOCATION ANALYSIS
# ============================================================

print("Writing location analysis...")

(
    location_analysis
    .write
    .mode("overwrite")
    .format("parquet")
    .save(
        f"{OUTPUT_BASE}/location_analysis"
    )
)


# ============================================================
# 34. WRITE CSV FOR EASY VIEWING
# ============================================================
# CSV is useful for opening results in Excel or Power BI.
#
# coalesce(1) is acceptable here because this is a small dataset.
# ============================================================

print("\n========== WRITING CSV FILES ==========")


(
    country_analysis
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv(
        f"{OUTPUT_BASE}/csv/country_analysis"
    )
)


(
    city_analysis
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv(
        f"{OUTPUT_BASE}/csv/city_analysis"
    )
)


(
    hemisphere_analysis
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv(
        f"{OUTPUT_BASE}/csv/hemisphere_analysis"
    )
)


(
    location_analysis
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv(
        f"{OUTPUT_BASE}/csv/location_analysis"
    )
)


# ============================================================
# 35. FINAL SUMMARY
# ============================================================

print("\n" + "=" * 60)
print("PROJECT COMPLETED SUCCESSFULLY")
print("=" * 60)

print(f"Total airports processed : {total_airports}")
print(f"Final airports           : {final_df.count()}")

print("\nHDFS output location:")
print(OUTPUT_BASE)

print("\nGenerated datasets:")
print("  - clean_airports")
print("  - country_analysis")
print("  - city_analysis")
print("  - hemisphere_analysis")
print("  - location_analysis")
print("  - csv/")

print(f"\n>>> Full terminal log saved to: {log_filename}")


# ============================================================
# 36. STOP SPARK
# ============================================================

spark.stop()

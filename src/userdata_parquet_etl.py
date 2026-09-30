# ============================================================
# 1. IMPORT LIBRARIES
# ============================================================

from pyspark.sql import SparkSession

from pyspark.sql.functions import (
    col,
    when,
    lit,
    trim,
    lower,
    regexp_replace,
    to_timestamp,
    try_to_date,
    year,
    month,
    dayofmonth,
    dayofweek,
    hour,
    datediff,
    current_date,
    floor,
    avg,
    min,
    max,
    count,
    countDistinct,
    desc,
    round,
    length,
    split,
    regexp_extract,
    initcap,
    concat_ws
)

import sys
from datetime import datetime

# ============================================================
# LOGGING - Save ALL terminal output to a text file
# ============================================================
log_filename = f"userdata_output_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

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
    .appName("UserDataProfessionalETL")
    .getOrCreate()
)

# Reduce unnecessary Spark logs
spark.sparkContext.setLogLevel("WARN")


# ============================================================
# 3. DEFINE HDFS PATHS
# ============================================================

RAW_PATH = (
    "hdfs://localhost:9000/"
    "user/hadoop/project/userdata.parquet"
)

SILVER_PATH = (
    "hdfs://localhost:9000/"
    "user/hadoop/project/silver/userdata_cleaned"
)

GOLD_BASE_PATH = (
    "hdfs://localhost:9000/"
    "user/hadoop/project/gold"
)


# ============================================================
# 4. READ RAW PARQUET DATA
# ============================================================

print("\n" + "=" * 70)
print("READING RAW PARQUET DATA")
print("=" * 70)

df = spark.read.parquet(RAW_PATH)


# ============================================================
# 5. DISPLAY RAW DATA
# ============================================================

print("\n========== RAW DATA ==========")

df.show(10, truncate=False)


# ============================================================
# 6. DISPLAY RAW SCHEMA
# ============================================================

print("\n========== RAW SCHEMA ==========")

df.printSchema()


# ============================================================
# 7. DATASET INFORMATION
# ============================================================

total_records = df.count()
total_columns = len(df.columns)

print("\n========== DATASET INFORMATION ==========")

print(f"Number of rows    : {total_records}")
print(f"Number of columns : {total_columns}")

print("\nColumns:")
print(df.columns)


# ============================================================
# 8. CHECK DUPLICATE IDS
# ============================================================

print("\n========== DUPLICATE ID ANALYSIS ==========")

duplicate_ids = (
    df.groupBy("id")
    .count()
    .filter(col("count") > 1)
)

duplicate_id_count = duplicate_ids.count()

print(f"Duplicate IDs: {duplicate_id_count}")


# ============================================================
# 9. REMOVE DUPLICATE IDS
# ============================================================

df = df.dropDuplicates(["id"])

print("\n========== AFTER DUPLICATE REMOVAL ==========")

print(f"Number of rows: {df.count()}")


# ============================================================
# 10. STANDARDIZE COLUMN NAMES
# ============================================================

df = df.toDF(
    "registration_dttm",
    "id",
    "first_name",
    "last_name",
    "email",
    "gender",
    "ip_address",
    "cc",
    "country",
    "birthdate",
    "salary",
    "title",
    "comments"
)


# ============================================================
# 11. REPLACE EMPTY STRINGS WITH NULL
# ============================================================

string_columns = [
    "first_name",
    "last_name",
    "email",
    "gender",
    "ip_address",
    "cc",
    "country",
    "birthdate",
    "title",
    "comments"
]

for column_name in string_columns:

    df = df.withColumn(
        column_name,
        when(
            trim(col(column_name)) == "",
            None
        ).otherwise(
            trim(col(column_name))
        )
    )


# ============================================================
# 12. DATA TYPE STANDARDIZATION
# ============================================================

df = (
    df
    .withColumn(
        "registration_dttm",
        to_timestamp(col("registration_dttm"))
    )
    .withColumn(
        "id",
        col("id").cast("integer")
    )
    .withColumn(
        "salary",
        col("salary").cast("double")
    )
)


# ============================================================
# 13. BIRTH DATE CLEANING
# ============================================================
# IMPORTANT:
# try_to_date() prevents Spark from crashing when a bad value
# appears in the birthdate column.
#
# Example bad value:
# "Saint Eustatius and Saba "
#
# Instead of failing, Spark returns NULL.
# ============================================================

df = df.withColumn(
    "birth_date",
    try_to_date(
        trim(col("birthdate")),
        "M/d/yyyy"
    )
)


# ============================================================
# 14. BIRTH DATE VALIDATION
# ============================================================

df = df.withColumn(
    "birth_date_valid",
    when(
        col("birthdate").isNull(),
        False
    )
    .when(
        col("birth_date").isNotNull(),
        True
    )
    .otherwise(False)
)


# ============================================================
# 15. SHOW INVALID BIRTH DATES
# ============================================================

print("\n========== INVALID BIRTHDATES ==========")

df.filter(
    col("birthdate").isNotNull() &
    col("birth_date").isNull()
).select(
    "id",
    "country",
    "birthdate"
).show(20, truncate=False)


# ============================================================
# 16. AGE CALCULATION
# ============================================================

df = df.withColumn(
    "age",
    when(
        col("birth_date").isNotNull(),
        floor(
            datediff(
                current_date(),
                col("birth_date")
            ) / 365.25
        )
    )
)


# ============================================================
# 17. AGE VALIDATION
# ============================================================

df = df.withColumn(
    "age_valid",
    when(
        col("age").between(0, 120),
        True
    ).otherwise(False)
)


# ============================================================
# 18. AGE GROUP
# ============================================================

df = df.withColumn(
    "age_group",
    when(
        col("age").isNull(),
        "Unknown"
    )
    .when(
        col("age") < 18,
        "Under 18"
    )
    .when(
        col("age").between(18, 29),
        "18-29"
    )
    .when(
        col("age").between(30, 39),
        "30-39"
    )
    .when(
        col("age").between(40, 49),
        "40-49"
    )
    .when(
        col("age").between(50, 59),
        "50-59"
    )
    .otherwise(
        "60+"
    )
)


# ============================================================
# 19. REGISTRATION DATE FEATURES
# ============================================================

df = (
    df
    .withColumn(
        "registration_date",
        col("registration_dttm").cast("date")
    )
    .withColumn(
        "registration_year",
        year(col("registration_dttm"))
    )
    .withColumn(
        "registration_month",
        month(col("registration_dttm"))
    )
    .withColumn(
        "registration_day",
        dayofmonth(col("registration_dttm"))
    )
    .withColumn(
        "registration_day_of_week",
        dayofweek(col("registration_dttm"))
    )
    .withColumn(
        "registration_hour",
        hour(col("registration_dttm"))
    )
)


# ============================================================
# 20. CLEAN GENDER
# ============================================================

df = df.withColumn(
    "gender",
    initcap(
        trim(col("gender"))
    )
)


# ============================================================
# 21. CLEAN COUNTRY
# ============================================================

df = df.withColumn(
    "country",
    trim(
        regexp_replace(
            col("country"),
            r'["]+$',
            ""
        )
    )
)


# ============================================================
# 22. CLEAN TITLE
# ============================================================

df = df.withColumn(
    "title",
    trim(col("title"))
)


# ============================================================
# 23. EMAIL DOMAIN
# ============================================================

df = df.withColumn(
    "email_domain",
    lower(
        regexp_extract(
            col("email"),
            r"@(.+)$",
            1
        )
    )
)


# ============================================================
# 24. EMAIL VALIDATION
# ============================================================

df = df.withColumn(
    "email_valid",
    when(
        col("email").rlike(
            r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
        ),
        True
    ).otherwise(False)
)


# ============================================================
# 25. IP ADDRESS VALIDATION
# ============================================================

df = df.withColumn(
    "ip_valid",
    when(
        col("ip_address").rlike(
            r"^([0-9]{1,3}\.){3}[0-9]{1,3}$"
        ),
        True
    ).otherwise(False)
)


# ============================================================
# 26. CREDIT CARD DATA PROTECTION
# ============================================================
# NEVER expose the complete credit-card-like value.
#
# We keep only the last four digits for analytical purposes.
# ============================================================

df = df.withColumn(
    "cc_last4",
    when(
        col("cc").isNotNull(),
        regexp_extract(
            col("cc"),
            r"(\d{4})$",
            1
        )
    )
)


df = df.withColumn(
    "cc_present",
    when(
        col("cc").isNotNull(),
        True
    ).otherwise(False)
)


# Remove sensitive raw value
df = df.drop("cc")


# ============================================================
# 27. SALARY VALIDATION
# ============================================================

df = df.withColumn(
    "salary_valid",
    when(
        col("salary").isNull(),
        False
    )
    .when(
        col("salary") >= 0,
        True
    )
    .otherwise(False)
)


# ============================================================
# 28. SALARY BAND
# ============================================================

df = df.withColumn(
    "salary_band",
    when(
        col("salary").isNull(),
        "Unknown"
    )
    .when(
        col("salary") < 50000,
        "Under 50K"
    )
    .when(
        col("salary") < 100000,
        "50K-99K"
    )
    .when(
        col("salary") < 200000,
        "100K-199K"
    )
    .otherwise(
        "200K+"
    )
)


# ============================================================
# 29. FULL NAME
# ============================================================

df = df.withColumn(
    "full_name",
    concat_ws(
        " ",
        col("first_name"),
        col("last_name")
    )
)


# ============================================================
# 30. NAME LENGTH FEATURES
# ============================================================

df = (
    df
    .withColumn(
        "first_name_length",
        length(col("first_name"))
    )
    .withColumn(
        "last_name_length",
        length(col("last_name"))
    )
)


# ============================================================
# 31. COMMENTS FEATURES
# ============================================================

df = df.withColumn(
    "has_comment",
    when(
        col("comments").isNotNull() &
        (length(trim(col("comments"))) > 0),
        True
    ).otherwise(False)
)


# Detect possible HTML / script content
df = df.withColumn(
    "contains_html",
    when(
        col("comments").rlike(
            r"(?i)<[^>]+>"
        ),
        True
    ).otherwise(False)
)


# ============================================================
# 32. DATA QUALITY SCORE
# ============================================================
# Maximum score = 6
#
# 1. Email valid
# 2. IP valid
# 3. Salary valid
# 4. Birth date valid
# 5. Country available
# 6. Title available
# ============================================================

df = df.withColumn(
    "data_quality_score",

    (
        when(col("email_valid"), 1).otherwise(0)
        +
        when(col("ip_valid"), 1).otherwise(0)
        +
        when(col("salary_valid"), 1).otherwise(0)
        +
        when(col("birth_date_valid"), 1).otherwise(0)
        +
        when(
            col("country").isNotNull(),
            1
        ).otherwise(0)
        +
        when(
            col("title").isNotNull(),
            1
        ).otherwise(0)
    )
)


# ============================================================
# 33. DATA QUALITY CATEGORY
# ============================================================

df = df.withColumn(
    "data_quality_category",
    when(
        col("data_quality_score") >= 5,
        "High"
    )
    .when(
        col("data_quality_score") >= 3,
        "Medium"
    )
    .otherwise(
        "Low"
    )
)


# ============================================================
# 34. DISPLAY CLEANED DATA
# ============================================================

print("\n========== CLEANED DATA ==========")

df.select(
    "id",
    "full_name",
    "email",
    "email_domain",
    "gender",
    "country",
    "birth_date",
    "age",
    "age_group",
    "salary",
    "salary_band",
    "title",
    "data_quality_score"
).show(
    20,
    truncate=False
)


# ============================================================
# 35. DATA QUALITY SUMMARY
# ============================================================

print("\n" + "=" * 70)
print("DATA QUALITY SUMMARY")
print("=" * 70)

total_records = df.count()

missing_salaries = df.filter(
    col("salary").isNull()
).count()

invalid_birthdates = df.filter(
    col("birthdate").isNotNull() &
    col("birth_date").isNull()
).count()

missing_birthdates = df.filter(
    col("birthdate").isNull()
).count()

invalid_emails = df.filter(
    ~col("email_valid")
).count()

invalid_ips = df.filter(
    ~col("ip_valid")
).count()

invalid_ages = df.filter(
    ~col("age_valid")
).count()

html_comments = df.filter(
    col("contains_html")
).count()

print(f"Total records       : {total_records}")
print(f"Missing salaries    : {missing_salaries}")
print(f"Missing birthdates  : {missing_birthdates}")
print(f"Invalid birthdates : {invalid_birthdates}")
print(f"Invalid emails     : {invalid_emails}")
print(f"Invalid IPs        : {invalid_ips}")
print(f"Invalid ages       : {invalid_ages}")
print(f"HTML comments      : {html_comments}")


# ============================================================
# 36. COUNTRY ANALYSIS
# ============================================================

print("\n========== COUNTRY ANALYSIS ==========")

country_analysis = (
    df.groupBy("country")
    .agg(
        count("*").alias("total_users"),
        round(avg("salary"), 2).alias("avg_salary"),
        round(min("salary"), 2).alias("min_salary"),
        round(max("salary"), 2).alias("max_salary"),
        countDistinct("title").alias("unique_titles")
    )
    .orderBy(
        desc("total_users")
    )
)

country_analysis.show(20, truncate=False)


# ============================================================
# 37. JOB / TITLE SALARY ANALYSIS
# ============================================================

print("\n========== JOB SALARY ANALYSIS ==========")

job_salary_analysis = (
    df.filter(
        col("title").isNotNull()
    )
    .groupBy("title")
    .agg(
        count("*").alias("employee_count"),
        round(avg("salary"), 2).alias("avg_salary"),
        round(min("salary"), 2).alias("min_salary"),
        round(max("salary"), 2).alias("max_salary")
    )
    .orderBy(
        desc("avg_salary")
    )
)

job_salary_analysis.show(20, truncate=False)


# ============================================================
# 38. GENDER ANALYSIS
# ============================================================

print("\n========== GENDER ANALYSIS ==========")

gender_analysis = (
    df.groupBy("gender")
    .agg(
        count("*").alias("total_users"),
        round(avg("salary"), 2).alias("avg_salary"),
        round(avg("age"), 2).alias("avg_age")
    )
    .orderBy(
        desc("total_users")
    )
)

gender_analysis.show(truncate=False)


# ============================================================
# 39. AGE GROUP ANALYSIS
# ============================================================

print("\n========== AGE GROUP ANALYSIS ==========")

age_analysis = (
    df.groupBy("age_group")
    .agg(
        count("*").alias("total_users"),
        round(avg("salary"), 2).alias("avg_salary")
    )
    .orderBy(
        "age_group"
    )
)

age_analysis.show(truncate=False)


# ============================================================
# 40. REGISTRATION TREND ANALYSIS
# ============================================================

print("\n========== REGISTRATION TREND ==========")

registration_analysis = (
    df.groupBy(
        "registration_year",
        "registration_month"
    )
    .agg(
        count("*").alias("registrations")
    )
    .orderBy(
        "registration_year",
        "registration_month"
    )
)

registration_analysis.show(
    100,
    truncate=False
)


# ============================================================
# 41. EMAIL DOMAIN ANALYSIS
# ============================================================

print("\n========== EMAIL DOMAIN ANALYSIS ==========")

email_domain_analysis = (
    df.groupBy("email_domain")
    .agg(
        count("*").alias("user_count")
    )
    .orderBy(
        desc("user_count")
    )
)

email_domain_analysis.show(
    20,
    truncate=False
)


# ============================================================
# 42. DATA QUALITY ANALYSIS
# ============================================================

print("\n========== DATA QUALITY ANALYSIS ==========")

quality_analysis = (
    df.groupBy(
        "data_quality_category"
    )
    .agg(
        count("*").alias("record_count"),
        round(
            avg("data_quality_score"),
            2
        ).alias("avg_quality_score")
    )
    .orderBy(
        desc("record_count")
    )
)

quality_analysis.show(
    truncate=False
)


# ============================================================
# 43. SALARY STATISTICS
# ============================================================

print("\n========== SALARY STATISTICS ==========")

salary_statistics = (
    df.select(
        round(avg("salary"), 2).alias("average_salary"),
        round(min("salary"), 2).alias("minimum_salary"),
        round(max("salary"), 2).alias("maximum_salary")
    )
)

salary_statistics.show(
    truncate=False
)


# ============================================================
# 44. WRITE SILVER DATA
# ============================================================

print("\n" + "=" * 70)
print("WRITING SILVER DATA")
print("=" * 70)

(
    df.write
    .mode("overwrite")
    .format("parquet")
    .option("compression", "snappy")
    .save(SILVER_PATH)
)

print(f"Silver data written to:")
print(SILVER_PATH)


# ============================================================
# 45. WRITE GOLD COUNTRY ANALYSIS
# ============================================================

print("\nWriting Gold: Country Analysis")

(
    country_analysis.write
    .mode("overwrite")
    .format("parquet")
    .save(
        f"{GOLD_BASE_PATH}/country_analysis"
    )
)


# ============================================================
# 46. WRITE GOLD JOB ANALYSIS
# ============================================================

print("Writing Gold: Job Salary Analysis")

(
    job_salary_analysis.write
    .mode("overwrite")
    .format("parquet")
    .save(
        f"{GOLD_BASE_PATH}/job_salary_analysis"
    )
)


# ============================================================
# 47. WRITE GOLD GENDER ANALYSIS
# ============================================================

print("Writing Gold: Gender Analysis")

(
    gender_analysis.write
    .mode("overwrite")
    .format("parquet")
    .save(
        f"{GOLD_BASE_PATH}/gender_analysis"
    )
)


# ============================================================
# 48. WRITE GOLD AGE ANALYSIS
# ============================================================

print("Writing Gold: Age Analysis")

(
    age_analysis.write
    .mode("overwrite")
    .format("parquet")
    .save(
        f"{GOLD_BASE_PATH}/age_analysis"
    )
)


# ============================================================
# 49. WRITE GOLD REGISTRATION ANALYSIS
# ============================================================

print("Writing Gold: Registration Analysis")

(
    registration_analysis.write
    .mode("overwrite")
    .format("parquet")
    .save(
        f"{GOLD_BASE_PATH}/registration_analysis"
    )
)


# ============================================================
# 50. WRITE GOLD EMAIL ANALYSIS
# ============================================================

print("Writing Gold: Email Domain Analysis")

(
    email_domain_analysis.write
    .mode("overwrite")
    .format("parquet")
    .save(
        f"{GOLD_BASE_PATH}/email_domain_analysis"
    )
)


# ============================================================
# 51. WRITE GOLD DATA QUALITY ANALYSIS
# ============================================================

print("Writing Gold: Data Quality Analysis")

(
    quality_analysis.write
    .mode("overwrite")
    .format("parquet")
    .save(
        f"{GOLD_BASE_PATH}/data_quality_analysis"
    )
)


# ============================================================
# 52. WRITE OPTIONAL CSV EXPORTS
# ============================================================
# CSV is useful for Power BI / Excel.
# Parquet remains the main Data Engineering format.


print("\n========== WRITING CSV EXPORTS ==========")

(
    country_analysis
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv(
        f"{GOLD_BASE_PATH}/csv/country_analysis"
    )
)


(
    job_salary_analysis
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv(
        f"{GOLD_BASE_PATH}/csv/job_salary_analysis"
    )
)


(
    gender_analysis
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv(
        f"{GOLD_BASE_PATH}/csv/gender_analysis"
    )
)


(
    age_analysis
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv(
        f"{GOLD_BASE_PATH}/csv/age_analysis"
    )
)


# ============================================================
# 53. FINAL MESSAGE
# ============================================================

print("\n" + "=" * 70)
print("ETL PIPELINE COMPLETED SUCCESSFULLY")
print("=" * 70)

print("\nSilver:")
print(SILVER_PATH)

print("\nGold:")
print(GOLD_BASE_PATH)

print("\nTotal records processed:")
print(total_records)

print(f"\n>>> Full terminal log saved to: {log_filename}")

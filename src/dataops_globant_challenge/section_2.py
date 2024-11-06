from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, ceil, month, year, avg, count
from pyspark.sql.types import IntegerType, DateType

spark: SparkSession = SparkSession.builder.appName("Section 1").getOrCreate()

df_hired_employees: DataFrame = (
    spark.read.format("csv")
    .options(**{"inferSchema": False})
    .load("../hired_employees.csv")
    .select(
        col("_c0").cast(IntegerType()).alias("id"),
        col("_c1").alias("name"),
        col("_c2").cast(DateType()).alias("datetime"),
        col("_c3").cast(IntegerType()).alias("department_id"),
        col("_c4").cast(IntegerType()).alias("job_id"),
    )
)

df_jobs: DataFrame = (
    spark.read.format("csv")
    .options(**{"inferSchema": False})
    .load("../jobs.csv")
    .select(col("_c0").cast(IntegerType()).alias("id"), col("_c1").alias("job"))
)

df_departments: DataFrame = (
    spark.read.format("csv")
    .options(**{"inferSchema": False})
    .load("../departments.csv")
    .select(col("_c0").cast(IntegerType()).alias("id"), col("_c1").alias("department"))
)


#
# Number of employees hired for each job and department in 2021 divided by quarter. The
# table must be ordered alphabetically by department and job.
#

df_pivot: DataFrame = (
    (
        df_hired_employees.withColumn(
            "quarter", ceil(month(col("datetime")) / lit(3))
        ).where(year(col("datetime")) == 2021)
    )
    .groupBy("department_id", "job_id")
    .pivot("quarter")
    .count()
)

df_response: DataFrame = (
    df_pivot.join(
        df_departments,
        df_departments.id == df_pivot.department_id,
        "outer",
    )
    .join(df_jobs, df_jobs.id == df_pivot.job_id, "outer")
    .select(
        col("department"),
        col("job"),
        col("1").alias("Q1"),
        col("2").alias("Q2"),
        col("3").alias("Q3"),
        col("4").alias("Q4"),
    )
    .orderBy([col("department"), col("job")], ascending=[False, False])
    .na.fill(0)
).show()

#
# List of ids, name and number of employees hired of each department that hired more
# employees than the mean of employees hired in 2021 for all the departments, ordered
# by the number of employees hired (descending).
#

df_count_employees_hired_by_department: DataFrame = (
    df_hired_employees.where(year(col("datetime")) == 2021)
    .groupBy("department_id")
    .agg(count("*").alias("number_employees_hired"))
)

mean: float = df_count_employees_hired_by_department.select(
    avg("number_employees_hired").alias("mean")
).take(1)[0]["mean"]

(
    df_count_employees_hired_by_department.join(
        df_departments,
        df_departments.id == df_count_employees_hired_by_department.department_id,
    )
    .where(df_count_employees_hired_by_department.number_employees_hired > mean)
    .select(
        df_departments.id,
        df_departments.department,
        df_count_employees_hired_by_department.number_employees_hired,
    )
    .orderBy(
        [col("number_employees_hired")], ascending = [False]
    )
).show()

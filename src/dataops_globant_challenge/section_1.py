from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, ceil, month, year
from pyspark.sql.types import IntegerType, DateType

spark: SparkSession = (
        SparkSession.builder.appName("Section 1")
        .config("spark.jars.packages", "com.microsoft.sqlserver:mssql-jdbc:12.8.1.jre11")
        .getOrCreate()
)

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


(
    df_departments.write.format("jdbc").options(
            url= "jdbc:sqlserver://localhost:1433;databaseName=globant_challenge",
            connectionProvider = "mssql",
            user = "sa",
            password = "mi_contra123456",
            dbtable = "departments",
            batchsize = 1000,
            numpartitions = 4,
    ).save()
)

(
    df_hired_employees.write.format("jdbc").options(
            url= "jdbc:sqlserver://localhost:1433;databaseName=globant_challenge",
            connectionProvider= "mssql",
            user= "sa",
            password= "mi_contra123456",
            dbtable= "jobs",
            batchsize= 1000,
            numpartitions= 4,
    ).save()
)

(
    df_hired_employees.write.format("jdbc").options(
            url= "jdbc:sqlserver://localhost:1433;databaseName=globant_challenge",
            connectionProvider= "mssql",
            user= "sa",
            password= "mi_contra123456",
            dbtable= "hired_employees",
            batchsize= 1000,
            numpartitions= 4,
    ).save()
)

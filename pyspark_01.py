from pyspark.sql import SparkSession

spark = SparkSession.builder.appName(
    'Offline SigAssignment').getOrCreate()

df = spark.read.csv('data/*.csv', sep=',', header=True)
df.createOrReplaceTempView("full_data")


# print(df.count())

# query 1----
def query1a():
    sqlDF = spark.sql(
        "WITH temp_dense AS (SELECT Date,Company,(High-Open)/Open as up_Move , dense_rank() OVER ( partition by Date order by (High-Open)/Open desc ) as dense_rank FROM full_data) select Company, up_Move, Date FROM temp_dense where dense_rank=1")
    sqlDF = sqlDF.collect()
    result =[]
    for row in sqlDF:
      result.append({'Date':row["Date"],'Company':row["Company"], 'Positive_move':row["up_Move"]})
    return result


def query1b():
  sqlDF2 = spark.sql(
    "WITH temp_dense AS (SELECT Date,Company,(Open-Low)/Open as down_Move, dense_rank() OVER ( partition by Date order by (Open-Low)/Open desc ) as dense_rank FROM full_data) select Company, down_Move, Date FROM temp_dense where dense_rank=1")
  sqlDF2 = sqlDF2.collect()
  result = []
  for row in sqlDF2:
    result.append({'Date': row["Date"], 'Company': row["Company"], 'Negative_move': row["down_Move"]})
  return result

def query1c():
  sqlDF = spark.sql(
    "WITH temp_dense AS (SELECT Date,Company,(High-Open)/Open as up_Move , dense_rank() OVER ( partition by Date order by (High-Open)/Open desc ) as dense_rank FROM full_data) select Company, Date, up_Move FROM temp_dense where dense_rank=1")
  sqlDF2 = spark.sql(
    "WITH temp_dense AS (SELECT Date,Company,(Open-Low)/Open as down_Move, dense_rank() OVER ( partition by Date order by (Open-Low)/Open desc ) as dense_rank FROM full_data) select Company, Date, down_Move FROM temp_dense where dense_rank=1")
  # sqlDF3 = sqlDF.unionAll(sqlDF2)
  # sqlDF3= sqlDF2.unionByName(sqlDF, allowMissingColumns=True)
  sqlDF = sqlDF.withColumnRenamed("Company", "up_Company")
  sqlDF2 = sqlDF2.withColumnRenamed("Company", "down_Company")
  sqlDF = sqlDF.withColumnRenamed("Date", "up_Date")
  sqlDF = sqlDF.withColumnRenamed("Date", "down_Date")
  sqlDF3 = sqlDF.join(sqlDF2, sqlDF.up_Company == sqlDF2.down_Company, "inner")
  sqlDF3.createOrReplaceTempView("full_data2")
  sqlDF4 = spark.sql("SELECT up_Company, MAX(up_Move) as max, MAX(down_Move) as max_, up_Date from full_data2 group by up_Company,up_Date").collect()
  result = []
  for row in sqlDF4:
    result.append({'Company': row["up_Company"], 'positive_Move': row["max"], 'Negative_move': row["max_"], 'Date': row["up_Date"]})
  return result


# query 2 - done
def query2():
    sqlDF = spark.sql(
        "SELECT Company, date, Volume from full_data where volume in (SELECT max(Volume) from full_data group by date)")
    sqlDF = sqlDF.collect()
    result = []
    for row in sqlDF:
      result.append({'Date': row["date"], 'Company': row["Company"], 'Volume': row["Volume"]})
    return result


# query 3- previous - current
def query3():
    sqlDF = spark.sql("with temp_previous_close as (select Company,Open,Date,Close,LAG(Close,1,35.724998) over(partition by Company order by Date) as previous_close from full_data ASC) select Company,ABS(previous_close-Open) as max_gap from temp_previous_close order by max_gap DESC limit 1 ")
    sqlDF = sqlDF.collect()
    dictt = dict(sqlDF)
    return dictt


# query 4 - moved maximum
def query4():
    # sqlDF = spark.sql(
    #     "with cte as (select volume, rank() over(order by date) min_rank, rank() over (order by date desc) max_rank from full_data) select * from cte where min_rank =1 or max_rank=1")
    sqlDF = spark.sql(
      "select Company,Open,High,(High-Open) as max_diff from (Select Company, (Select Open from full_data limit 1) as Open, max(High) as High from full_data group by Company)full_data order by max_diff desc limit 1")
    sqlDF = sqlDF.collect()
    result = []
    for row in sqlDF:
      result.append({'Open': row["Open"], 'Company': row["Company"], 'High': row["High"], 'Max_move':row["max_diff"]})
    return result


# query 5 - done
def query5():
    sqlDF = spark.sql("SELECT Company, STDDEV(Close) as Standard_Deviation FROM full_data group by Company")
    sqlDF = sqlDF.collect()
    dictt = dict(sqlDF)
    return dictt


# query 6 - mean
def query6a():
    sqlDF = spark.sql("SELECT Company, avg(Close) as mean FROM full_data group by Company")
    sqlDF = sqlDF.collect()
    dictt = dict(sqlDF)
    return dictt

# query 6 - median
def query6b():
    sqlDF2 = spark.sql(
        "SELECT Company,PERCENTILE_CONT(0.5) within group (order by Close) over (partition by Company) as Median from full_data")
    sqlDF2 = sqlDF2.collect()
    dictt = dict(sqlDF2)
    return dictt

# query 7 - done
def query7():
    sqlDF = spark.sql("SELECT Company, avg(Volume) FROM full_data group by Company")
    sqlDF = sqlDF.collect()
    dictt = dict(sqlDF)
    return dictt


# query 8 - done
def query8():
    sqlDF = spark.sql(
        "SELECT Company, avg(volume) as Highest_avg_volume from full_data group by company order by avg(volume) desc limit 1")
    sqlDF = sqlDF.collect()
    dictt = dict(sqlDF)
    return dictt


# query 9 - done
def query9():
    sqlDF = spark.sql("SELECT date, company, MAX(High) as max, Min(Low) as min FROM full_data group by Company, date")
    sqlDF = sqlDF.collect()
    result = []
    for row in sqlDF:
      result.append({'Company': row["company"], 'Date': row["date"], 'Highest_price': row["max"], 'Lowest_price':row["min"]})
    return result


# query1c()
# data = query1c()
# print(data)

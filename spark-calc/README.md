

### import CSV to C*

``` bash
# make raw sstable
sbt "runMain org.sws9f.sparkcalc.CaxSSTableWriterTranx"

# create cql table
cql> 
CREATE TABLE ks1.tranx (
  uid varchar, date date,
  c1 varchar, c2 varchar, c3 varchar, sid varchar,
  payment double,
  PRIMARY KEY ((c1, c2, c3, sid), date, uid)
);

cqlsh> SELECT * FROM ks1.tranx LIMIT 5;

# ssloader
sstableloader -d 127.0.0.1 ~/repos/data/sstable/ks1/tranx
```

### running in sbt console

``` scala
import org.sws9f.sparkcalc._
import com.datastax.spark.connector._

val sc = SparkApp.sc
val sqlContext = SparkApp.sqlContext
import sqlContext.implicits._

val rdd1 = sc.cassandraTable[Tranx]("ks1", "tranx").where("date < ?", "2014-03-01")

val rdd1 = sc.cassandraTable[Tranx]("ks1", "tranx").select("c1", "c2", "c3", "sid", "uid", "date", "payment").where("date < ?", "2014-03-01")
rdd1.cache
val df1 = rdd1.toDF()
df1.cache
df1.registerTempTable("df1")
val df2 = sqlContext.sql("SELECT * FROM df1")
df2.show

df2.registerTempTable("df2")
val df4 = sqlContext.sql("SELECT c1, sid, COUNT(uid) AS tranx FROM df2 t WHERE t.date < '2013-05-01' GROUP BY t.c1, t.sid")

sqlContext.sql("SELECT c1, sid, COUNT(uid) as trans, AVG(payment) as avf, STDDEV_POP(payment) as std FROM df1 t WHERE t.date < '2013-05-01' GROUP BY c1, sid").show

rdd1.partitions.size


df1.select('*).show
df1.where(df1("sid") === "1B01").show

:paste

val rdd2 = rdd1.groupBy(r => r.sid) 
// map each group - an Iterable[Row] - to a list and sort by the second column
.map(g => g._2.toList.sortBy(row => row.sid.toString))     
.collect()


import org.apache.spark.sql.hive.HiveContext

val sqlContext = new HiveContext(sc)
import sqlContext.implicits._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._



val overC1 = Window.partitionBy("c1").orderBy(desc("payment"))

val overC1 = Window.partitionBy("c1").orderBy($"payment".desc)
val rank = denseRank.over(overC1)
val ranked = df1.withColumn("payment", rank)


#----

import org.apache.spark.sql.expressions._

val df2 = sqlContext.sql("""
SELECT 
  c1, sid, uid, date, payment,
  LAG(date,1)  OVER (PARTITION BY uid, c1, c2, c3, sid ORDER BY date) AS lead_date,
  LEAD(date,1) OVER (PARTITION BY uid, c1, c2, c3, sid ORDER BY date) AS next_date
FROM df1
""")

df2.cache
df2.registerTempTable("df2")

val df3 = sqlContext.sql("""
SELECT 
  c1, sid, uid, date, payment, lead_date, next_date, 
  DATEDIFF(date, lead_date) AS diff
FROM df2
""")

df3.cache
df3.registerTempTable("src_trans")


val df_nes02u = sqlContext.sql("""
SELECT 
  c1, sid, uid, date, payment, lead_date, next_date, diff
FROM df2
GROUP BY c1, sid
""")

sqlContext.sql("""
SELECT 
  c1, sid, SUM(payment) AS price, 
  STDDEV_POP(diff) AS std_diff,
  AVG(diff) AS avg_diff
FROM src_trans
GROUP BY c1, sid
""").show


```




``` scala
// get config
sc.getConf.getOption("spark.app.name")
sc.getConf.toDebugString
sc.getConf.getAll
sc.setLogLevel("INFO")

// default partition num
sc.defaultParallelism
rdd1.repartition(3).count
rdd1.getStorageLevel

SparkContext.setCheckpointDir("/Users/larrysu/tmp-spark")

// c* special
rdd1.cassandraCount
spanByKey


CLASSPATH=`find /Users/larrysu/repos/scala-examples/spark-calc/lib_managed -name '*.jar' | xargs echo | tr ' ' ':'`; 

CLASSPATH=`find /Users/larrysu/repos/scala-examples/spark-calc/lib_managed -name '*.jar' | xargs echo | tr ' ' ','`; \
spark-submit \
  --class "org.sws9f.sparkcalc.SimpleApp" \
  --master "spark://larrymac.local:7077" \
  --jars "$CLASSPATH" \
  target/scala-2.10/sparkcalculator_2.10-1.0.jar 


```
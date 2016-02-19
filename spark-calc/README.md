

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

val sparkapp = new SparkApp
val sc = sparkapp.sc

val sqlContext = sparkapp.sqlContext
import sqlContext.implicits._

val rdd1 = sc.cassandraTable[Tranx]("ks1", "tranx").select("c1", "c2", "c3", "sid", "uid", "date", "payment").where("date < ?", "2014-03-01")
rdd1.cache
val df1 = rdd1.toDF()
df1.cache
df1.registerTempTable("df1")
val df2 = sqlContext.sql("SELECT * FROM df1")
df2.show

df2.registerTempTable("df2")
val df4 = sqlContext.sql("SELECT c1, sid, COUNT(uid) AS tranx FROM df2 t WHERE t.date < '2013-05-01' GROUP BY t.c1, t.sid")

sqlContext.sql("SELECT c1, COUNT(uid) as trans, AVG(payment) as avf, STDDEV_POP(payment) as std FROM df2 t WHERE t.date < '2013-05-01' GROUP BY c1").show

```
#### 前提：我将文件放到虚拟机的`/mnt/hgfs/LinuxShareFolder/data/`中 ####
```
//将文件读成RDD，并过滤掉第一行元信息
val rdd=sc.textFile("file:/mnt/hgfs/LinuxShareFolder/data/movie_metadata.csv")
          .filter(!_.startsWith("color,director_name"))
//将每一行按照“，”分割,由于有些字段数据缺失，导致数据不整齐，需要过滤
val movieRdd=rdd.map(_.split(",")).filter(_.lenght==28)
//获取国家数据
val countries=movieRdd.map(_(20))
//过滤掉字段为空的数据集，取distinct并显示
countries.filter(_.length>0).distinct.foreach(println)

//过滤出中国电影
val chinaMovie=movieRdd.filter(_(20)=="China")
//中国电影的数量
chianMovie.count

val bestMovie=movieRdd.map(x=>(x(12),x(23),x(11),x(1),x(20)))
                      .filter(_._5.equals("China"))
                      .map(x=>(x._1.toInt,(x._2,x._3,x._4)))
                      .sortByKey(false)
                      .take(3)
                      .foreach(println)

```
- 使用spark sql的DataFrame和Dataset API功能实现
```
class case Movie(movie_title:String,director_name:String,num_voted_users:Long,country:String,title_year:String)

val movieDF=movieRdd.map(x=>(x(12),x(23),x(11),x(1),x(20)))
                    .map(x=>Movie(x(0).toString,x(1).toString,x(2).toLong,x(3).toString,x(4).toString))
                    .toDF()
movieDF.createOrReplaceTempView("movie")
val countries=spark.sql("select distinct country from movie")
val chinaMoive=spark.sql("select count(*) from movie where country=China")
val bestMovie=spark.sql("select movie_title,director_name,title_year form movie order by num_voted_users desc limit 3")


还有一种直接读入csv格式的方式，读入的数据直接是dataset格式
```
val ds=spark.read.option("header","true").csv("file:/mnt/hgfs/LinuxShareFolder/data/movie_metadata.csv")
```



```

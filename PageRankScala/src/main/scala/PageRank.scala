import org.apache.spark.{SparkConf, SparkContext}

object  PageRank{

  //Methods: MAIN

  def main(args:Array[String]): Unit ={

    //init
    val conf = new SparkConf().setAppName("PageRank").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    //input
    val lines = sc.textFile("file://" + args(0))

    //input:RDD[String]--> RDD[(string,Array[String])]
    val links = lines.map(line=>{
      val parts = line.split("\\s")
      (parts(0), parts(1).split(";"))
    })

    //creat RDD[(string,float(init 1))]
    var ranks = links.mapValues(v => 1.0f)

    //11times do
    for(i <- 0 to 10){

      // RDD links join RDD ranks
      //define flatMap's method after link, value:[(Array[String],float)]--> RDD[String, float]
      val contrib = links.join(ranks).values.flatMap{
        namesRank =>
          namesRank._1.map(name => {
            val rank = name.split(":")
            (rank(0), rank(1).toFloat * namesRank._2)
          })
      }

      //reduce RDD by Key, value = sum(values)
      //sort ranks by value
      ranks = contrib.reduceByKey(_+_).sortBy(_._2,false)
    }

    //output
    ranks.saveAsTextFile("file://" + args(1))
  }
}

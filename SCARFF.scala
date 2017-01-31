import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._
import java.text.NumberFormat
import java.util.Date
import java.util.HashMap
import org.apache.kafka.clients.producer.{ProducerConfig, KafkaProducer, ProducerRecord}
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{SparseVector, DenseVector, Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.StreamingContext._
import scala.collection.mutable.ArrayBuffer
import weka.core.Instances
import sys.process._
import scala.io.Source


object SCARFF extends StreamingTools {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.FATAL)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    //Kafka info
    val Array(zkQuorum, group, topics, numThreads) = Array("localhost:2181", "1", "transactionschannel", "1")
    //Cassandra Info
    val host = "localhost"
    val ks = "ksfraud"
    //Cassandra tables
    val tableRaw = "rawtrx"
    val tableRank = "ranktrx"
    
    val sparkConf = new SparkConf()
        .set("spark.cassandra.connection.host",host)
        .set("spark.streaming.blockInterval","2000")
        .set("spark.cassandra.input.split.size_in_mb","128")
        
    @transient val ssc = new StreamingContext(sparkConf, Seconds(180))
    ssc.checkpoint("checkpoint")
    val sc = ssc.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val start: Long = System.currentTimeMillis
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    
    val attList = defineAttributes
    val generalDataset = new Instances("generalDataset",attList,1000000)
    generalDataset.setClassIndex(generalDataset.numAttributes-1)
    
    truncateTables(ks, tableRaw, tableRank, host) //truncate tables
    
    //dictionary to treat categorical features with a big number of cases
    val dictionary = Source.fromFile("/home/guest/SCARFFFiles/dictionary.csv").getLines.map(line =>line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1)).toArray

    val windows = 2// days for the aggregation 
    val numDaysBeforeTraining = 3 // number of days to wait before start training models
    val nDelayDay = 2   // number of delay days: usually from t to t-nDelayDay
    val nTrainDay = 2   // days for delayed model training: from t-nDelayDay to t-nDelayDay-nFeedbackDay
    val nFeedbackDay = 4  // number of days for the feedback usage: from t to t-nFeedbackDay
    val removeTrx = 5*24 //number of hours before data removal
    var removed = -1 //removed partition
    val numTxReported = 20 // number of transaction to alert and to train feedback models
    val nTree = 10  //number of trees per partition
    val ratio0 = 1 // ratio between fraud a genuine per tree
    var dayCounter=0
    var startTimestamp = 0L
    var model = new ForestCollection
    var feedback = new FeedbackCollection(nFeedbackDay, numDaysBeforeTraining)
    
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2).foreachRDD( rdd => {
      val batch = rdd.map{line =>
        line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1)  
      }
      val control = batch.isEmpty
      if(! control) {
        val runtime = Runtime.getRuntime
        val datetime = format.parse(batch.first.apply(6)).getTime //first batch timestamp
        if(startTimestamp == 0) startTimestamp = datetime
        val day = ((datetime-startTimestamp)/86400000L).toInt
        val hourCounter = ((datetime-startTimestamp)/3600000L).toInt
        //remove the transactions
        if (hourCounter>=removeTrx) {
            removeOldTrx(removed, hourCounter-removeTrx, ks, tableRaw, host)
            removed = hourCounter-removeTrx
        }
        //if the stream timestamp changes date then update the model
        if( day > dayCounter){
          dayCounter = day
          if( dayCounter > numDaysBeforeTraining) {
            model.update(startTimestamp, dayCounter, numDaysBeforeTraining, nTrainDay, nDelayDay, nFeedbackDay, feedback, nTree, ratio0, ks, tableRaw, tableRank, sqlContext, host)
          }
        }          
        runtime.gc
        //return aggregation+classification
        val out = aggregation(windows, datetime, startTimestamp, hourCounter, dayCounter, sqlContext, feedback, model, batch, dictionary, format, numTxReported, generalDataset, ks, tableRaw) 
              
        //save to cassandra
        out.saveToCassandra(ks,tableRaw,SomeColumns("cardid", "cat1", "cat2", "merchant", "merchantarea", "tx_amount", "tx_datetime", "tx_dhour", "tx_hour", "tx_dayw", "boo1", "boo2", "age", "nationality", "gender", "cat3", "cat4", "label",
          "w1_min_amount", "w1_max_amount", "w1_mmdif_amount", "w1_sum_amount", "w1_count_amount", "w1_avg_amount", "w1_countboo1false", "w1_min_merchantarea", "w1_max_merchantarea", "w1_mmdif_merchantarea", "w1_sum_merchantarea", "w1_count_merchantarea", "w1_avg_merchantarea",
           "last_merchantarea", "last_cat1", "last_cat2", "last_merchant", "zfraud_pr", "zfraud_pr_d", "zfraud_pr_f"))
        println("Receiving transactions from day: "+day)
      }else{
        println("No data streamed!!!")
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }
}

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.streaming._
import java.util.ArrayList
import java.util.Arrays
import java.util.Collection
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
import scala.util.Random
import weka.classifiers.trees.J48
import weka.classifiers.trees.RandomForest
import weka.core.Attribute
import weka.core.Instance
import weka.core.DenseInstance
import weka.core.Instances
import com.datastax.driver.core.Cluster

class FeedbackCollection(nFeedbackDay: Int, numDaysBeforeTraining:Int) extends Serializable with WekaTools{
  //initialize an Array with top risky transactions for one day
  var dailyTable = Array[(String, Long, Boolean, Double, Double, Double, Double, Instance)]()
  //initialize an Array to store a collection of dailyTable
  var periodTable = Array[Array[(String, Long, Boolean, Double, Double, Double, Double, Instance)]]()
  var forest = new RandomForest
  //new dataset
  val attList = defineAttributes
  val race = new Instances("race",attList,1000000)
  race.setClassIndex(race.numAttributes-1)
  var instances = new Instances("instances",attList,1000000)
  //var dayCounter = 0
  //add a transaction to dailyTable 
  def addTx(t: Array[(String, Long, Boolean, Double, Double, Double, Double, Instance)]){
    dailyTable = dailyTable ++ t
  }
  //remove a transaction to dailyTable 
  def removeTx(){
    dailyTable = dailyTable.tail
  }  
  //add dailyTable to periodTable and store top risky transactions to DB
  def addDay(dayCounter: Int, tableRank: String, host: String, ks: String){
    val dayPrev = dayCounter - 1
    periodTable = periodTable ++ Array(dailyTable)
    val cluster = Cluster.builder.addContactPoint(host).build
    val session = cluster.connect(ks)
    //save on cassandra
    for (a <- 0 to (dailyTable.length - 1)){
      val b = dailyTable(a)
      session.execute("INSERT INTO "+ tableRank +
          " (tx_rank, cardid, day, tx_datetime, label, tx_amount, zfraud_pr, zfraud_pr_d, zfraud_pr_f) VALUES (" +
          (a+1) + ",'" + 
          b._1 + "'," + 
          dayPrev + "," +
          b._2 + "," + 
          b._3 + "," + 
          b._4 + "," + 
          b._5 + "," + 
          b._6 + "," + 
          b._7 +
          ")")
    }
  }
  //remove day from periodTable
  def removeDay(){
    periodTable = periodTable.tail
  }
  //empty daily table
  def emptyDailyTable(){
    dailyTable = Array[(String, Long, Boolean, Double, Double, Double, Double, Instance)]()
  }
  //sort and keep top risky transactions
  def prune(numTxReported: Int){
      dailyTable = dailyTable.sortWith(_._5 > _._5)
      dailyTable = dailyTable.take(numTxReported) 
  }
    
  // train a random forest
  def train() ={
    var sumTable = Array[(String, Long, Boolean, Double, Double, Double, Double, Instance)]()
    //merge arrays
    for( a <- 0 to (periodTable.length-1)){
      sumTable = sumTable ++ periodTable(a)
    }
    instances = new Instances("instances",attList,1000000)
    sumTable.map(i => {
      instances.add(i._8)
    })
    instances.setClassIndex(instances.numAttributes-1)
    forest = new RandomForest
    forest.setNumTrees(100)
    forest.buildClassifier(instances) 
  }    
  //classify an instance
  def classify(testData: Instance, dayCounter: Int): Double ={
    testData.setDataset(race)
    try{forest.distributionForInstance(testData).apply(1) }catch {case _:Throwable => -1}
  }
}

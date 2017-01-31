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
import weka.core.Attribute
import weka.core.Instance
import weka.core.DenseInstance
import weka.core.Instances
import org.apache.spark.sql._

class ForestCollection extends Serializable{
  var value = Array[TreesByPartition]()
  ////var day = 0 //day counter
  def add(arr: Array[TreesByPartition]){
    value = value ++ arr
  }
  def remove(){
    value = value.tail
  }
  //ensemble prediction
  def classify(testData: Instance, feed: FeedbackCollection, dayCounter: Int): (Double, Double, Double) ={ 
    //delayed classification
    val pred = value.map( f =>  f.classify(testData)) 
    //ensemble of delayed classification
    val delPred = pred.sum /pred.length.toDouble     
    //feedback classification are provided from nFeedbackDay days
    val feedPred = feed.classify(testData, dayCounter)       
    //if we have a prediction from feedback model we compute the average, otherwise we keep the delayed prediction as main
    val out = if(feedPred >= 0){                      
      val totPred = (0.5 * delPred) + (0.5 * feedPred)
      (totPred, delPred, feedPred)
    }else (delPred, delPred, feedPred)
    out
  }
  def update(mTimestamp: Long, dayCounter: Int, numDaysBeforeTraining:Int, nTrainDay: Int, nDelayDay: Int, nFeedbackDay: Int, feed: FeedbackCollection, nTree: Int, ratio0: Int, ks: String, tableRaw: String, tableRank: String, sqlc: SQLContext, host: String) ={ //ensemble prediction
    ////day += 1
    //in the first nTrainDay create model and push them in the Array[TreesByPartition]
    if(dayCounter < (numDaysBeforeTraining + nTrainDay)){
      //val date = new Date(mTimestamp+(dayCounter*86400000L))
      val newModel = new TreesByPartition(dayCounter-1, nTree: Int, ratio0, ks, tableRaw, sqlc, true)
      add(Array(newModel))
    }
    //when we have (nTrainDay + nDelayDay) days, create a model with data from nDelayDay days ago, push it in Array[TreesByPartition] and remove the oldest model
    if(dayCounter > (numDaysBeforeTraining + nDelayDay + nTrainDay)){
      val newModel = new TreesByPartition(dayCounter - nDelayDay , nTree: Int, ratio0, ks, tableRaw, sqlc, false)
      add(Array(newModel))
      remove
    }
    //we have feedbacks form the second day, since there are not alert at the first day
    if(dayCounter > (1 + numDaysBeforeTraining)){
      //add collected daily alerts
      feed.addDay(dayCounter,tableRank,host,ks)
      //be ready to collect new day transaction probabilities
      feed.emptyDailyTable
    }
    //we start to remove alert from the stack after nFeedbackDay days 
    if(dayCounter > (1 + numDaysBeforeTraining + nFeedbackDay)) feed.removeDay
    //we start to train model from nFeedbackDay days
    if(dayCounter > (numDaysBeforeTraining + nFeedbackDay)) feed.train
  }
}






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
import weka.core.Attribute
import weka.core.DenseInstance
import weka.core.Instance
import weka.core.Instances
import org.apache.spark.sql._

trait WekaTools extends Serializable{
 // define attribute of a dataset
 def defineAttributes():  ArrayList[Attribute] = {
  val age = new Attribute("age")
  val c3 = new ArrayList[String](7)
  for( a <- 0 to 6){
    c3.add(a,a.toString)
  }
  val cat3 = new Attribute("cat3",c3)
  val c4 = new ArrayList[String](7)
  for( a <- 0 to 6){
    c4.add(a,a.toString)
  }
  val cat4 = new Attribute("cat4",c4)
  val gen = new ArrayList[String](3)
  for( a <- 0 to 2){
    gen.add(a,a.toString)
  }
  val gender = new Attribute("gender",gen)
  val nat = new ArrayList[String](20)
  for( a <- 0 to 19){
    nat.add(a,a.toString)
  }
  val nationality = new Attribute("nationality",nat)
  val last_merchantarea = new Attribute("last_merchantarea")
  val last_merchant = new Attribute("last_merchant")
  val last_cat2 = new Attribute("last_cat2")
  val last_cat1 = new Attribute("last_cat1")
  val merchantarea = new Attribute("merchantarea")
  val merchant = new Attribute("merchant")
  val cat2 = new Attribute("cat2")
  val cat1 = new Attribute("cat1")
  val sec = new ArrayList[String](2)
  sec.add(0,"0")
  sec.add(1,"1")
  val boo2 = new Attribute("boo2",sec)
  val acc = new ArrayList[String](2)
  acc.add(0,"0")
  acc.add(1,"1")
  val boo1 = new Attribute("boo1",acc)
  val tx_amount  = new Attribute("tx_amount ")
  val tx_dayw = new Attribute("tx_dayw")
  val tx_hour = new Attribute("tx_hour")
  val w1_avg_amount = new Attribute("w1_avg_amount")
  val w1_avg_merchantarea = new Attribute("w1_avg_merchantarea")
  val w1_count_amount = new Attribute("w1_count_amount")
  val w1_count_merchantarea = new Attribute("w1_count_merchantarea")
  val w1_max_amount = new Attribute("w1_max_amount")
  val w1_max_merchantarea = new Attribute("w1_max_merchantarea")
  val w1_min_amount = new Attribute("w1_min_amount")
  val w1_min_merchantarea = new Attribute("w1_min_merchantarea")
  val w1_mmdif_amount = new Attribute("w1_mmdif_amount")
  val w1_mmdif_merchantarea = new Attribute("w1_mmdif_merchantarea")
  val w1_countboo1false = new Attribute("w1_countboo1false")
  val w1_sum_amount = new Attribute("w1_sum_amount")
  val w1_sum_merchantarea = new Attribute("w1_sum_merchantarea")
  val nom = new ArrayList[String](2) 
  nom.add(0,"genuine")
  nom.add(1,"fraud")
  val label = new Attribute("label",nom)
  new ArrayList(Arrays.asList(age, cat3, cat4, gender, nationality, last_merchantarea, last_merchant, last_cat2, last_cat1, merchantarea, merchant, cat2, cat1, boo2, boo1, tx_amount, tx_dayw, tx_hour, 
    w1_avg_amount, w1_avg_merchantarea, w1_count_amount, w1_count_merchantarea, w1_max_amount, w1_max_merchantarea, w1_min_amount, w1_min_merchantarea, w1_mmdif_amount, w1_mmdif_merchantarea, w1_countboo1false, w1_sum_amount, w1_sum_merchantarea, label))
  
 }
 implicit class SuperRow(val cr: Row) {
   def toInstance(attList: ArrayList[Attribute]): Instance = {
      // Create empty instance with three attribute values 
      val inst = new DenseInstance(45)
      // Set instance's values for the attributes 

      //inst.setValue(attList.get(0), cr.getLong(1))
      inst.setValue(attList.get(0), cr.getInt(3))
      inst.setValue(attList.get(1), cr.getInt(8))
      inst.setValue(attList.get(2), cr.getInt(9))
      inst.setValue(attList.get(3), cr.getInt(10))
      inst.setValue(attList.get(4), cr.getInt(18))
      inst.setValue(attList.get(5), cr.getFloat(15))
      inst.setValue(attList.get(6), cr.getFloat(14))
      inst.setValue(attList.get(7), cr.getFloat(13))
      inst.setValue(attList.get(8), cr.getFloat(12))
      inst.setValue(attList.get(9), cr.getFloat(17))
      inst.setValue(attList.get(10), cr.getFloat(16))
      inst.setValue(attList.get(11), cr.getFloat(7))
      inst.setValue(attList.get(12), cr.getFloat(6))
      inst.setValue(attList.get(13), if (cr.getBoolean(5)) 1 else 0)
      inst.setValue(attList.get(14), if (cr.getBoolean(4)) 1 else 0)
      inst.setValue(attList.get(15), cr.getFloat(19))
      inst.setValue(attList.get(16), cr.getInt(20))
      inst.setValue(attList.get(17), cr.getInt(21))
      inst.setValue(attList.get(18), cr.getFloat(22))
      inst.setValue(attList.get(19), cr.getFloat(23))
      inst.setValue(attList.get(20), cr.getFloat(24).intValue)
      inst.setValue(attList.get(21), cr.getFloat(25).intValue)
      inst.setValue(attList.get(22), cr.getFloat(27))
      inst.setValue(attList.get(23), cr.getFloat(28))
      inst.setValue(attList.get(24), cr.getFloat(29))
      inst.setValue(attList.get(25), cr.getFloat(30))
      inst.setValue(attList.get(26), cr.getFloat(31))
      inst.setValue(attList.get(27), cr.getFloat(32))
      inst.setValue(attList.get(28), cr.getInt(26))
      inst.setValue(attList.get(29), cr.getFloat(33))
      inst.setValue(attList.get(30), cr.getFloat(34))
      inst.setValue(attList.get(31), if (cr.getBoolean(11)) 1 else 0)
      inst    
   }
 }
}

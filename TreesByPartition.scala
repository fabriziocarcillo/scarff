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
import weka.classifiers.trees.RandomTree
import weka.core.Attribute
import weka.core.Instance
import weka.core.DenseInstance
import weka.core.Instances
import org.apache.spark.sql._


class TreesByPartition(day: Int, nTree: Int, ratio0: Int, ks: String, tableRaw: String, sqlc: SQLContext, bigger: Boolean) extends WekaTools{
 
   //retrieve card info from DB
    val subset =  
      if(bigger) sqlc.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> tableRaw, "keyspace" -> ks )).load().filter("tx_dhour > '" + day*24 + "'").rdd
      else sqlc.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> tableRaw, "keyspace" -> ks )).load().filter("tx_dhour < '" + day*24 + "'").rdd
    //filter fraud and collect them
    val rddBC = subset.filter(i => i.getBoolean(11) == true).collect
    val num1 = rddBC.length
    //filter genuine
    val rdd0 = subset.filter(i => i.getBoolean(11) == false)
    //define a dataset that will be filled with training trx
    val attList = defineAttributes 
    val race = new Instances("race",attList,1000000)
    race.setClassIndex(race.numAttributes-1)
    //for each partition of the genuine rdd
    val trees = rdd0.mapPartitions (p => {  
      val pp = p.toList
      val numPart0 = pp.length
      if(numPart0 > 10){
        val groups0 =  math.max(2, try{numPart0 / (ratio0 * num1)}catch {case _:Throwable =>0}) 
        //assign a random integer to any trx in the partition
        val ran = List.fill(pp.length)(Random.nextInt.abs % groups0) 
        val pr = pp.zip(ran)
        val r1 = rddBC
        val attList = defineAttributes
        //initialize an array to store the trees
        var decTrees: Array[RandomTree] = Array() 
        //in this partition create nTree Trees
        for( a <- 0 to math.min(9,groups0 - 1)){ 
          //subset the genuine sample
          val set0 = pr.filter(i => i._2 == a).map(i => i._1)  
          //merge fraud to genuine subsets
          val set = set0 ++ r1.toList 
          val instances = new Instances("instances",attList,1000000)
          set.map(i=> {
            val j = i.toInstance(attList)
            instances.add(j)
          })
          instances.setClassIndex(instances.numAttributes-1)
          //new instance of tree
          val tree = new RandomTree         
          tree.setSeed(a+1)
          //store the model
          decTrees = try{
            tree.buildClassifier(instances)
            decTrees ++ Array(tree)
          }catch{case _:Throwable => decTrees}
         }
       decTrees.toIterator}else Iterator()
  }).collect
  def classify(testData: Instance): Double={  
    val ro = trees.map(p => {
      testData.setDataset(race)
      // classify the new instance for any Tree in the collection previously built
      val pred = try{p.distributionForInstance(testData).apply(1)//classifyInstance(testData)//   // class or probability
      }catch{case _:Throwable => 0.002809.toDouble}
      //p.distributionForInstance(testData).apply(1)
      pred
    }).toList
    //average the predictions
    ro.sum/ro.length.toDouble 
  }  
}

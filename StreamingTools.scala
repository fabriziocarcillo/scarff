import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd._
import com.datastax.spark.connector.streaming._
import java.util.ArrayList
import java.util.Date
import java.util.HashMap
import org.apache.kafka.clients.producer.{ProducerConfig, KafkaProducer, ProducerRecord}
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{SparseVector, DenseVector, Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.sql._
import org.apache.spark.streaming._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.StreamingContext._
import scala.collection.mutable.ArrayBuffer
import weka.core.Attribute
import weka.core.DenseInstance
import weka.core.Instance
import weka.core.Instances
import com.datastax.driver.core.Cluster
import java.util.Calendar
import java.util.regex.Matcher
import scala.collection.JavaConversions._

trait StreamingTools  extends Serializable with WekaTools{
  //object to collect values
  case class RawTableAgg(cardid: String, cat1: Float, cat2: Float, merchant: Float, merchantarea: Float, tx_amount: Float, tx_datetime: Long, tx_dhour: Int, tx_hour: Int, tx_dayw: Int, boo1: Boolean, boo2: Boolean, age: Int, nationality: Int, gender: Int, cat3: Int, cat4: Int, label: Boolean,
     w1_min_amount: Float, w1_max_amount: Float, w1_mmdif_amount: Float, w1_sum_amount: Float, w1_count_amount: Float, w1_avg_amount: Float, w1_countboo1false: Int, w1_min_merchantarea: Float, w1_max_merchantarea: Float, w1_mmdif_merchantarea: Float, w1_sum_merchantarea: Float, w1_count_merchantarea: Float, w1_avg_merchantarea: Float,
    last_merchantarea: Float, last_cat1: Float, last_cat2: Float, last_merchant: Float){
    //add probabilities
    def toRawTablePr(zfraud_pr: Float, zfraud_pr_d: Float, zfraud_pr_f: Float):RawTablePr = {
       new RawTablePr(cardid: String, cat1: Float, cat2: Float, merchant: Float, merchantarea: Float, tx_amount: Float, tx_datetime: Long, tx_dhour: Int, tx_hour: Int, tx_dayw: Int, boo1: Boolean, boo2: Boolean, age: Int, nationality: Int, gender: Int, cat3: Int, cat4: Int, label: Boolean,
       w1_min_amount: Float, w1_max_amount: Float, w1_mmdif_amount: Float, w1_sum_amount: Float, w1_count_amount: Float, w1_avg_amount: Float, w1_countboo1false: Int, w1_min_merchantarea: Float, w1_max_merchantarea: Float, w1_mmdif_merchantarea: Float, w1_sum_merchantarea: Float, w1_count_merchantarea: Float, w1_avg_merchantarea: Float,
       last_merchantarea: Float, last_cat1: Float, last_cat2: Float, last_merchant: Float, zfraud_pr: Float, zfraud_pr_d: Float, zfraud_pr_f: Float)
    }
    def toInstance(classRequired: Boolean): Instance = {
      // Create empty instance with three attribute values 
      val inst = new DenseInstance( if (classRequired) 45 else 44)
      //set values
      //inst.setValue(0, tx_datetime)
      inst.setValue(0, age)
      inst.setValue(1, cat3)
      inst.setValue(2, cat4)
      inst.setValue(3, gender)
      inst.setValue(4, nationality)
      inst.setValue(5, last_merchantarea)
      inst.setValue(6, last_merchant)
      inst.setValue(7, last_cat2)
      inst.setValue(8, last_cat1)
      inst.setValue(9, merchantarea)
      inst.setValue(10, merchant)
      inst.setValue(11, cat2)
      inst.setValue(12, cat1)
      inst.setValue(13, if (boo2) 1 else 0)
      inst.setValue(14, if (boo1) 1 else 0)
      inst.setValue(15, tx_amount)
      inst.setValue(16, tx_dayw)
      inst.setValue(17, tx_hour)
      inst.setValue(18, w1_avg_amount)
      inst.setValue(19, w1_avg_merchantarea)
      inst.setValue(20, w1_count_amount)
      inst.setValue(21, w1_count_merchantarea)
      inst.setValue(22, w1_max_amount)
      inst.setValue(23, w1_max_merchantarea)
      inst.setValue(24, w1_min_amount)
      inst.setValue(25, w1_min_merchantarea)
      inst.setValue(26, w1_mmdif_amount)
      inst.setValue(27, w1_mmdif_merchantarea)
      inst.setValue(28, w1_countboo1false)
      inst.setValue(29, w1_sum_amount)
      inst.setValue(30, w1_sum_merchantarea)
      if(classRequired) inst.setValue(31, if (label) 1 else 0)
      inst    
    }
  }
  //object to collect values + probability
  case class RawTablePr(cardid: String, cat1: Float, cat2: Float, merchant: Float, merchantarea: Float, tx_amount: Float, tx_datetime: Long, tx_dhour: Int, tx_hour: Int, tx_dayw: Int, boo1: Boolean, boo2: Boolean, age: Int, nationality: Int, gender: Int, cat3: Int, cat4: Int, label: Boolean,
      w1_min_amount: Float, w1_max_amount: Float, w1_mmdif_amount: Float, w1_sum_amount: Float, w1_count_amount: Float, w1_avg_amount: Float, w1_countboo1false: Int, w1_min_merchantarea: Float, w1_max_merchantarea: Float, w1_mmdif_merchantarea: Float, w1_sum_merchantarea: Float, w1_count_merchantarea: Float, w1_avg_merchantarea: Float,
      last_merchantarea: Float, last_cat1: Float, last_cat2: Float, last_merchant: Float, zfraud_pr: Float, zfraud_pr_d: Float, zfraud_pr_f: Float)
  //aggregate and classify
  def aggregation(windows: Int, datetime: Long, startTimestamp: Long, dayCounter: Int, hourCounter: Int, sqlc: SQLContext, feed: FeedbackCollection, model: ForestCollection, batch: RDD[Array[String]], dictionary: Array[Array[String]], format:  java.text.SimpleDateFormat, numTxReported: Int, generalDataset: Instances, ks: String, tableRaw: String): 
   RDD[RawTablePr] = {
    val cardIDRDD = batch.map(i => (i.apply(0).replace("\"", "").trim,(0.toFloat,true,0.toFloat,0.toFloat,0.toFloat,0.toInt,0.toFloat,0.toFloat,0.toFloat, false))).distinct  //RDD of unique cardid
    val cardID = cardIDRDD.map(i=> i._1).collect  // Array of unique cardid
    val prioriRisk = 0.002809.toFloat
    val in = batch.map(i => {  //create an RDD of (cardid, Lis of values)
     if(i.length == 15){
      val cardid = try{i.apply(0).replace("\"", "").trim}catch{case _:Throwable => "aaa"}  //String
      val cat1 = dictionaryRisk(dictionary, "cat1", i.apply(1).replace("\"", ""))  //Float factor   
      val cat2 = dictionaryRisk(dictionary, "cat2", i.apply(2).replace("\"", ""))  //Float factor
      val merchant = dictionaryRisk(dictionary, "merchant", i.apply(3).replace("\"", ""))  //Float factor
      val merchantarea = dictionaryRisk(dictionary, "TERM_COUNTRY", i.apply(4).replace("\"", ""))    //Float factor
      val tx_amount = checkFloat(i.apply(5)) //Float
      val dat = format.parse(i.apply(6).replace("\"", ""))
      val cal = Calendar.getInstance
      cal.setTime(dat)
      val tx_datetime = cal.getTimeInMillis  //Long
      val tx_dhour = ((tx_datetime - startTimestamp)/3600000).toInt //Long 
      val tx_hour = cal.get(Calendar.HOUR_OF_DAY) //Int  
      val tx_dayw = cal.get(Calendar.DAY_OF_WEEK) //Int  
      val boo1 = try{i.apply(7).replace("\"", "").toBoolean}catch{case _:Throwable => true}  //Boolean
      val boo2 = try{i.apply(8).replace("\"", "").toBoolean}catch{case _:Throwable => true} //Boolean  
      val age = checkInt(i.apply(9).replace("\"", ""))  //Int            

      val nationality = dictionaryID(dictionary, "nationality", i.apply(10).replace("\"", ""))    //Int factor         
      val gender = dictionaryID(dictionary, "GENDER", i.apply(11).replace("\"", ""))    //Int factor                
      val cat3 = dictionaryID(dictionary, "cat3", i.apply(12).replace("\"", ""))  //Int factor               
      val cat4 = dictionaryID(dictionary, "cat4", i.apply(13).replace("\"", ""))    //Int factor             
      val label = try{i.apply(14).toBoolean}catch{case _:Throwable => false} //Boolean
      (cardid, List(cat1, cat2, merchant, merchantarea, tx_amount, tx_datetime, tx_dhour, tx_hour, tx_dayw, boo1, boo2, age, nationality, gender, cat3, cat4, label))
     }else
      ("NA", List(prioriRisk,prioriRisk, prioriRisk, prioriRisk, 88.toFloat, 0L, 0, 0, 0==0, 0==0, 42, 0, 0, 0, 0, 0==1))
    })
    //create aggregated features
    val aggrList = aggregate(datetime, sqlc, in, windows, cardIDRDD, cardID, ks, tableRaw, hourCounter) 
    //transform in an object RawTableAgg
    val out = aggrList.map{i =>
      RawTableAgg(i._1, i._2(0).asInstanceOf[Float], i._2(1).asInstanceOf[Float], i._2(2).asInstanceOf[Float], i._2(3).asInstanceOf[Float], i._2(4).asInstanceOf[Float], i._2(5).asInstanceOf[Long], i._2(6).asInstanceOf[Int], i._2(7).asInstanceOf[Int], i._2(8).asInstanceOf[Int], i._2(9).asInstanceOf[Boolean], i._2(10).asInstanceOf[Boolean], i._2(11).asInstanceOf[Int], i._2(12).asInstanceOf[Int], i._2(13).asInstanceOf[Int], i._2(14).asInstanceOf[Int], i._2(15).asInstanceOf[Int], 
          i._2(16).asInstanceOf[Boolean], i._2(17).asInstanceOf[Float], i._2(18).asInstanceOf[Float], i._2(19).asInstanceOf[Float], i._2(20).asInstanceOf[Float], i._2(21).asInstanceOf[Float], i._2(22).asInstanceOf[Float], i._2(23).asInstanceOf[Int], i._2(24).asInstanceOf[Float], i._2(25).asInstanceOf[Float], i._2(26).asInstanceOf[Float], i._2(27).asInstanceOf[Float], i._2(28).asInstanceOf[Float],
          i._2(29).asInstanceOf[Float], i._2(30).asInstanceOf[Float], i._2(31).asInstanceOf[Float], i._2(32).asInstanceOf[Float], i._2(33).asInstanceOf[Float])
    }
    //classify and create a new object RawTablePr
    val out2 = 
      if (model.value.isEmpty) out.map(i => (i.toRawTablePr(0.1.toFloat,0.1.toFloat,0.1.toFloat), (-10.toDouble,-10.toDouble,-10.toDouble), new DenseInstance(44)))
      else {        
        out.map(i => {
          val inst = i.toInstance(false)
          generalDataset.add(inst)
          val classif = model.classify(inst, feed, dayCounter)
          val inst2 = i.toInstance(true)
          generalDataset.add(inst2)
          val out3 = i.toRawTablePr(classif._1.toFloat,classif._2.toFloat,classif._3.toFloat)
          (out3, classif, inst2)
        })
      }
    
    val inArray = out2.top(100)(Ordering[(Double)].on(x => (x._1.zfraud_pr))) //.filter(i => i._1.zfraud_pr > 0.8).collect//  val inArray = out2.filter(i => i._1.zfraud_pr > 0.7).collect
    inArray.map { x => 
       feed.addTx(Array((x._1.cardid, x._1.tx_datetime, x._1.label, x._1.tx_amount, x._2._1, x._2._2, x._2._3, x._3)))
    }
    //keep top risky transactions to ask feedback
    feed.prune(numTxReported)
    //output
    (out2.map(i => i._1))
  }
  //replace category with Int ID
  def dictionaryID(dictionary: Array[Array[String]], variable: String, level: String): Int = {
    val out = dictionary.filter(j => (j.apply(0)== variable && j.apply(2)== level))
    val out1 = if (out.size == 0) 0 else out.apply(0).apply(1).toInt
    out1
  }
  //replace category with risk of category
  def dictionaryRisk(dictionary: Array[Array[String]], variable: String, level: String): Float = {
    val out = dictionary.filter(j => (j.apply(0)== variable && j.apply(2) == level))
    val out1 = if (out.size == 0) 0.002809.toFloat else out.apply(0).apply(3).toFloat
    out1
  }
  def checkFloat(term: String): Float = try{term.toFloat}catch{case _:Throwable => 88}
  //if age is not Int then replace avg age
  def checkInt(term: String): Int = try{Integer.parseInt(term)}catch{case _:Throwable => 42}
  //statistics from multiple features history 
  def multipleAggr(subsetP: RDD[(String, (Float,Boolean,Float))], cardIDRDD: RDD[(String, (Float, Boolean, Float, Float, Float, Int, Float, Float, Float, Boolean))] ):  RDD[(String, List[AnyVal])] = {   
    val stat = subsetP
    .mapValues(x => (x._1,x._2,x._3, 1.toFloat, 1.toFloat, if(x._2) 0.toInt else 1.toInt, 1.toFloat, 1.toFloat, 1.toFloat,true))
    .union(cardIDRDD)
    .reduceByKey((x, y) => 
                             //get smallest, biggest, sum and count of values by key
        if(x._10 == y._10) (math.min(x._1, y._1), true, math.max(x._1, y._1),x._1 + y._1, x._4 + y._4,   x._6 + y._6,  math.min(x._3, y._3),math.max(x._3, y._3),x._3 + y._3, true)
        else (math.max(x._1, y._1), true, math.max(x._1, y._1),x._1 + y._1, x._4 + y._4, x._6 + y._6, math.max(x._3, y._3),math.max(x._3, y._3),x._3 + y._3, true)
    )
    
    // get (max - min) and avg
    val stat2 = stat.map(x=>(x._1,(x._2._1, x._2._3,x._2._3 - x._2._1, x._2._4,x._2._5,x._2._4/math.max(x._2._5, 1),x._2._6,  x._2._7, x._2._8,x._2._8 - x._2._7, x._2._9,x._2._5,x._2._9/math.max(x._2._5, 1) ))) 
    //zeros for new cards
    val stat3 = cardIDRDD.leftOuterJoin(stat2).map(i => {
      val op = i._2._2
      val op2 = op match {
              case Some(op)=> op
              case None => (0.toFloat,0.toFloat,0.toFloat,0.toFloat,0.toFloat,0.toFloat,0.toInt,0.toFloat,0.toFloat,0.toFloat,0.toFloat,0.toFloat,0.toFloat)
      }
    (i._1, List[AnyVal](op2._1,op2._2,op2._3,op2._4,op2._5,op2._6,op2._7,op2._8,op2._9,op2._10,op2._11,op2._12,op2._13))
    })
    //output
    stat3
    }
  def aggregate(datetime: Long, sqlc: SQLContext, in: RDD[(String, List[AnyVal])], windows: Int, cardIDRDD: RDD[(String, (Float, Boolean, Float, Float, Float, Int, Float, Float, Float, Boolean))] , cardID: Array[String], ks: String, tableRaw: String, hourCounter: Int): RDD[(String, List[AnyVal])] = {
    val now: Long = datetime
    var aggregated = in // to this RDD will be appended all the aggregated features
    //retrieve card info from DB
    val subset = sqlc.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> tableRaw, "keyspace" -> ks )).load()
    .select("cardid", "tx_dhour", "tx_datetime","tx_amount","boo1","merchantarea","cat1","cat2","merchant")
    .rdd.cache
    val subsetWP1 = subset.map{case Row(cardid: String, tx_dhour: Int, tx_datetime: Date, tx_amount: Float, boo1: Boolean, merchantarea: Float, cat1: Float,cat2: Float,merchant: Float) =>(cardid,(tx_amount,boo1,merchantarea))}

    //aggregate info
    var amountAgg= multipleAggr(subsetWP1, cardIDRDD)
    //join aggregated to input transactions
    aggregated = aggregated.join(amountAgg).map(i => (i._1, i._2._1 ++ i._2._2))
    // for multiple windows size 
 
    //Info from the last transaction in DB
    //retrieve card info from DB
    val subsetWP2 = subset.map{case Row(cardid: String, tx_dhour: Int, tx_datetime: Date, tx_amount: Float, boo1: Boolean, merchantarea: Float, cat1: Float,cat2: Float,merchant: Float) =>(cardid,(tx_datetime.getTime,tx_amount,boo1,merchantarea,cat1,cat2,merchant))}
    //last transaction RDD
    val lastTrx = subsetWP2.mapValues(i => (i._1, i)).reduceByKey((x,y)=> if(x._1>y._1) (x._1,x._2) else (y._1,y._2)).map(i=> (i._1, i._2._2))   
    //last transaction details
    val lastTrxInf = lastTrx.map( i => (i._1.trim,(i._2._4,i._2._5,i._2._6,i._2._7)))
    //last transaction for new cards 0,0,0,0
    val lastTrxInf2 = cardIDRDD.leftOuterJoin(lastTrxInf).map(i => {
      val op = i._2._2 
      val op2 = op match {
              case Some(op)=> op
              case None => (0.toFloat,0.toFloat,0.toFloat,0.toFloat)
      }
    (i._1, List(op2._1,op2._2,op2._3,op2._4)) //leftouterjoin with 0
    })

    //join info
    aggregated = aggregated.join(lastTrxInf2).map(i => (i._1, i._2._1 ++ i._2._2))
    //output
    aggregated
  }
  // DROP and CREATE tables
  def truncateTables(ks: String, tableRaw: String, tableRank: String, host: String){
    val cluster = Cluster.builder.addContactPoint(host).build
    val session = cluster.connect(ks)
    session.execute("TRUNCATE " + ks + "." + tableRaw + ";")
    session.execute("TRUNCATE " + ks + "." + tableRank + ";")
  }
  // DELETE trx in partition between  removed+1 and toRemove
  def removeOldTrx(removed: Int, toRemove: Int, ks: String, tableRaw: String, host: String){
    val cluster = Cluster.builder.addContactPoint(host).build
    val session = cluster.connect(ks)
    for ( a <- (removed+1) to toRemove) session.execute("DELETE FROM " + ks + "." + tableRaw + " WHERE tx_dhour = " + a.toString + ";")
  }
}

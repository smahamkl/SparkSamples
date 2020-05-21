package sorting

import broadcast.GuavaTableLoader
import com.google.common.collect.Table
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import utils.MLIDUtils._
import utils.SparkJob
import org.apache.spark.sql._
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.io.Text
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._


object MLIDSecSort extends SparkJob {

  type RefTable = Table[String, String, String]


  def runSecondarySortExample(args: Array[String]): Unit = {

    //    val pspMlidData = "/Users/smahamka/Documents/hdfs_dir_l/SPARK_MLID_SELECTED/psp_assn/*"
    //    val mlidDtlData = "/Users/smahamka/Documents/hdfs_dir_l/SPARK_MLID_SELECTED/dqm_mrch_hrchy_dtl_short"
    //    val auCSData = "/Users/smahamka/Documents/hdfs_dir_l/SPARK_MLID_SELECTED/au_cs_lnkg_short"
    //    val outputPath = "/Users/smahamka/Documents/hdfs_dir_l/SPARK_MLID_SELECTED/OUT/"
    //    val totPartitions = 5
    //    val orphGMR = "/Users/smahamka/Documents/hdfs_dir_l/SPARK_MLID_SELECTED/orph_short"
    //    val master = "local[4]"

    var master = args(0)
    val pspMlidData = args(1)
    val mlidDtlData = args(2)
    val auCSData = args(3)
    val outputPath = args(4)
    val orphGMR = args(5)
    val totPartitions = args(6).toInt

    println("PSP MLID Data dir: " + pspMlidData)
    println("Merchant hrchy dtl dir: " + mlidDtlData)
    println("AU CS Data dir: " + auCSData)
    println("Output Data dir: " + outputPath)

    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    val conf = new SparkConf().setAppName("MLID FV Selected").setMaster(master)
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)


    conf.set("spark.speculation", "false")
    conf.set("spark.sql.parquet.enableVectorizedReader", "false")

    var aucsRDD: RDD[Row] = sc.emptyRDD

    if (!DEBUG_MODE) {
      val hiveObj = new HiveContext(sc)
      aucsRDD = hiveObj.sql(AUCS_SQLSTR).rdd
      //remove DIDs getting stamped on Auth side
      aucsRDD = aucsRDD.filter(row => {
        //temporarily exclude all DID(6-series) on Auth side
        if ((row(0).toString.substring(0, 1) == "6") && row(0).toString.length == 11)
          false
        else
          true
      })

      //regular auth psp gmrids are pulled from gmrid core tables(tmatc_gmr_id_psp) and
      //so exclude here. so including only sponsored merchant based PSP auth GMRIDs
      aucsRDD = aucsRDD.filter(row => {
        try {
          if ((row(2).toString.trim == "Y") && row(3).toString.substring(0, 1) != "8")
            false
          else
            true
        } catch {
          case e: Exception => true
        }
      })

      //auSSOTRDD = hiveObj.sql(AU_SSOT_SQLSTR).rdd
    } else {
      aucsRDD = sqlContext.read.format("com.databricks.spark.csv")
        .option("inferSchema", "true") // Automatically infer data types
        .option("delimiter", "\u0001")
        .option("nullValue", "\\N")
        .load(auCSData)
        .rdd

      aucsRDD = aucsRDD.filter(row => {
        if ((row(2).toString.substring(0, 1) == "6") && row(2).toString.length == 11)
          false
        else
          true
      })

      aucsRDD.take(10).foreach(println)
      //      auSSOTRDD = sqlContext.read.format("com.databricks.spark.csv")
      //        .option("inferSchema", "true") // Automatically infer data types
      //        .option("delimiter", "\u0001")
      //        .option("nullValue", "\\N")
      //        .load(auSSOT)
      //        .rdd
      //
      //      auSSOTRDD = auSSOTRDD.filter(row => (row(1) != null && row(1) != 0 && row(2) != null && row(2) != 0))
      //
      //      auSSOTRDD.take(10).foreach(println)
    }


    sqlContext.setConf("spark.sql.caseSensitive", "false")
    val pspMlidAsscDF = sqlContext.read.parquet(pspMlidData)
    val mlidDtlDF = sqlContext.read.parquet(mlidDtlData)

    val orphGMRRDD = sqlContext.read.parquet(orphGMR).rdd.map(row => Row(0, null, null, null, row(3), row(2), row(1), row(4), row(5), null, null, null, null, null, null, null, null, null,
      null, null, null, null, null, null, null, null, null, "N", row(0), row(7)))

    //*****************************************************************************************

    // using the string column names:
    val mlidDtlDFSel = mlidDtlDF.select(hrchyDtlColWithoutGMRID.head, hrchyDtlColWithoutGMRID.tail: _*)

    val mlidDtlDFull = mlidDtlDF.select(hrchyDtlColWitGMRID.head, hrchyDtlColWitGMRID.tail: _*)
    // or, equivalently, using Column objects:
    //val result = dataframe.select(columnNames.map(c => col(c)): _*)


    val pspMLIDRDD = pspMlidAsscDF.rdd.map(x => (x.getLong(0), x)).repartition(totPartitions)

    //take distinct PSPID & MLID without GMRID
    val pspMLIDRDD1 = pspMLIDRDD.mapPartitions(iter => {
      iter.map(prod => (prod._1, prod._2.getInt(1)))
    }, true)
      .reduceByKey((x, _) => x)


    //take distinct MLID details group by MLID
    val mlidDtlRDD = mlidDtlDFSel.rdd.repartition(totPartitions).mapPartitions(x => {
      x.map(x => ((x.get(DTL_MRCH_HRCHY_ID)), (x)))
    }, true).reduceByKey { case (a, b) => a }.map(_._2)

    if (DEBUG_MODE) {
      pspMlidAsscDF.printSchema
      mlidDtlDF.printSchema
      println("Total elements in PSP MLID Data: " + pspMLIDRDD1.collect().length)
      println("Total elements in DTL MLID Data: " + mlidDtlRDD.collect().length)
      println("Total distinct MLIDs in DTL MLID Data: " + mlidDtlRDD.map(f => f.getLong(0)).distinct().count())
      mlidDtlDFSel.filter("mid_src_desc <> 'PSPID'").take(20).foreach(println)
      System.exit(0)
    }

    //****** CLOSURES for MLID Key Generation **********************
    //    val createPSPMLIDKey: ((Long, Int)) => (PSPHrchyKey, List[String]) = {
    //      case (a, b) => (PSPHrchyKey(a, b), null)
    //    }
    val createMLIDDtlKey: ((Long)) => HrchyDTLKey = {
      case (a) => (HrchyDTLKey(a))
    }

    //****** Guava table loader can be used for small lookups *******
    //val table = GuavaTableLoader.load(refDataPath, refDataFiles)
    //val bcTable = sc.broadcast(table)
    //***************************************************************

    //below two function calls are valid. One by def & other by val -> val is safer
    //val mlidPSPData = pspMlidRDD.map(x => createKeyValueTuple(x._1, x._2))
    //val mlidPSPData = pspMlidRDD.map(x => createPSPMLIDKey(x._1, x._2))
    //val mlidPSPData = pspMlidRDD.map(x => (createMLIDDtlKey(x._1), x))

    //    val mlidPSPData = pspMLIDRDD1.mapPartitions(iter => {
    //      iter.map(x => (createMLIDDtlKey(x._1), x))
    //    }, true)
    //
    //
    //    val mlidDTLData = mlidDtlRDD.mapPartitions(iter => {
    //      iter.map(x => (createMLIDDtlKey(x.getLong(0)), x))
    //    }, true)

    val mlidPSPData = pspMLIDRDD1.mapPartitions(iter => {
      iter.map(x => (x._1, x))
    }, true)


    val mlidDTLData = mlidDtlRDD.mapPartitions(iter => {
      iter.map(x => (x.getLong(0), x))
    }, true)


    //    val pspDataSorted = mlidPSPData.repartitionAndSortWithinPartitions(new MLIDPartitioner(totPartitions))
    //    val hrchyDataSorted = mlidDTLData.repartitionAndSortWithinPartitions(new MLIDPartitioner(totPartitions))

    //*******First build the PSPID & MLID DETAIL relationships *******
    var joinedData = mlidPSPData.join(mlidDTLData).mapPartitions(iter => iter.map(x => (Row.fromSeq(Array[Any](x._2._1._2) ++ x._2._2.toSeq))), true)
    //var joinedData = pspDataSorted.join(hrchyDataSorted).map(x => (Row.fromSeq(Array[Any](x._2._1._2) ++ x._2._2.toSeq)))

    if (DEBUG_MODE) {
      mlidPSPData.take(10).foreach(println)
      mlidDTLData.take(10).foreach(println)

      //print elements from a specific partition
      //pspDataSorted.mapPartitionsWithIndex((index: Int, it: Iterator[(HrchyDTLKey, Any)]) => it.take(10).toList.map(x => if (index == 1) {
      //  println(x._1)
      //}).iterator).collect
      //hrchyDataSorted.mapPartitionsWithIndex((index: Int, it: Iterator[(HrchyDTLKey, Any)]) => it.take(10).toList.map(x => if (index == 1) {
      //  println(x._1 + ":" + x._2)
      //}).iterator).collect

      println("------------------------------------------------------------------------")
      joinedData.take(10).foreach(println)
      println(countRDD(joinedData))
      println("------------------------------------------------------------------------")
    }


    //joinedData.cache()

    val rddPSPMLIDbroadcastVar = sc.broadcast(
      joinedData.keyBy { d => (d.getLong(1)) } // turn to pair RDD
        .collectAsMap() // collect as Map
    )


    var pspMLIDRDDJoined = pspMLIDRDD.mapPartitions({ iter =>
      iter.map(psp =>
        Row.fromSeq(rddPSPMLIDbroadcastVar.value.getOrElse(psp._1, Row(None)).toSeq ++ Array[Any](psp._2.getLong(2)) ++ Array[Any](psp._2.getString(3)))
      )
    }
    )

    pspMLIDRDDJoined = pspMLIDRDDJoined.filter(f => (f != None && f != Nil))


    //val pspMLIDJoinedDF = preparePSPMLIDDF(sqlContext, pspMLIDRDDJoined)

    if (DEBUG_MODE) {
      //pspMLIDJoinedDF.printSchema()
      pspMLIDRDDJoined.take(10).foreach(println)
    }

    val partitioner = new HashPartitioner(partitions = totPartitions)

    //join to get the auth cs linkages based details
    val mlidDtlForAuth = mlidDtlDFull.filter("gmr_id > 0").rdd
      .mapPartitions(iter=>{iter.map(x => (x.getLong(GMR_ID_IDX_POS), x))}).partitionBy(partitioner)

    //keep the key as CS GMRID
    var aucsRDD1: RDD[(Long, Long)] = sc.emptyRDD

    if (DEBUG_MODE)
      aucsRDD1 = aucsRDD.map(x => (x.getLong(3), x.getLong(2))).partitionBy(partitioner)
    else
      aucsRDD1 = aucsRDD.mapPartitions(iter => {iter.map(x => (x.getLong(1), x.getLong(0)))}).partitionBy(partitioner)


    var aucsDTLJoined = mlidDtlForAuth.join(aucsRDD1, partitioner)
      .mapPartitions({ iter => iter.map(x => (Row.fromSeq(x._2._1.toSeq ++ Array[Any](x._2._2)))) })

    try {
      hdfs.delete(new org.apache.hadoop.fs.Path(outputPath + "/" + OUTPUT_DIR_NAME + "/"), true)
    } catch {
      case _: Throwable => {}
    }
    //pspMLIDRDDJoined.map(r => r.mkString("\u0007")).saveAsTextFile(outputPath + "/pspmlid/")

    val aucsDTLJoined_final = aucsDTLJoined.mapPartitions(iter => {
      iter.map(row => Row(0, row(0), row(1), row(2), row(3), row(4), row(5), row(6), row(7), row(8), row(9), row(10), row(11), row(12), row(13), row(14), row(15),
        row(16), row(17), row(18), row(19), row(20), row(21), row(22), row(23), row(24), row(25), row(26), row(28), AUTH_STR))
    }, true)

    val mlidDTLCSFinal = mlidDtlDFull.filter("gmr_id > 0").rdd.mapPartitions({ iter => iter.map(row => Row.fromSeq(Array[Any](0) ++ row.toSeq ++ Array[Any](CLEARING_STR))) })

    mlidDTLCSFinal.union(aucsDTLJoined_final).union(pspMLIDRDDJoined).union(orphGMRRDD)
      .mapPartitions({ iter => iter.map(r => r.mkString(OUTPUT_DELIM)) }, true)
      .saveAsTextFile(outputPath + "/" + OUTPUT_DIR_NAME + "/")


    //val mlidDtlDFullRDD_C = mlidDtlDFull.filter("gmr_id > 0").rdd.map(x => (Row.fromSeq(Array[Any](0) ++ x.toSeq))).map(x => transformRowStringEnd("C", x))
    //val mlidDtlDFullRDD_A = mlidDtlDFull.filter("gmr_id > 0").rdd.map(x => (Row.fromSeq(Array[Any](0) ++ x.toSeq))).map(x => transformRowStringEnd("A", x))


    //    val mlidDtlDFullRDD_C_1 = mlidDtlDFullRDD_C.filter(x => {
    //      try {
    //        x.getLong(GMR_ID_IDX_POS) > 0
    //      } catch {
    //        case e: Exception => {
    //          false
    //        }
    //      }
    //    })
    //    val mlidDtlDFullRDD_A_1 = mlidDtlDFullRDD_A.filter(x => {
    //
    //      try {
    //        x.getLong(GMR_ID_IDX_POS) > 0
    //      } catch {
    //        case e: Exception => {
    //          false
    //        }
    //      }
    //    }
    //    )

    //    try {
    //      hdfs.delete(new org.apache.hadoop.fs.Path(outputPath + "/authcs/"), true)
    //    } catch {
    //      case _: Throwable => {}
    //    }
    //    aucsDTLJoined.map(r => r.mkString("\u0007")).saveAsTextFile(outputPath + "/authcs/")


    //    var aucsDTLJoinedDF = sqlContext.createDataFrame(aucsDTLJoined, AU_SCHEMA)
    //    aucsDTLJoinedDF = aucsDTLJoinedDF.withColumn("psp_id", lit(0))
    //    aucsDTLJoinedDF = aucsDTLJoinedDF.withColumn("tran_sys_cd", lit("A"))
    //    val aucsDTLJoinedDFSel = aucsDTLJoinedDF.select(aucsHrchyDataSet.head, aucsHrchyDataSet.tail: _*)

    //    val mlidDtlDFullRDD_C = mlidDtlDFull.filter("gmr_id > 0")
    //      .withColumn("psp_id", lit(0))
    //      .withColumn("tran_sys_cd", lit("C"))
    //
    //    val mlidDtlDFullDF = mlidDtlDFullRDD_C.select(aucsHrchyDataSet.head, aucsHrchyDataSet.tail: _*)


    //mlidDtlDFullRDD_C.write.mode("overwrite").parquet(outputPath + )

    //    mlidDtlDFullDF.write
    //      .format("com.databricks.spark.csv")
    //      .option("delimiter", "\u0007")
    //      .mode(SaveMode.Overwrite)
    //      .save(outputPath + "/mliddtl/")

    //    pspMLIDJoinedDF.write
    //      .format("com.databricks.spark.csv")
    //      .option("delimiter", "\u0007")
    //      .mode(SaveMode.Overwrite)
    //      .save(outputPath + "/pspmlid/")

    //    aucsDTLJoinedDFSel.write
    //      .format("com.databricks.spark.csv")
    //      .option("delimiter", "\u0007")
    //      .mode(SaveMode.Overwrite)
    //      .save(outputPath + "/authcs/")
    //
    //    orphGMRDF.write
    //      .format("com.databricks.spark.csv")
    //      .option("delimiter", "\u0007")
    //      .mode(SaveMode.Overwrite)
    //      .save(outputPath + "/orphgmr/")

    //val unionedDF = mlidDtlDFullDF.unionAll(pspMLIDJoinedDF).unionAll(aucsDTLJoinedDFSel).unionAll(orphGMRDF)


    //unionedRDD.map { x =>x.mkString("\0001") }.saveAsTextFile(outputPath)

    //unionedDF.write.mode("overwrite").parquet(outputPath) //parquet(outputPath)


    //println(countRDD(unionedRDD))
    //    var joinedPSP = pspMLIDRDD.mapPartitions({ iter =>
    //      val y = broadcastVar.value
    //      for {
    //        (x) <- iter
    //      } yield (x, y)
    //    }, preservesPartitioning = true)


    //val translatedData = keyedDataSorted.map(t => createDelayedFlight(t._1, t._2, bcTable))

    //printing out only done locally for demo purposes, usually write out to HDFS
    //translatedData.collect().foreach(println)
  }


  //  def checkIfGMRID(line: Row): Option[Boolean] = {
  //    try {
  //      Some(line.getLong(GMR_ID_IDX_POS) > 0)
  //    } catch {
  //      case e: Exception => Some(false)
  //    }
  //  }

  def countRDD(rdd: RDD[Row]): Long = {
    rdd.count()
  }

  //  def createDelayedFlight(key: FlightKey, data: List[String], bcTable: Broadcast[RefTable]): DelayedFlight = {
  //    val table = bcTable.value
  //    val airline = table.get(AIRLINE_DATA, key.airLineId)
  //    val destAirport = table.get(AIRPORT_DATA, key.arrivalAirportId.toString)
  //    val destCity = table.get(CITY_DATA, data(3))
  //    val origAirport = table.get(AIRPORT_DATA, data(1))
  //    val originCity = table.get(CITY_DATA, data(2))
  //
  //    DelayedFlight(airline, data.head, origAirport, originCity, destAirport, destCity, key.arrivalDelay)
  //  }
  //
  //  def createKeyValueTuple(i: Long, j: Int): (PSPHrchyKey, List[String]) = {
  //    (createKey(i,j), listData(i,j))
  //  }

  //  def createKey(data: Array[String]): FlightKey = {
  //    FlightKey(data(UNIQUE_CARRIER), safeInt(data(DEST_AIRPORT_ID)), safeDouble(data(ARR_DELAY)))
  //  }

  //  def createKey(i: Long, j: Int): PSPHrchyKey = {
  //    //FlightKey(data(UNIQUE_CARRIER), safeInt(data(DEST_AIRPORT_ID)), safeDouble(data(ARR_DELAY)))
  //    PSPHrchyKey(i,j)
  //  }

  //  def listData(i: Long, j: Int): List[String] = {
  //    List(i.toString, j.toString)
  //  }

  //def transformRow(i: Int, row: Row): Row = Row.fromSeq(Array[Any](i) ++ row.toSeq)

  def transformRowStringEnd(str: String, row: Row): Row = {
    return Row.fromSeq(row.toSeq ++ Array[Any](str))
  }


  //  def listData(i: Int, x: Row): Row = {
  //
  //
  ////    List(x.getLong(DTL_MRCH_HRCHY_ID), x.getString(DTL_MRCH_STORE_NM), x.getLong(DTL_VSID), x.getLong(DTL_BRND_HRCHY_ID),
  ////      x.getString(DTL_BRND_HRCHY_NM), x.getLong(DTL_VMID), x.getLong(DTL_ENTRPRS_HRCHY_ID), x.getString(DTL_ENTRSPRS_MRCH_NM), x.getString(DTL_MLID_ADDR_1_NM),
  ////      x.getString(DTL_MLID_ADDR_2_NM), x.getString(DTL_MLID_CITY_NM), x.getString(DTL_MLID_ST_PRVNC_CD), x.getDecimal(DTL_MLID_CTRY_CD), x.getString(DTL_MLID_PSTL_CD),
  ////      x.getString(DTL_MLID_PSTL_SUFX_CD), x.getDecimal(DTL_MLID_LAT_VAL), x.getDecimal(DTL_MLID_LONG_VAL), x.getString(DTL_VSID_CRT_TS), x.getString(DTL_VSID_UPS_TS),
  ////      x.getString(DTL_VMID_CRT_TS), x.getString(DTL_VMID_UPS_TS), x.getLong(DTL_MLID_DUNS), x.getLong(DTL_MLID_MAT_SID), x.getString(DTL_MLID_SRCE_SYS_CD),
  ////      x.getString(DTL_MLID_SRCE_DESC),x.getString(DTL_MID_SRCE_DESC), x.getString(DTL_SPNSR_MRCH_VSID_IND))
  //  }

  /* Sample snippets for future use
      //Example of what to do to strip off first line of file
    val rawData = sc.textFile(args(0)).mapPartitionsWithIndex((i, line) => skipLines(i, line, 1))
    //example of keyBy but retains entire array in value
    val keyedByData = rawDataArray.keyBy(arr => createKey(arr))
     val finalData = keyedDataSorted.map({ case (key, list) => DelayedFlight.fromKeyAndData(key, list) })

    //finalData.collect().foreach(println)

   */


}

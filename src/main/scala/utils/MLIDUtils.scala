package utils

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.{DataType, DecimalType}
import org.apache.spark.sql.functions.lit

object MLIDUtils {

  val MRCH_HRCHY_ID = 0
  val GMR_PSP_ID = 1
  val GMR_ID = 2
  val GMR_ID_TRAN_SYS_CD = 3

  val DTL_MRCH_HRCHY_ID = 0
  val GMR_ID_IDX_POS = 27
  val DEBUG_MODE = false
  val TOT_FINAL_DATA_PARTITIONS = 512
  val CLEARING_STR = "C"
  val AUTH_STR = "A"
  val OUTPUT_DIR_NAME = "aupspmlid"
  val OUTPUT_DELIM = "\u0007"
  val AUCS_SQLSTR = "select a.au_gmrid, a.cs_gmrid, a.au_gmr_pf_flag, a.vsid from gmr_poc.authcs_lnkg_combined a " +
    "where a.vsid > 0 and a.au_gmrid > 0 and a.cs_gmrid > 0 "

  //  val AU_SCHEMA = new StructType()
  //    //.add(StructField("psp_id", IntegerType, true))
  //    .add(StructField("mrch_hrchy_id", LongType, true))
  //    .add(StructField("mrch_store_nm", StringType, true))
  //    .add(StructField("vsid", LongType, true))
  //    .add(StructField("brnd_hrchy_id", LongType, true))
  //    .add(StructField("brnd_mrch_nm", StringType, true))
  //    .add(StructField("vmid", LongType, true))
  //    .add(StructField("entrprs_hrchy_id", LongType, true))
  //    .add(StructField("entrprs_mrch_nm", StringType, true))
  //    .add(StructField("mlid_addr_1_nm", StringType, true))
  //    .add(StructField("mlid_addr_2_nm", StringType, true))
  //    .add(StructField("city_nm", StringType, true))
  //    .add(StructField("mlid_st_prvnc_cd", StringType, true))
  //    .add(StructField("mlid_ctry_cd", DecimalType(4, 0), true))
  //    .add(StructField("mlid_pstl_cd", StringType, true))
  //    .add(StructField("mlid_pstl_sffx_cd", StringType, true))
  //    .add(StructField("locn_latitd_val", DecimalType(20, 9), true))
  //    .add(StructField("locn_longitd_val", DecimalType(20, 9), true))
  //    .add(StructField("vsid_crt_ts", StringType, true))
  //    .add(StructField("vsid_upd_ts", StringType, true))
  //    .add(StructField("vmid_crt_ts", StringType, true))
  //    .add(StructField("vmid_upd_ts", StringType, true))
  //    .add(StructField("duns_num", LongType, true))
  //    .add(StructField("mat_sid", IntegerType, true))
  //    .add(StructField("mrch_srce_sys_cd", StringType, true))
  //    .add(StructField("mlid_src_desc", StringType, true))
  //    .add(StructField("mid_src_desc", StringType, true))
  //    .add(StructField("spnr_mrch_vsid_ind", StringType, true))
  //    .add(StructField("cs_gmr_id", LongType, true))
  //    //.add(StructField("tran_sys_cd", StringType, true))
  //    .add(StructField("gmr_id", LongType, true))
  //
  //  val FINAL_SCHEMA = new StructType()
  //    .add(StructField("psp_id", IntegerType, true))
  //    .add(StructField("mrch_hrchy_id", LongType, true))
  //    .add(StructField("mrch_store_nm", StringType, true))
  //    .add(StructField("vsid", LongType, true))
  //    .add(StructField("brnd_hrchy_id", LongType, true))
  //    .add(StructField("brnd_mrch_nm", StringType, true))
  //    .add(StructField("vmid", LongType, true))
  //    .add(StructField("entrprs_hrchy_id", LongType, true))
  //    .add(StructField("entrprs_mrch_nm", StringType, true))
  //    .add(StructField("mlid_addr_1_nm", StringType, true))
  //    .add(StructField("mlid_addr_2_nm", StringType, true))
  //    .add(StructField("city_nm", StringType, true))
  //    .add(StructField("mlid_st_prvnc_cd", StringType, true))
  //    .add(StructField("mlid_ctry_cd", DecimalType(4, 0), true))
  //    .add(StructField("mlid_pstl_cd", StringType, true))
  //    .add(StructField("mlid_pstl_sffx_cd", StringType, true))
  //    .add(StructField("locn_latitd_val", DecimalType(20, 9), true))
  //    .add(StructField("locn_longitd_val", DecimalType(20, 9), true))
  //    .add(StructField("vsid_crt_ts", StringType, true))
  //    .add(StructField("vsid_upd_ts", StringType, true))
  //    .add(StructField("vmid_crt_ts", StringType, true))
  //    .add(StructField("vmid_upd_ts", StringType, true))
  //    .add(StructField("duns_num", LongType, true))
  //    .add(StructField("mat_sid", IntegerType, true))
  //    .add(StructField("mrch_srce_sys_cd", StringType, true))
  //    .add(StructField("mlid_src_desc", StringType, true))
  //    .add(StructField("mid_src_desc", StringType, true))
  //    .add(StructField("spnr_mrch_vsid_ind", StringType, true))
  //    .add(StructField("gmr_id", LongType, true))
  //    .add(StructField("tran_sys_cd", StringType, true))
  //
  val hrchyDtlColWithoutGMRID = Seq("mrch_hrchy_id", "mrch_store_nm", "vsid", "brnd_hrchy_id", "brnd_mrch_nm", "vmid", "entrprs_hrchy_id", "entrprs_mrch_nm",
    "mlid_addr_1_nm", "mlid_addr_2_nm", "city_nm", "mlid_st_prvnc_cd", "mlid_ctry_cd", "mlid_pstl_cd", "mlid_pstl_sffx_cd", "locn_latitd_val", "locn_longitd_val",
    "vsid_crt_ts", "vsid_upd_ts", "vmid_crt_ts", "vmid_upd_ts", "duns_num", "mat_sid", "mrch_srce_sys_cd", "mlid_src_desc", "mid_src_desc", "spnr_mrch_vsid_ind")

  val hrchyDtlColWitGMRID = Seq("mrch_hrchy_id", "mrch_store_nm", "vsid", "brnd_hrchy_id", "brnd_mrch_nm", "vmid", "entrprs_hrchy_id", "entrprs_mrch_nm",
    "mlid_addr_1_nm", "mlid_addr_2_nm", "city_nm", "mlid_st_prvnc_cd", "mlid_ctry_cd", "mlid_pstl_cd", "mlid_pstl_sffx_cd", "locn_latitd_val", "locn_longitd_val",
    "vsid_crt_ts", "vsid_upd_ts", "vmid_crt_ts", "vmid_upd_ts", "duns_num", "mat_sid", "mrch_srce_sys_cd", "mlid_src_desc", "mid_src_desc", "spnr_mrch_vsid_ind", "gmr_id")

  //
  //  val aucsHrchyDataSet = Seq("psp_id", "mrch_hrchy_id", "mrch_store_nm", "vsid", "brnd_hrchy_id", "brnd_mrch_nm", "vmid", "entrprs_hrchy_id", "entrprs_mrch_nm",
  //    "mlid_addr_1_nm", "mlid_addr_2_nm", "city_nm", "mlid_st_prvnc_cd", "mlid_ctry_cd", "mlid_pstl_cd", "mlid_pstl_sffx_cd", "locn_latitd_val", "locn_longitd_val",
  //    "vsid_crt_ts", "vsid_upd_ts", "vmid_crt_ts", "vmid_upd_ts", "duns_num", "mat_sid", "mrch_srce_sys_cd", "mlid_src_desc", "mid_src_desc", "spnr_mrch_vsid_ind",
  //    "gmr_id", "tran_sys_cd")

  //Lookup keys
  //  val AIRLINE_DATA = "L_UNIQUE_CARRIERS"
  //  val AIRPORT_DATA = "L_AIRPORT_ID"
  //  val CITY_DATA = "L_CITY_MARKET_ID"

  //  case class DelayedFlight(airLine: String,
  //                           date: String,
  //                           originAirport: String,
  //                           originCity: String,
  //                           destAirport: String,
  //                           destCity: String,
  //                           arrivalDelay: Double) {
  //  }

  //
  //  val preparePSPMLIDDF: ((SQLContext, RDD[Row])) => DataFrame = {
  //    case (a, b) => {
  //      import a.implicits._
  //      var pspMLIDJoinedDF1 = a.createDataFrame(b, FINAL_SCHEMA)
  //      //pspMLIDJoinedDF1 = pspMLIDJoinedDF1.drop("spnr_mrch_vsid_ind")
  //      pspMLIDJoinedDF1 = pspMLIDJoinedDF1.withColumn("spnr_mrch_vsid_ind", lit("N"))
  //      //    pspMLIDJoinedDF1 = pspMLIDJoinedDF1.select($"psp_id", $"mrch_hrchy_id", $"mrch_store_nm", $"vsid", $"brnd_hrchy_id", $"brnd_mrch_nm", $"vmid", $"entrprs_hrchy_id",
  //      //      $"entrprs_mrch_nm", $"mlid_addr_1_nm", $"mlid_addr_2_nm", $"city_nm", $"mlid_st_prvnc_cd", $"mlid_ctry_cd", $"mlid_pstl_cd", $"mlid_pstl_sffx_cd", $"locn_latitd_val", $"locn_longitd_val",
  //      //      $"vsid_crt_ts", $"vsid_upd_ts", $"vmid_crt_ts", $"vmid_upd_ts", $"duns_num", $"mat_sid", $"mrch_srce_sys_cd", $"mlid_src_desc", $"mid_src_desc", $"spnr_mrch_vsid_ind",
  //      //      $"gmr_id", $"tran_sys_cd")
  //
  //      pspMLIDJoinedDF1
  //    }
  //  }
  //
  //  val prepareOrphGMRDF: ((SQLContext, DataFrame)) => DataFrame = {
  //    case (a, b) => {
  //      import a.implicits._
  //
  //      var orphGMRDF = b
  //      //orphGMRDF = orphGMRDF.filter("orph_gmr_vmid_ind = \"Y\"")
  //      orphGMRDF = orphGMRDF.withColumn("psp_id", lit(null))
  //      orphGMRDF = orphGMRDF.withColumn("mrch_hrchy_id", lit(0.longValue()))
  //      orphGMRDF = orphGMRDF.withColumn("mrch_store_nm", lit(null))
  //      orphGMRDF = orphGMRDF.withColumn("vsid", lit(null))
  //      orphGMRDF = orphGMRDF.withColumn("mlid_addr_1_nm", lit(null))
  //      orphGMRDF = orphGMRDF.withColumn("mlid_addr_2_nm", lit(null))
  //      orphGMRDF = orphGMRDF.withColumn("city_nm", lit(null))
  //      orphGMRDF = orphGMRDF.withColumn("mlid_st_prvnc_cd", lit(null))
  //      //orphGMRDF = orphGMRDF.withColumn("mlid_ctry_cd", lit(0.0).cast("decimal(4,0)"))
  //      orphGMRDF = orphGMRDF.withColumn("mlid_ctry_cd", lit(null))
  //      orphGMRDF = orphGMRDF.withColumn("mlid_pstl_cd", lit(null))
  //      orphGMRDF = orphGMRDF.withColumn("mlid_pstl_sffx_cd", lit(null))
  //      //    orphGMRDF = orphGMRDF.withColumn("locn_latitd_val", lit(null).cast("decimal(20,9)"))
  //      //    orphGMRDF = orphGMRDF.withColumn("locn_longitd_val", lit(null).cast("decimal(20,9)"))
  //      orphGMRDF = orphGMRDF.withColumn("locn_latitd_val", lit(null))
  //      orphGMRDF = orphGMRDF.withColumn("locn_longitd_val", lit(null))
  //      orphGMRDF = orphGMRDF.withColumn("vsid_crt_ts", lit(null))
  //      orphGMRDF = orphGMRDF.withColumn("vsid_upd_ts", lit(null))
  //      orphGMRDF = orphGMRDF.withColumn("vmid_crt_ts", lit(null))
  //      orphGMRDF = orphGMRDF.withColumn("vmid_upd_ts", lit(null))
  //      orphGMRDF = orphGMRDF.withColumn("duns_num", lit(null))
  //      orphGMRDF = orphGMRDF.withColumn("mat_sid", lit(null))
  //      orphGMRDF = orphGMRDF.withColumn("mrch_srce_sys_cd", lit("ORPH_GMR"))
  //      orphGMRDF = orphGMRDF.withColumn("mlid_src_desc", lit(null))
  //      orphGMRDF = orphGMRDF.withColumn("mid_src_desc", lit(null))
  //      orphGMRDF = orphGMRDF.withColumn("spnr_mrch_vsid_ind", lit("N"))
  //
  //
  //      orphGMRDF = orphGMRDF.select($"psp_id", $"mrch_hrchy_id", $"mrch_store_nm", $"vsid", $"mid" as "brnd_hrchy_id", $"brnd_mrch_nm", $"vmid", $"eid" as "entrprs_hrchy_id",
  //        $"entrprs_mrch_nm", $"mlid_addr_1_nm", $"mlid_addr_2_nm", $"city_nm", $"mlid_st_prvnc_cd", $"mlid_ctry_cd", $"mlid_pstl_cd", $"mlid_pstl_sffx_cd", $"locn_latitd_val", $"locn_longitd_val",
  //        $"vsid_crt_ts", $"vsid_upd_ts", $"vmid_crt_ts", $"vmid_upd_ts", $"duns_num", $"mat_sid", $"mrch_srce_sys_cd", $"mlid_src_desc", $"mid_src_desc", $"spnr_mrch_vsid_ind",
  //        $"gmr_id", $"tran_sys_cd")
  //
  //      if (DEBUG_MODE) {
  //        orphGMRDF.printSchema()
  //
  //        orphGMRDF.take(10).foreach(println)
  //      }
  //      orphGMRDF
  //    }
  //  }


  //  case class FlightKey(mrchHrchyId: Long, gmrPSPId: Integer, gmrId: Long)
  //
  //  object FlightKey {
  //
  //    implicit def orderingByIdAirportIdDelay[A <: FlightKey] : Ordering[A] = {
  //      //Ordering.by(fk => (fk.mrchHrchyId, fk.gmrPSPId, fk.gmrId * -1))
  //      Ordering.by(fk => (fk.mrchHrchyId, fk.gmrPSPId, fk.gmrId))
  //    }
  //  }

  case class PSPHrchyKey(mrchHrchyId: Long, gmrPSPId: Int)

  object PSPHrchyKey {

    implicit def orderingByIdPSPMLID[A <: PSPHrchyKey]: Ordering[A] = {
      //Ordering.by(fk => (fk.mrchHrchyId, fk.gmrPSPId, fk.gmrId * -1))
      Ordering.by(fk => (fk.mrchHrchyId, fk.gmrPSPId))
    }
  }

  class MLIDPartitioner(partitions: Int) extends Partitioner {
    require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

    override def numPartitions: Int = partitions

    override def getPartition(key: Any): Int = {

      var part = 0
      if (key.isInstanceOf[PSPHrchyKey]) {
        val k = key.asInstanceOf[PSPHrchyKey]
        part = k.mrchHrchyId.hashCode() % numPartitions
      }
      else if (key.isInstanceOf[HrchyDTLKey]) {
        val k = key.asInstanceOf[HrchyDTLKey]
        part = k.mrchHrchyId.hashCode() % numPartitions
      }

      part
    }
  }

  //---------------------------------------------------------------------
  case class HrchyDTLKey(mrchHrchyId: Long)

  object HrchyDTLKey {

    implicit def orderingByMLID[A <: HrchyDTLKey]: Ordering[A] = {
      //Ordering.by(fk => (fk.mrchHrchyId, fk.gmrPSPId, fk.gmrId * -1))
      Ordering.by(fk => (fk.mrchHrchyId))
    }
  }

}

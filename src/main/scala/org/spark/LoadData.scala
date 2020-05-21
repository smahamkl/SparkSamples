package org.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

/**
  * Created by smahamka on 3/21/17.
  */
object LoadData {

  def emptyValueSubstitution = udf[String, String] {
    case "" => "NA"
    case null => "null"
    case value => value
  }

  val TAB_DELIM = "\007"

  def main(args: Array[String]): Unit = {

    val master = args(0)
    val sqlStr = args(1)

    val conf = new SparkConf().setAppName("GMRFrameworkFinalJoin").setMaster(master)
    val sc = new SparkContext(conf)


    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    sqlContext.setConf("spark.sql.caseSensitive", "false")
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    val (finalHDFSDir, parqfileFVS, parqfileDnb, parqfileAmmf, parqfile, auditFile, parqOTF, parqMBRP, finalAMMFDNB) = try {
      //val prop = new Properties()
      //prop.load(new FileInputStream(new File(runDir + "/config.properties")))
      (
        args(2),
        sqlContext.read.parquet(args(3)),
        sqlContext.read.parquet(args(4)),
        sqlContext.read.parquet(args(5)),
        sqlContext.read.parquet(args(6)).filter("gmr_tran_source='C'"),
        (if (master == "local") sqlContext.read.parquet(args(7)).filter("srce_nm='GMRID'") else sqlContext.read.format("com.databricks.spark.avro").load(args(7)).filter("srce_nm='GMRID'")),
        sqlContext.read.parquet(args(8)).filter("clas='OTHER'"),
        sqlContext.read.parquet(args(9)),
        args(10)
      )
    } catch {
      case e: Exception =>
        e.printStackTrace()
        sys.exit(1)
    }

    println("master:" + master)
    println("sqlGMRs:" + sqlStr)
    println("finalHDFS:" + finalHDFSDir)
    println("parqfileFVS:" + parqfileFVS)
    println("parqfileDnb:" + parqfileDnb)
    println("parqfileAmmf:" + parqfileAmmf)
    println("FlatView:" + parqfile)
    println("auditFile:" + auditFile)
    println("parqOTF:" + parqOTF)
    println("parqMBRP:" + parqMBRP)


    import sqlContext.implicits._


    val otfDF = parqOTF.select($"mrch_locn_id", $"otf_b1_name", $"mwd_b1_name", $"mwd_b_name", $"user_b1_name", $"user_b1_name", $"user_b_name").persist()
    sc.broadcast(otfDF)
    otfDF.count()

    val join1DF = parqfile.as('fv).join(parqfileFVS.as('fvs), parqfile("gmr_id") === parqfileFVS("gmr_id"), "left_outer")
      .join(parqMBRP.as('mbrp), parqfile("gmr_id") === parqMBRP("gmr_id"), "left_outer")
      .join(auditFile.as('audit), parqfile("gmr_id") === auditFile("srce_rec_id") && parqfile("gmr_mlid_id") === auditFile("mrch_hrchy_id"), "left_outer")
      .join(otfDF.as('otf), parqfile("gmr_mlid_id") === otfDF("mrch_locn_id"), "left_outer")

    // join1DF.select($"a.mlid_hrchy_id_l", $"b.gmr_id", $"b.gmrid_cleansed_nm", $"a.duns_num", $"a.mat_sid", $"c.otf_b1_name", $"c.mwd_b1_name", $"c.mwd_b_name",
    //  $"d.mrch_dba_nm",$"e.category_of_change").distinct().take(50).foreach(println)

    var result1DF = join1DF.select($"fvs.mlid_hrchy_id_l", $"fvs.mat_sid", $"fvs.duns_num", $"fvs.mlid_bus_nm_l", $"fvs.vsid", $"fvs.mlid_hrchy_id_bm",
      $"fvs.mlid_bus_nm_bm", $"fvs.mrch_srce_sys_cd", $"fvs.mlid_src_desc",
      $"fvs.mid_src_desc", $"fvs.vmid", $"fvs.mlid_addr_1_nm", $"fvs.mlid_addr_2_nm", $"fvs.city_nm", $"fvs.mlid_st_prvnc_cd", $"fvs.mlid_pstl_cd",
      $"fvs.mlid_ctry_cd", $"fvs.vsid_crt_ts", $"fvs.vsid_upd_ts", $"fvs.vmid_crt_ts", $"fvs.vmid_upd_ts", $"fv.gmr_id",
      $"fv.gmr_mrch_nm", $"fv.gmr_names_concatd", $"fv.gmr_cib_num", $"fv.gmr_bin_num", $"fv.gmr_crd_acptr_id", $"fv.gmr_mat_sid",
      $"fv.gmr_mrch_catg_cd", $"fv.gmr_mrch_mcc_desc", $"fv.gmr_mrch_stor_num", $"fv.gmr_tran_mrch_city_nm", $"fv.gmr_tran_mrch_st_cd", $"fv.gmr_tran_mrch_pstl_cd",
      $"fv.gmr_tran_mrch_ctry_cd", $"fv.gmr_tran_mrch_ctry_nm", $"fv.gmr_id_func_val", $"fv.gmr_id_ptrn_id",
      $"otf.otf_b1_name", $"otf.mwd_b1_name", $"otf.mwd_b_name", $"otf.user_b1_name", $"user_b_name", $"audit.category_of_change", $"audit.odate",
      $"mbrp.mrch_corp_nm", $"mbrp.mrch_dba_id", $"mbrp.mrch_dba_nm", $"mbrp.dba_mbrp_rul_ind", $"mbrp.drf_tran_cnt", $"mbrp.drf_tran_us_amt")


    result1DF = result1DF.withColumnRenamed("user_b1_name", "user_bus_nm2")
    result1DF = result1DF.withColumnRenamed("user_b_name", "user_bus_nm1")
    result1DF = result1DF.withColumnRenamed("city_nm", "mlid_city_nm")
    result1DF = result1DF.withColumnRenamed("mwd_b1_name", "mwd_bus_nm2")
    result1DF = result1DF.withColumnRenamed("mwd_b_name", "mwd_bus_nm1")
    result1DF = result1DF.withColumnRenamed("otf_b1_name", "otf_bus_nm")
    result1DF = result1DF.withColumnRenamed("duns_num", "gmr_duns_num")

    result1DF.printSchema()

    result1DF.persist(StorageLevel.MEMORY_AND_DISK)
    result1DF.registerTempTable("join1")


    //---------   DNB EXCL --------------
    parqfileDnb.registerTempTable("dnb")

    //val parqfileDnb1_excl = parqfileDnb.filter("mlid_hrchy_id_l not in(" + sqlStr + ")")
    //parqfileDnb1_excl.registerTempTable("dnb_excl_ext")
    //---------   AMMF EXCL --------------

    parqfileAmmf.registerTempTable("ammf")
    //val parqfileAmmf1_excl = parqfileAmmf.filter("mlid_hrchy_id_l not in(" + sqlStr + ")")
    //parqfileAmmf1_excl.registerTempTable("ammf_excl_ext")

    //---------   DNB  --------------

    //val parqfileDnb1 = parqfileDnb.filter("mlid_hrchy_id_l in(" + sqlStr + ")")
    //parqfileDnb1.persist()
    //Broadcast(parqfileDnb1)
    //perform an action to force the evaluation
    //parqfileDnb1.count()
    //parqfileDnb1.registerTempTable("dnb_ext")
    //---------   AMMF  --------------

    //val parqfileAmmf1 = parqfileAmmf.filter("mlid_hrchy_id_l in(" + sqlStr + ")")
    //parqfileAmmf1.persist()
    //Broadcast(parqfileAmmf1)
    //parqfileAmmf1.count()
    //parqfileAmmf1.registerTempTable("ammf_ext")

    //------------------------------------
    val results1 = sqlContext.sql("SELECT " +
      "a.mlid_hrchy_id_l as mrch_hrchy_id,a.mlid_bus_nm_l as mrch_hrchy_store_nm,a.vsid, " +
      "a.mlid_hrchy_id_bm as mlid_brand_mrch_id,a.mlid_bus_nm_bm as mlid_brand_mrch_nm,a.mrch_srce_sys_cd as mlid_src_nm, " +
      "upper(a.mlid_src_desc) as mlid_src_nm_desc, upper(a.mid_src_desc) as brand_src_nm_desc,a.vmid,a.mlid_addr_1_nm,a.mlid_addr_2_nm,a.mlid_city_nm,a.mlid_st_prvnc_cd,a.mlid_pstl_cd," +
      "a.mlid_ctry_cd,a.vsid_crt_ts,a.vsid_upd_ts,a.vmid_crt_ts,a.vmid_upd_ts,a.gmr_id," +
      "a.gmr_mrch_nm as gmr_eo_name,a.gmr_names_concatd as gmr_raw_names,a.gmr_cib_num,a.gmr_bin_num,a.gmr_crd_acptr_id,a.gmr_mat_sid as gmr_x_mat_sid," +
      "a.gmr_mrch_catg_cd,a.gmr_mrch_mcc_desc,a.gmr_mrch_stor_num,a.gmr_tran_mrch_city_nm as gmr_city_nm,a.gmr_tran_mrch_st_cd as gmr_state_cd,a.gmr_tran_mrch_pstl_cd as gmr_pstl_cd," +
      "a.gmr_tran_mrch_ctry_cd as gmr_ctry_cd,a.gmr_tran_mrch_ctry_nm as gmr_ctry_nm,a.gmr_id_func_val,a.gmr_id_ptrn_id,d.duns_num as mlid_duns_num,upper(d.dnb_frchs_nm) as dnb_frchs_nm," +
      "upper(d.dnb_bus_nm) as dnb_bus_nm,upper(d.dnb_trd_style_1_nm) as dnb_trd_style_1_nm," +
      "upper(d.dnb_trd_style_2_nm) as dnb_trd_style_2_nm,upper(d.dnb_trd_style_3_nm) as dnb_trd_style_3_nm," +
      "upper(d.dnb_trd_style_4_nm) as dnb_trd_style_4_nm,upper(d.dnb_trd_style_5_nm) as dnb_trd_style_5_nm," +
      "upper(d.dnb_hq_parnt_bus_nm) as dnb_hq_parnt_bus_nm ,upper(d.dnb_str_1_addr) as dnb_str_1_addr," +
      "upper(d.dnb_city_nm) as dnb_city_nm,upper(d.dnb_st_prvnc_nm) as dnb_st_prvnc_nm,d.dnb_pstl_cd," +
      "upper(d.dnb_ctry_nm) as dnb_ctry_nm,d.dmstc_ultm_duns_num," +
      "upper(d.dmstc_ultm_bus_nm) as dmstc_ultm_bus_nm,d.glbl_ultm_duns_num,upper(d.dnb_glbl_ultm_bus_nm) as dnb_glbl_ultm_bus_nm, " +
      "e.mat_sid as mlid_mat_sid,  " +
      "e.ammf_mrch_dba_nm,e.ammf_mrch_bus_lgl_nm,e.ammf_mrch_tax_idntfn_num,e.ammf_acqr_bin,e.ammf_crd_acptr_id," +
      "e.ammf_mrch_str_1_addr,e.ammf_mrch_str_2_addr,e.ammf_mrch_city_nm,e.ammf_mrch_st_prvnc_cd,e.ammf_mrch_pstl_cd," +
      "e.ammf_mrch_ctry_cd,otf_bus_nm, mwd_bus_nm1, mwd_bus_nm2, user_bus_nm1, user_bus_nm2,upper(category_of_change) as gmr_mlid_change_rsn_desc,odate as gmr_mlid_change_dt, " +
      "mrch_corp_nm, mrch_dba_id, mrch_dba_nm, dba_mbrp_rul_ind, drf_tran_cnt as prev_mth_tran_cnt, drf_tran_us_amt as prev_mth_drf_tran_us_amt " +
      "FROM " +
      //"(select * from join1 where mlid_hrchy_id_l not in(" + sqlStr + ") ) a " +
      "(select * from join1 ) a " +
      "left outer join dnb d on a.mlid_hrchy_id_l=d.mlid_hrchy_id_l  and a.gmr_id = d.gmr_id " +
      "left outer join ammf e on a.mlid_hrchy_id_l=e.mlid_hrchy_id_l and a.gmr_id = e.gmr_id ")
    //and a.gmr_duns_num = d.duns_num | and a.mat_sid = e.mat_sid

    results1.persist(StorageLevel.MEMORY_AND_DISK)

    (if (master == "local") results1.rdd.saveAsTextFile(finalHDFSDir) else results1.write.mode("overwrite").format("parquet").save(finalHDFSDir))

    //if(master == "local")
    System.exit(0)

    //sqlContext.sql("SET spark.sql.autoBroadcastJoinThreshold = -1")
    val results2 = sqlContext.sql("SELECT " +
      "a.mlid_hrchy_id_l as mrch_hrchy_id,a.mlid_bus_nm_l as mrch_hrchy_store_nm,a.vsid, " +
      "a.mlid_hrchy_id_bm as mlid_brand_mrch_id,a.mlid_bus_nm_bm as mlid_brand_mrch_nm,a.mrch_srce_sys_cd as mlid_src_nm, " +
      "upper(a.mlid_src_desc) as mlid_src_nm_desc, upper(a.mid_src_desc) as brand_src_nm_desc,a.vmid,a.mlid_addr_1_nm,a.mlid_addr_2_nm,a.mlid_city_nm,a.mlid_st_prvnc_cd,a.mlid_pstl_cd," +
      "a.mlid_ctry_cd,a.vsid_crt_ts,a.vsid_upd_ts,a.vmid_crt_ts,a.vmid_upd_ts,a.gmr_id," +
      "a.gmr_mrch_nm as gmr_eo_name,a.gmr_names_concatd as gmr_raw_names,a.gmr_cib_num,a.gmr_bin_num,a.gmr_crd_acptr_id,a.gmr_mat_sid as gmr_x_mat_sid," +
      "a.gmr_mrch_catg_cd,a.gmr_mrch_mcc_desc,a.gmr_mrch_stor_num,a.gmr_tran_mrch_city_nm as gmr_city_nm,a.gmr_tran_mrch_st_cd as gmr_state_cd,a.gmr_tran_mrch_pstl_cd as gmr_pstl_cd," +
      "a.gmr_tran_mrch_ctry_cd as gmr_ctry_cd,a.gmr_tran_mrch_ctry_nm as gmr_ctry_nm,a.gmr_id_func_val,a.gmr_id_ptrn_id,a.gmr_duns_num as mlid_duns_num,upper(d.dnb_frchs_nm) as dnb_frchs_nm," +
      "upper(d.dnb_bus_nm) as dnb_bus_nm,upper(d.dnb_trd_style_1_nm) as dnb_trd_style_1_nm," +
      "upper(d.dnb_trd_style_2_nm) as dnb_trd_style_2_nm,upper(d.dnb_trd_style_3_nm) as dnb_trd_style_3_nm," +
      "upper(d.dnb_trd_style_4_nm) as dnb_trd_style_4_nm,upper(d.dnb_trd_style_5_nm) as dnb_trd_style_5_nm," +
      "upper(d.dnb_hq_parnt_bus_nm) as dnb_hq_parnt_bus_nm ,upper(d.dnb_str_1_addr) as dnb_str_1_addr," +
      "upper(d.dnb_city_nm) as dnb_city_nm,upper(d.dnb_st_prvnc_nm) as dnb_st_prvnc_nm,d.dnb_pstl_cd," +
      "upper(d.dnb_ctry_nm) as dnb_ctry_nm,d.dmstc_ultm_duns_num," +
      "upper(d.dmstc_ultm_bus_nm) as dmstc_ultm_bus_nm,d.glbl_ultm_duns_num,upper(d.dnb_glbl_ultm_bus_nm) as dnb_glbl_ultm_bus_nm, " +
      "a.mat_sid as mlid_mat_sid,  " +
      "e.ammf_mrch_dba_nm,e.ammf_mrch_bus_lgl_nm,e.ammf_mrch_tax_idntfn_num,e.ammf_acqr_bin,e.ammf_crd_acptr_id," +
      "e.ammf_mrch_str_1_addr,e.ammf_mrch_str_2_addr,e.ammf_mrch_city_nm,e.ammf_mrch_st_prvnc_cd,e.ammf_mrch_pstl_cd," +
      "e.ammf_mrch_ctry_cd,otf_bus_nm, mwd_bus_nm1, mwd_bus_nm2, user_bus_nm1, user_bus_nm2,upper(category_of_change) as gmr_mlid_change_rsn_desc,odate as gmr_mlid_change_dt, " +
      "mrch_corp_nm, mrch_dba_id, mrch_dba_nm, dba_mbrp_rul_ind, drf_tran_cnt as prev_mth_tran_cnt, drf_tran_us_amt as prev_mth_drf_tran_us_amt " +
      "FROM " +
      "(select * from join1 where mlid_hrchy_id_l in (" + sqlStr + ") ) a " +
      "left outer join dnb_ext d on a.mlid_hrchy_id_l=d.mlid_hrchy_id_l  " +
      "left outer join ammf_ext e on a.mlid_hrchy_id_l=e.mlid_hrchy_id_l  ")

    results2.persist(StorageLevel.MEMORY_AND_DISK)
    results2.write.mode("append").format("parquet").save(finalHDFSDir)

   val results3 = sqlContext.sql("SELECT " +
      "a.mlid_hrchy_id_l as mrch_hrchy_id,a.mlid_bus_nm_l as mrch_hrchy_store_nm,a.vsid, " +
      "a.mlid_hrchy_id_bm as mlid_brand_mrch_id,a.mlid_bus_nm_bm as mlid_brand_mrch_nm,a.mrch_srce_sys_cd as mlid_src_nm, " +
      "'' as mlid_src_nm_desc, '' as brand_src_nm_desc,a.vmid,a.mlid_addr_1_nm,a.mlid_addr_2_nm,a.mlid_city_nm,a.mlid_st_prvnc_cd,a.mlid_pstl_cd," +
      "a.mlid_ctry_cd,a.vsid_crt_ts,a.vsid_upd_ts,a.vmid_crt_ts,a.vmid_upd_ts,a.gmr_id," +
      "a.gmr_mrch_nm as gmr_eo_name,a.gmr_names_concatd as gmr_raw_names,a.gmr_cib_num,a.gmr_bin_num,a.gmr_crd_acptr_id,a.gmr_mat_sid as gmr_x_mat_sid," +
      "a.gmr_mrch_catg_cd,a.gmr_mrch_mcc_desc,a.gmr_mrch_stor_num,a.gmr_tran_mrch_city_nm as gmr_city_nm,a.gmr_tran_mrch_st_cd as gmr_state_cd," +
      "a.gmr_tran_mrch_pstl_cd as gmr_pstl_cd," +
      "a.gmr_tran_mrch_ctry_cd as gmr_ctry_cd,a.gmr_tran_mrch_ctry_nm as gmr_ctry_nm,a.gmr_id_func_val,a.gmr_id_ptrn_id,a.gmr_duns_num as mlid_duns_num, " +
      "'' as dnb_frchs_nm," +
      "'' as dnb_bus_nm,'' as dnb_trd_style_1_nm," +
      "'' as dnb_trd_style_2_nm,'' as dnb_trd_style_3_nm," +
      "'' as dnb_trd_style_4_nm,'' as dnb_trd_style_5_nm," +
      "'' as dnb_hq_parnt_bus_nm ,'' as dnb_str_1_addr," +
      "'' as dnb_city_nm,'' as dnb_st_prvnc_nm,'' as dnb_pstl_cd," +
      "'' as dnb_ctry_nm,'' as dmstc_ultm_duns_num," +
      "'' as dmstc_ultm_bus_nm,'' as glbl_ultm_duns_num,'' as dnb_glbl_ultm_bus_nm, " +
      "0 as mlid_mat_sid,  " +
      "'' as ammf_mrch_dba_nm,'' as ammf_mrch_bus_lgl_nm,'' as ammf_mrch_tax_idntfn_num,'' as ammf_acqr_bin,'' as ammf_crd_acptr_id," +
      "'' as ammf_mrch_str_1_addr,'' as ammf_mrch_str_2_addr,'' as ammf_mrch_city_nm,'' as ammf_mrch_st_prvnc_cd,'' as ammf_mrch_pstl_cd," +
      "'' as ammf_mrch_ctry_cd,otf_bus_nm, mwd_bus_nm1, mwd_bus_nm2, user_bus_nm1, user_bus_nm2,'' as gmr_mlid_change_rsn_desc,odate as gmr_mlid_change_dt, " +
      "mrch_corp_nm, mrch_dba_id, mrch_dba_nm, dba_mbrp_rul_ind, drf_tran_cnt as prev_mth_tran_cnt, drf_tran_us_amt as prev_mth_drf_tran_us_amt " +
      "FROM " +
      "(select * from join1 where mlid_hrchy_id_l is null ) a ")

    results3.persist(StorageLevel.MEMORY_AND_DISK)
    results3.write.mode("append").format("parquet").save(finalHDFSDir)

  }

}

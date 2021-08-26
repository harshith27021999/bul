# Databricks notebook source
# MAGIC %sql
# MAGIC select count(*) from delta.`/mnt/e2ecdll0_l2objects/l2objectscore/prodATP/dna_md_cdm_delivery/` where l2_upt_ between '2021-08-0T00:00:00' and '2021-08-07T00:00:00'

# COMMAND ----------

# MAGIC %sql
# MAGIC describe delta.`/mnt/e2ecdll0_l2objects/atp/l2OpenOrders`

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select SRC_SYS_CD,max(l2_upt_) from delta.`/mnt/e2ecdll0_l2objects/l2objectscore/prodATP/dna_ph_cdm_materialuom/` group by SRC_SYS_CD

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select MFG_DT, from delta.`/mnt/e2ecdll0_l2objects/l2objectscore/prodATP/dna_md_cdm_delivery/`  where MFG_DT < "1753-01-01";

# COMMAND ----------

l2Query = f"""select *  from delta.`/mnt/e2ecdll0_l2objects/l2objectscore/prodATP/dna_md_cdm_materialuom/`"""
dataFromTheMntlocation=spark.sql(l2Query)
dataFromTheMntlocation.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC  SELECT --MARM.MANDT,
# MAGIC -- MARM.MATNR,
# MAGIC -- MARM.MEINH,
# MAGIC -- T006A.MSEHL,
# MAGIC -- T006A.MSEHI
# MAGIC -- MARM.EAN11,
# MAGIC -- MARM.NUMTP,
# MAGIC T006.ISOCODE
# MAGIC -- MARM.UMREZ,
# MAGIC -- MARM.UMREN,
# MAGIC -- MARM.BREIT,
# MAGIC -- MARM.MEABM,
# MAGIC -- MARM.BRGEW,
# MAGIC -- MARM.GEWEI,
# MAGIC -- MARM.LAENG,
# MAGIC -- MARM.HOEHE,
# MAGIC -- MARM.VOLUM,
# MAGIC -- MARM.VOLEH 
# MAGIC FROM delta.`/mnt/ajdpdeltaadlsn3/prd/raw/hcs/t006` as T006

# COMMAND ----------

# MAGIC %sql
# MAGIC describe delta.`/mnt/ajdpdeltaadlsn3/prd/raw/hcs/mchb` 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from delta.`/mnt/e2ecdll0_l2objects/l2objectscore/prodATP/dna_ph_cdm_materialuom/`  where l2_upt_ > "2021-08-17T10:11:08" and SRC_SYS_CD ="BBN"

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct MARM._upt_ from  delta.`/mnt/ajdpdeltaadlsn1/prd/raw/bbl/marm/` MARM Left Outer Join delta.`/mnt/ajdpdeltaadlsn1/prd/raw/bbl/t006/` T006 ON MARM.MSEHI = T006.MSEHI and MARM.MANDT=T006.MANDT Left Outer Join delta.`/mnt/ajdpdeltaadlsn1/prd/raw/bbl/t006a/` t006a ON MARM.MSEHI =  T006A.MSEHI and T006A._deleted_='F' and MARM.MANDT=T006A.MANDT and T006A.SPRAS = 'E' inner Join delta.`/mnt/ajdpdeltaadlsn1/prd/raw/bbl/mara/` mara on marm.MATNR = MARA.MATNR where mara.ZZSECTOR ='PHR' and (MARM._upt_ BETWEEN '2020-10-16' AND '2021-05-04') order by MARM._upt_ asc

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct l0_upt_ from  delta.`/mnt/e2ecdll0_l2objects/l2objectscore/prodATP/dna_ph_cdm_materialuom/` where  SRC_SYS_CD = 'BBL' and (l0_upt_ BETWEEN '2020-10-16T11:15:22.481+0000' AND '2021-05-04') order by l0_upt_ asc

# COMMAND ----------

# MAGIC %sql
# MAGIC  SELECT --SRC_SYS_CD_FUNC('{sourceName}') AS SRC_SYS_CD,
# MAGIC         MARM.MANDT AS CLI,
# MAGIC         UPPER('{sectorName}') AS SECTR,
# MAGIC         MARM.MATNR AS MTL_NO,
# MAGIC         MARM.MEINH AS MTL_ALT_UOM_CD,
# MAGIC         MARM._upt_ as l0_upt_,
# MAGIC         --current_timestamp() as l2_upt_,
# MAGIC         MARM._deleted_ as _deleted_,
# MAGIC         T006A.MSEHL AS UOM_DESCN,
# MAGIC         T006A.MSEHI AS INT_MEAS_UNIT,
# MAGIC         MARM.EAN11 AS GTIN_NO,
# MAGIC         MARM.NUMTP AS GTIN_CAT_CD,
# MAGIC         T006.ISOCODE AS ISO_CODE,
# MAGIC         CAST(MARM.UMREZ AS DECIMAL(6)) AS NUMNTR_MEAS,
# MAGIC         CAST(MARM.UMREN AS DECIMAL(6)) AS DNMNTR_MEAS,
# MAGIC         CAST(MARM.BREIT AS DECIMAL(17,3)) AS WDT_MEAS,
# MAGIC         MARM.MEABM AS DIM_UOM_CD,
# MAGIC         CAST(MARM.BRGEW AS DECIMAL(17,3)) AS GRS_WGT_MEAS,
# MAGIC         MARM.GEWEI AS WT_UOM_CD,
# MAGIC         CAST(MARM.LAENG AS DECIMAL(17,3)) AS LEN_MEAS,
# MAGIC         CAST(MARM.HOEHE AS DECIMAL(17,3)) AS HGT_MEAS,
# MAGIC         CAST(MARM.VOLUM AS DECIMAL(17,3)) AS VOL_MEAS,
# MAGIC         MARM.VOLEH AS VOL_UOM_CD from  delta.`/mnt/ajdpdeltaadlsn1/prd/raw/bbl/marm/` MARM Left Outer Join delta.`/mnt/ajdpdeltaadlsn1/prd/raw/bbl/t006/` T006 ON MARM.MSEHI = T006.MSEHI and MARM.MANDT=T006.MANDT Left Outer Join delta.`/mnt/ajdpdeltaadlsn1/prd/raw/bbl/t006a/` t006a ON MARM.MSEHI =  T006A.MSEHI and T006A._deleted_='F' and MARM.MANDT=T006A.MANDT and T006A.SPRAS = 'E' inner Join delta.`/mnt/ajdpdeltaadlsn1/prd/raw/bbl/mara/` mara on marm.MATNR = MARA.MATNR where mara.ZZSECTOR ='PHR' and (MARM._upt_ = '2021-04-27T09:19:34.504+0000
# MAGIC ')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from  delta.`/mnt/e2ecdll0_l2objects/l2objectscore/prodATP/dna_md_cdm_materialuom/` where  SRC_SYS_CD = 'BBL' and (l0_upt_ ="2021-04-27T09:19:34.504+0000") 

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from delta.`/mnt/e2ecdll0_l2objects/l2objectscore/prodATP/dna_md_cdm_delivery/` where l2_upt_ between '2021-08-11T00:00:00' and '2021-08-20T00:00:00' AND TO_DATE(MFG_DT, 'yyyyMMdd') >= TO_DATE('17530101','yyyyMMdd')

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*)  from delta.`/mnt/e2ecdll0_l2objects/l2objectscore/prodATP/dna_md_cdm_delivery/` where l2_upt_ between '2021-08-11T00:00:00' and '2021-08-16T09:00:00' 

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from delta.`/mnt/e2ecdll0_l2objects/l2objectscore/prodATP/dna_md_cdm_delivery/` where l2_upt_ between '2021-08-16T00:00:00' and '2021-08-16T12:00:00' and SRC_SYS_CD ='BBL';

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct l0_upt_ from delta.`/mnt/e2ecdll0_l2objects/l2objectscore/prodATP/dna_md_cdm_delivery/` where l2_upt_ between '2021-08-16T00:00:00' and '2021-08-16T12:00:00' and SRC_SYS_CD ='BBL' order by l0_upt_ asc;

# COMMAND ----------

SRC_SYS_CD,CLI,DLVRY_NO,DLVRY_LN_NO,l0_upt_,l2_upt_,YEAR,SECTR,_deleted_,l0_createTime_,ERDAT,SHP_TO_CUST_NO,CRT_TM,POD_DT,RTE_NO,PLAN_GI_DT,PCK_TM,DLVRY_DT,ACTL_GI_DT,DLVRY_TYP_CD,DLVRY_TYP_DESCN,SHP_BI_RSN,ACTL_SKU_DLVRY_QTY,ACTL_SLS_UNIT_DLVRY_QTY,FACT_DENOM_MEAS,FACT_NUMRTR_MEAS,DLVRY_QTY,MTL_SHRT_DESCN,MTL_MVMT_TYP_CD,SLS_ORD_NO,PICK_CNTL_STS_CD,LN_ITM_CAT_CD,SPL_STK_TYP_CD,PARNT_BTCH_SPLT_CNTR_NO,PARNT_BOM_CNTR_NO,BASE_UOM_CD,BTCH_NO,MFG_DT,EXP_DT,MTL_NO,MTL_MATWA,RECV_PLNT_CD,SLS_ORD_LN_NO,SLS_UOM_CD,SHPG_PLNT_CD as SHPPING_PLNT_CD,SLOC_CD,VEND_BTCH_NO,HDR_DLVRY_BLK_DESCN,SHPG_TYP_CD as SHPPING_TYP_CD,BILL_OF_LDNG_NO,BILL_ICMPT_TOT_STS_CD,BILL_STS_CD,CHG_DT,CNFRM_STS_HDR_CD,CR_CHK_TOT_STS_CD,DLVRY_ICMPT_TOT_STS_CD,DLVRY_STS_HDR_CD,DLVRY_TOT_STS_HDR_CD,EXTRNL_DLVRY_NO,GM_ICMPT_TOT_STS_CD,GM_TOT_STS_CD,IN_PLNT_IND,ICMPT_TOT_STS_CD,INTCO_BILL_TOT_STS_CD,INVC_LIST_STS_CD,ORD_BILL_STS_HDR_CD,PACK_ICMPT_TOT_STS_CD,PACK_TOT_STS_CD,PICK_CNFRM_STS_HDR_CD,PICK_ICMPT_TOT_STS_CD,PICK_TOT_STS_CD,PSTNG_BILL_STS_CD,PRCS_ORD_NO,PRCSG_TOT_STS_HDR_CD,REF_DOC_STS_CD,REF_DOC_TOT_STS_CD,REJ_TOT_STS_CD,SLS_ORD_CAT_CD,SHPG_COND_CD as SHPPING_COND_CD,SHPG_PT_NO as SHPPING_PT_NO,SOLD_TO_CUST_NO,SUP_NO,TOT_PKGS_CNT,TRNSP_PLAN_STS_CD,WM_TOT_STS_CD,DOC_DEL_ID,CNFRM_STS_ITM_CD,DLVRY_BILL_STS_CD,DLVRY_CMPLT_IND,DLVRY_ICMPT_STS_CD,DLVRY_STS_ITM_CD,DLVRY_TOT_STS_ITM_CD,GM_ICMPT_STS_CD,GM_STS_CD,PACK_ICMPT_STS_CD,ICMPT_STS_CD,INTCO_BILL_STS_CD,ORD_BILL_STS_ITM_CD,PACK_STS_CD,PICK_CNFRM_STS_ITM_CD,PICK_ICMPT_STS_CD,PICK_STS_CD,PRC_ICMPT_STS_CD,PRCSG_TOT_STS_ITM_CD,REF_STS_CD,REF_TOT_STS_CD,REJ_STS_CD,WM_STS_CD,TOT_WT,WT_UOM,VOLUME,VOL_UOM,GROSS_WT,DEFT_DLVRY_BLK,DLVRY_HDR_BLK_CONF,REJ_RSN,DLVRY_BLK,MTL_AVL_DT,CUMM_BTCH_QTY,PO_NO,BILL_ICMPT_STS_CD,SHPG_RGN_CD as SHPPING_RGN_CD,MFG_PLNT_CD,CRTD_BY,PARTIAL_DLVRY,LOAD_DT,TRANSP_PNG_DT,PICK_DT,BILL_DT,NET_WT,DOC_CURR,REQ_DLVRY_DT,RELEASE_DT,REF_DOC_NO,ORD_COMBO_IND,EXP_IND,BLK_DOC_HD,MEANS_OF_TRANS_TYP,COND_PRIC_UNIT,COND_UNIT,FXD_SHPG_PROS_TIM as FXD_SHP_PROS_TIM,VAR_SHP_PROS_TIM,CONSM_POSTING,IND_UNLIMITED_OVR_DELIV,POINT_OF_DELIV,DIVISION,SD_DOC_CAT,MATERIAL_GRP,SUBTOT_DELIV_ITM,NET_VAL_DELIV_ITM,COST_DELIV_ITM,REG

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from delta.`/mnt/e2ecdll0_l2objects/l2objectscore/prodATP/dna_md_cdm_delivery/` where l0_upt_ between '2020-11-13T00:00:00' and '2020-11-14T21:43:06' and l2_upt_ between '2021-08-16T00:00:00' and '2021-08-16T12:00:00'

# COMMAND ----------

spark.sql(f"""CREATE TABLE if not exists reporting.reporting_metadata (
starttime timestamp,
endtime timestamp,
lastsuccefulruntime timestamp
) USING DELTA Location '/mnt/e2ecdll0_l2objects/reporting/reporting_metadata'
PARTITIONED BY (
      module string,
      sector string
)""")

# COMMAND ----------



# COMMAND ----------


# Databricks notebook source
# MAGIC %run ../../Config/SystemConfig

# COMMAND ----------

# MAGIC %run ../../Common/CommonUtils

# COMMAND ----------

# MAGIC %run ./reporting_metadata_reporting

# COMMAND ----------

environment = dbutils.widgets.get("environment")

# COMMAND ----------

l2Path = SystemConfig.getL2Path(environment)
atpPath = SystemConfig.getATPPath(environment)
filteredOpenOrdersPath = "{0}{1}".format(atpPath, "filteredNonOpenOrders")
openOrdersPath = "{0}{1}".format(atpPath, "openOrders")
print(environment,l2Path,atpPath,filteredOpenOrdersPath,openOrdersPath,sep="\n")

# COMMAND ----------

# MAGIC %py
# MAGIC def Database():
# MAGIC   synServer=dbutils.secrets.get(scope="atpdev", key="synapsesqlserver")
# MAGIC   synDB=dbutils.secrets.get(scope="atpdev", key="synDatabase")
# MAGIC   synUser=dbutils.secrets.get(scope="atpdev", key="synUserWithServer")
# MAGIC   synPwd=dbutils.secrets.get(scope="atpdev", key="synPassword")
# MAGIC   dwJdbcExtraOptions = "encrypt=true;trustServerCertificate=true;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
# MAGIC   sqlDwUrl = "jdbc:sqlserver://" + synServer + ":1433" + ";database=" + synDB + ";user=" + synUser+";password=" + synPwd + ";$dwJdbcExtraOptions"
# MAGIC   sqlDwUrlSmall = "jdbc:sqlserver://" + synServer + ":" + "1433" + ";database=" + synDB + ";user=" + synUser+";password=" + synPwd
# MAGIC   return sqlDwUrlSmall
# MAGIC 
# MAGIC 
# MAGIC def Blobstorage():
# MAGIC   synStorage=dbutils.secrets.get(scope="atpdev", key="blobStorage")
# MAGIC   synContainer=dbutils.secrets.get(scope="atpdev", key="blobContainer")
# MAGIC   synAccesskey=dbutils.secrets.get(scope="atpdev", key="blobAccessKey")
# MAGIC   tempDir = "wasbs://" + synContainer + "@" + synStorage +"/atp/tempDirs"
# MAGIC   acntInfo = "fs.azure.account.key."+ synStorage
# MAGIC   spark.conf.set(  "fs.azure.account.key."+"e2ecdll0"+".blob.core.windows.net",   synAccesskey)
# MAGIC   spark.conf.set("spark.sql.parquet.writeLegacyFormat",True)
# MAGIC   return tempDir
# MAGIC 
# MAGIC 
# MAGIC def truncateTable(tableName):
# MAGIC   synServer=dbutils.secrets.get(scope="atpdev", key="synapsesqlserver")
# MAGIC   synDB=dbutils.secrets.get(scope="atpdev", key="synDatabase")
# MAGIC   synUser=dbutils.secrets.get(scope="atpdev", key="synUserWithServer")
# MAGIC   synPwd=dbutils.secrets.get(scope="atpdev", key="synPassword")
# MAGIC   cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+synServer+';DATABASE='+synDB+';UID='+synUser+';PWD='+ synPwd)
# MAGIC   cursor = cnxn.cursor()
# MAGIC   cnxn.autocommit = True
# MAGIC   cnxn.execute("truncate table {}".format(tableName))

# COMMAND ----------

def SP_call(module):
  synServer=dbutils.secrets.get(scope="atpdev", key="synapsesqlserver")
  synDB=dbutils.secrets.get(scope="atpdev", key="synDatabase")
  synUser=dbutils.secrets.get(scope="atpdev", key="synUserWithServer")
  synPwd=dbutils.secrets.get(scope="atpdev", key="synPassword")
  
  cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+synServer+';DATABASE='+synDB+';UID='+synUser+';PWD='+ synPwd)
  cursor = cnxn.cursor()
  cnxn.autocommit = True
  cnxn.execute(f"SP_ATP_Merge_{module}")

# COMMAND ----------

import pyodbc
from pyspark.sql.functions import to_timestamp
import time
def pushDataSnap(module):                        #'D3algooutput' module = 6
  try:
    startEndTimeofl2objDict=fetchLastSuccessfulRunTime(module)
#     startDateTime = '2021-08-19T00:00:00'
    startDateTime = startEndTimeofl2objDict["startDate"]
#     endDateTime = '2021-08-20T00:00:00'
    endDateTime = startEndTimeofl2objDict["endDate"]     
    sqlDwUrlSmall = Database()    
    tempDir = Blobstorage()       
                                                                                       #getD4Query('2021-08-19T00:00:00','2021-08-20T00:00:00')
                                                                                      #getD4Query(startDate,endDate)
    DestinationSourceMappingDict = {'D4AlgoOutput':['atp_stage.D4AlgoOutput','append',getD4Query(startDateTime,endDateTime)],'d3algooutput':['atp_stage.d3algooutput','append',getD3Query(startDateTime,endDateTime)],'openOrders':['atp_stage.openOrders','append',getOpenOrderQuery(startDateTime,endDateTime)]}
    
    tableName = DestinationSourceMappingDict[module][0]
    mode = DestinationSourceMappingDict[module][1]
    sqlQuery = DestinationSourceMappingDict[module][2]
    
    
    truncateTable(tableName)
    descDf = spark.sql(sqlQuery)   # Source Dataframe in databricks
    print(descDf.count())
    
    time.sleep(5)
    descDf.write.format("com.databricks.spark.sqldw") \
    .option("url", sqlDwUrlSmall) \
    .option("dbtable", tableName) \
    .option("forward_spark_azure_storage_credentials", "True") \
    .option("tempdir", tempDir) \
    .mode(mode) \
    .save()
    
    #SP call
    SP_call(module)
    currentSystemTime_end = datetime.now()
    endTableTime = currentSystemTime_end.strftime("%Y-%m-%dT%H:%M:%S")
    updateMetaData(module,"success",endTableTime)
  except Exception as errorMsg:
    print('error in load-objects')
    print(errorMsg)
    currentSystemTime_end = datetime.now()
    endTableTime = currentSystemTime_end.strftime("%Y-%m-%dT%H:%M:%S")
    updateMetaData(module,"failed",endTableTime)
    sys.exit(1)


# COMMAND ----------

def getOpenOrderQuery(startDate,endDate):
  openOrdersqlQuery=f"""select SRC_SYS_CD
        ,executionId
        ,SLS_ORD_NO
        ,SLS_ORD_LN_NO
        ,SCHD_LN_NO
        ,CRT_DT
        ,SO_SCHD_DELIV_DT
        ,SO_SCHD_GOODS_ISSUE_DT
        ,SO_SCHD_MTL_AVLB_DT
        ,cast(obdCollection as string) as obdCollection
        ,PLNT_CD
        ,SLS_ORD_TYP_CD
        ,MTL_NO
        ,MTL_TYPE_CD
        ,LN_ITM_CAT_CD
        ,REJ_RSN_CD
        ,SLS_ORD_QTY
        ,SO_SCHD_SO_ORD_QTY
        ,SO_SCHD_CONF_QTY
        ,REQ_QTY_SKU
        ,ACTL_SKU_DLVRY_QTY_SUM
        ,cumulativeSumConfQty
        ,deliveredQtyBySchdLine
        ,deliveryStatus
        ,actualShipDate
        ,IsVirtualSchdLine
        ,createdTime
        ,updatedTime
        ,isAllScheduleLinesCompleted
        ,isSoftAllocated
        ,isHardAllocated  from delta.`/mnt/e2ecdll0_l2objects/atp/openOrders` where createdTime between '{startDate}' and '{endDate}' """
#   from delta.`{openOrdersPath}`
  return openOrdersqlQuery



# COMMAND ----------

def getD4Query(startDate,endDate):
  D4sqlQuery=f"""SELECT 
  SRC_SYS_CD 
  ,executionId
  ,jobType
  ,SLS_ORD_NO
  ,SLS_ORD_LN_NO
  ,SCHD_LN_NO
  ,CRT_DT
  ,orderCreatedTime
  ,createdTime
  ,customerRDD
  ,shipToCountry
  ,shipToCustomerCode
  ,route
  ,pickPackTime
  ,transitTime
  ,cal_identity
  ,LoadingGroup
  ,HoldLeadTime
  ,case WHEN customData.calculatedDeliveryDate  IS null THEN null ELSE customData.customData.expectedMAD END as expectedMAD
  ,case WHEN customData.calculatedDeliveryDate  IS null THEN null ELSE to_timestamp(customData.customData.1_orderCutOffDay) END as 1_orderCutOffDay
  ,case WHEN customData.calculatedDeliveryDate  IS null THEN null ELSE cast(customData.customData.1_dateAfterCutOffTime as decimal) END as 1_dateAfterCutOffTime
  ,case WHEN customData.calculatedDeliveryDate  IS null THEN null ELSE cast(customData.customData.1_orderCutOffTime as decimal) END as 1_orderCutOffTime
  ,case WHEN customData.calculatedDeliveryDate  IS null THEN null ELSE cast(customData.customData.1_isCutOffTimeApply as boolean) END as 1_isCutOffTimeApply 
  ,case WHEN customData.calculatedDeliveryDate  IS null THEN null ELSE to_timestamp(customData.customData.1_dateAfterCutOffDays) END as 1_dateAfterCutOffDays
  ,case WHEN customData.calculatedDeliveryDate IS null THEN null ELSE to_timestamp(customData.customData.1_orderCutOffFinalDate)  END as 1_orderCutOffFinalDate
  ,case WHEN customData.calculatedDeliveryDate IS null THEN null ELSE to_timestamp(customData.customData.2_ExpectedCleanOrderDate) END as 2_ExpectedCleanOrderDate
  ,case WHEN customData.calculatedDeliveryDate  IS null THEN null ELSE to_timestamp(customData.customData.2_DateAfterholdAndBlockLeadTime) END as 2_DateAfterholdAndBlockLeadTime
  ,case WHEN customData.calculatedDeliveryDate IS null THEN null ELSE cast(customData.customData.2_HoldAndBlockLeadTime as decimal) END as 2_HoldAndBlockLeadTime
  ,case WHEN customData.calculatedDeliveryDate  IS null THEN null ELSE to_timestamp(customData.customData.3_calculatedDateAfterPickPackLeadTime) END as 3_calculatedDateAfterPickPackLeadTime
  ,case WHEN customData.calculatedDeliveryDate IS null THEN null ELSE cast(customData.customData.3_calculatedSapPickPackLeadTime as decimal) END as 3_calculatedSapPickPackLeadTime
  ,case WHEN customData.calculatedDeliveryDate IS null THEN null ELSE cast(customData.customData.3_calculatedProductLeadTime as decimal) END as 3_calculatedProductLeadTime
  ,case WHEN customData.calculatedDeliveryDate IS null THEN null ELSE to_timestamp(customData.customData.3_calculatedShippingDate) END as 3_calculatedShippingDate
  ,case WHEN customData.calculatedDeliveryDate  IS null THEN null ELSE cast(customData.customData.3_calculatedDocLeadTime as decimal) END as 3_calculatedDocLeadTime
  ,case WHEN customData.calculatedDeliveryDate  IS null THEN null ELSE to_timestamp(customData.customData.3_calculatedPickpackProcessDate) END as 3_calculatedPickpackProcessDate
  ,case WHEN customData.calculatedDeliveryDate  IS null THEN null ELSE cast(customData.customData.3_calculatedBufferLeadTime as decimal) END as 3_calculatedBufferLeadTime
  ,case WHEN customData.calculatedDeliveryDate  IS null THEN null ELSE cast(customData.customData.3_calculatedPickPackLeadTime as decimal) END as 3_calculatedPickPackLeadTime
  ,case WHEN customData.calculatedDeliveryDate  IS null THEN null ELSE to_timestamp(customData.customData.4_calculatedDateAfterTransitLeadTime) END as 4_calculatedDateAfterTransitLeadTime
  ,case WHEN customData.calculatedDeliveryDate  IS null THEN null ELSE to_timestamp(customData.customData.4_calculatedTransitProcessDate) END as 4_calculatedTransitProcessDate
  ,case WHEN customData.calculatedDeliveryDate  IS null THEN null ELSE cast(customData.customData.4_calculatedTransitLeadTime as decimal) END as 4_calculatedTransitLeadTime
  ,case WHEN customData.calculatedDeliveryDate  IS null THEN null ELSE to_timestamp(customData.customData.4_calculatedDeliveryDate) END as 4_calculatedDeliveryDate
  ,case WHEN customData.calculatedDeliveryDate  IS null THEN null ELSE cast(customData.customData.5_expectedDocLeadTime as decimal) END as 5_expectedDocLeadTime
  ,case WHEN customData.calculatedDeliveryDate  IS null THEN null ELSE cast(customData.customData.5_expectedProductLeadTime as decimal) END as 5_expectedProductLeadTime
  ,case WHEN customData.calculatedDeliveryDate  IS null THEN null ELSE to_timestamp(customData.customData.5_expectedShippingDate) END as 5_expectedShippingDate
  ,case WHEN customData.calculatedDeliveryDate  IS null THEN null ELSE cast(customData.customData.5_expectedPickPackLeadTime as decimal) END as 5_expectedPickPackLeadTime
  ,case WHEN customData.calculatedDeliveryDate  IS null THEN null ELSE to_timestamp(customData.customData.5_expectedDateAfterPickPackLeadTime) END as 5_expectedDateAfterPickPackLeadTime
  ,case WHEN customData.calculatedDeliveryDate  IS null THEN null ELSE to_timestamp(customData.customData.5_expectedPickpackProcessDate) END as 5_expectedPickpackProcessDate
  ,case WHEN customData.calculatedDeliveryDate  IS null THEN null ELSE cast(customData.customData.5_expectedBufferLeadTime as decimal) END as 5_expectedBufferLeadTime
  ,case WHEN customData.calculatedDeliveryDate  IS null THEN null ELSE cast(customData.customData.5_expectedSapPickPackLeadTime as decimal) END as 5_expectedSapPickPackLeadTime
  ,case WHEN customData.calculatedDeliveryDate  IS null THEN null ELSE to_timestamp(customData.customData.6_expectedDateAfterTransitLeadTime) END as 6_expectedDateAfterTransitLeadTime
  ,case WHEN customData.calculatedDeliveryDate  IS null THEN null ELSE cast(customData.customData.6_expectedTransitLeadTime as decimal) END as 6_expectedTransitLeadTime
  ,case WHEN customData.calculatedDeliveryDate IS null THEN null ELSE to_timestamp(customData.customData.6_expectedDeliveryDate) END as 6_expectedDeliveryDate
  ,case WHEN customData.calculatedDeliveryDate IS null THEN null ELSE to_timestamp(customData.customData.6_expectedTransitProcessDate) END as 6_expectedTransitProcessDate
  ,case WHEN customData.calculatedDeliveryDate  IS null THEN null ELSE to_timestamp(customData.customData.7_recommendedPickpackProcessDate) END as 7_recommendedPickpackProcessDate
  ,case WHEN customData.calculatedDeliveryDate  IS null THEN null ELSE cast(customData.customData.7_recommendedDocLeadTime as decimal) END as 7_recommendedDocLeadTime
  ,case WHEN customData.calculatedDeliveryDate  IS null THEN null ELSE cast(customData.customData.7_recommendedBufferLeadTime as decimal) END as 7_recommendedBufferLeadTime
  ,case WHEN customData.calculatedDeliveryDate  IS null THEN null ELSE to_timestamp(customData.customData.7_recommendedDateAfterPickPackLeadTime) END as 7_recommendedDateAfterPickPackLeadTime
  ,case WHEN customData.calculatedDeliveryDate  IS null THEN null ELSE cast(customData.customData.7_recommendedProductLeadTime as decimal) END as 7_recommendedProductLeadTime
  ,case WHEN customData.calculatedDeliveryDate  IS null THEN null ELSE cast(customData.customData.7_recommendedSapPickPackLeadTime as decimal) END as 7_recommendedSapPickPackLeadTime
  ,case WHEN customData.calculatedDeliveryDate  IS null THEN null ELSE cast(customData.customData.7_recommendedPickPackLeadTime as decimal) END as 7_recommendedPickPackLeadTime
  ,case WHEN customData.calculatedDeliveryDate  IS null THEN null ELSE to_timestamp(customData.customData.7_recommendedShippingDate) END as 7_recommendedShippingDate
  ,case WHEN customData.calculatedDeliveryDate  IS null THEN null ELSE to_timestamp(customData.customData.8_recommendedDateAfterTransitLeadTime) END as 8_recommendedDateAfterTransitLeadTime
  ,case WHEN customData.calculatedDeliveryDate  IS null THEN null ELSE to_timestamp(customData.customData.8_recommendedTransitProcessDate) END as 8_recommendedTransitProcessDate
  ,case WHEN customData.calculatedDeliveryDate  IS null THEN null ELSE to_timestamp(customData.customData.8_recommendedDeliveryDate) END as 8_recommendedDeliveryDate
  ,case WHEN customData.calculatedDeliveryDate  IS null THEN null ELSE cast(customData.customData.8_recommendedTransitLeadTime as decimal) END as 8_recommendedTransitLeadTime
  ,case WHEN customData.calculatedDeliveryDate  IS null THEN null ELSE to_timestamp(customData.customData.expectedDeliveryDate) END as expectedDeliveryDate
  ,case WHEN customData.calculatedDeliveryDate IS null THEN null ELSE to_timestamp(calculatedDeliveryDate) END as calculatedDeliveryDate
  ,case WHEN customData.calculatedDeliveryDate  IS null THEN null ELSE to_timestamp(recommendedShippingDate) END as recommendedShippingDate
  ,case WHEN customData.calculatedDeliveryDate  IS null THEN null ELSE to_timestamp(recommendedMAD) END as recommendedMAD
  ,case WHEN customData.calculatedDeliveryDate  IS null THEN null ELSE to_timestamp(recommendedDD) END as recommendedDD
  
  ,case WHEN customData.calculatedDeliveryDate  IS null THEN null ELSE to_timestamp(customData.customData.9_recommendedDeliveryDate) END as 9_recommendedDeliveryDate
  ,case WHEN customData.calculatedDeliveryDate  IS null THEN null ELSE to_timestamp(customData.customData.9_recommendedTransitProcessDate) END as 9_recommendedTransitProcessDate
  ,case WHEN customData.calculatedDeliveryDate  IS null THEN null ELSE cast(customData.customData.9_recommendedTransitLeadTime as decimal) END as 9_recommendedTransitLeadTime
  ,case WHEN customData.calculatedDeliveryDate  IS null THEN null ELSE to_timestamp(customData.customData.9_recommendedDateAfterTransitLeadTime) END as 9_recommendedDateAfterTransitLeadTime
   
  FROM atpdev.D4AlgoOutput where createdTime  between '{startDate}' and '{endDate}'  """
  return D4sqlQuery




# COMMAND ----------

def getD3Query(startDate,endDate):
  D3sqlQuery=f"""SELECT 
  SRC_SYS_CD 
  ,executionId
  ,jobType
  ,SLS_ORD_NO
  ,SLS_ORD_LN_NO
  ,SCHD_LN_NO
  ,to_date(CRT_DT) as CRT_DT
  ,orderCreatedTime
  ,to_date(d3MadDate) as d3MadDate
  ,to_date(customerRDD) as customerRDD
  ,shipToCountry
  ,shipToCustomerCode
  ,route
  ,cal_identity
  ,LoadingGroup
  ,pickPackTime
  ,transitTime
  ,isOrderFullfilled
  ,case WHEN customData.recommendedDeliveryDate  IS null THEN null ELSE cast(customData.customData.1_ExpectedPickPackLeadTime as decimal) END as 1_ExpectedPickPackLeadTime
  ,case WHEN customData.recommendedDeliveryDate  IS null THEN null ELSE cast(customData.customData.1_ExpectedBufferLeadTime as decimal) END as 1_ExpectedBufferLeadTime
  ,case WHEN customData.recommendedDeliveryDate IS null THEN null ELSE to_timestamp(customData.customData.1_ExpectedShippingDate)  END as 1_ExpectedShippingDate
  ,case WHEN customData.recommendedDeliveryDate  IS null THEN null ELSE cast(customData.customData.1_ExpectedDocLeadTime as decimal) END as 1_ExpectedDocLeadTime
  ,case WHEN customData.recommendedDeliveryDate  IS null THEN null ELSE cast(customData.customData.1_ExpectedProductLeadTime as decimal) END as 1_ExpectedProductLeadTime
  ,case WHEN customData.recommendedDeliveryDate  IS null THEN null ELSE cast(customData.customData.1_ExpectedSapPickPackLeadTime as decimal) END as 1_ExpectedSapPickPackLeadTime
  ,case WHEN customData.recommendedDeliveryDate IS null THEN null ELSE to_timestamp(customData.customData.1_ExpectedDateAfterPickPackLeadTime)  END as 1_ExpectedDateAfterPickPackLeadTime
  ,case WHEN customData.recommendedDeliveryDate IS null THEN null ELSE to_timestamp(customData.customData.1_ExpectedPickpackProcessDate)  END as 1_ExpectedPickpackProcessDate
  ,case WHEN customData.recommendedDeliveryDate IS null THEN null ELSE to_timestamp(customData.customData.2_ExpectedTransitProcessDate)  END as 2_ExpectedTransitProcessDate
  ,case WHEN customData.recommendedDeliveryDate IS null THEN null ELSE to_timestamp(customData.customData.2_ExpectedDeliveryDate)  END as 2_ExpectedDeliveryDate
  ,case WHEN customData.recommendedDeliveryDate IS null THEN null ELSE to_timestamp(customData.customData.2_ExpectedDateAfterTransitLeadTime)  END as 2_ExpectedDateAfterTransitLeadTime
  ,case WHEN customData.recommendedDeliveryDate  IS null THEN null ELSE cast(customData.customData.2_ExpectedTransitLeadTime as decimal) END as 2_ExpectedTransitLeadTime
  ,case WHEN customData.recommendedDeliveryDate IS null THEN null ELSE to_timestamp(customData.customData.3_RecommendedDeliveryDate)  END as 3_RecommendedDeliveryDate
  ,case WHEN customData.recommendedDeliveryDate IS null THEN null ELSE to_timestamp(customData.customData.3_RecommendedTransitProcessDate)  END as 3_RecommendedTransitProcessDate
  ,case WHEN customData.recommendedDeliveryDate IS null THEN null ELSE to_timestamp(customData.customData.3_RecommendedDateAfterTransitLeadTime)  END as 3_RecommendedDateAfterTransitLeadTime
  ,case WHEN customData.recommendedDeliveryDate  IS null THEN null ELSE cast(customData.customData.3_RecommendedTransitLeadTime as decimal) END as 3_RecommendedTransitLeadTime
  ,case WHEN customData.recommendedDeliveryDate  IS null THEN null ELSE  customData.customData.Factorykey   END as Factorykey
  ,recommendedMaterialAvailabilitydate
  ,recommendedShippingDate
  ,recommendedDeliveryDate
  ,expectedDeliveryDate
  ,createdTime 
  FROM atpdev.d3algooutput where createdTime  between '{startDate}' and '{endDate}'  
  """
  return D3sqlQuery




# COMMAND ----------

pushDataSnap('D4AlgoOutput')

# COMMAND ----------

from multiprocessing.pool import ThreadPool
import multiprocessing as mp
import time


inputs=['d3algooutput','openOrders' ,'D4AlgoOutput']
cnt = mp.cpu_count()   
if cnt < 3:
  splits = 1
else:
  splits = cnt - 1        
    
pool=ThreadPool(splits)
pool.map(pushDataSnap,inputs)        # pushDataSnap('d3algooutput')   ----->    pushDataSnap(module)




# COMMAND ----------

dbutils.notebook.exit("SUCCESS")

# COMMAND ----------

# MAGIC %sql
# MAGIC --insert into reporting.reporting_metadata_atp values('','D4AlgoOutput','NA','','2005-01-01T00:00:00');
# MAGIC --insert into reporting.reporting_metadata_atp values('','d3algooutput','NA','','2005-01-01T00:00:00');
# MAGIC --insert into reporting.reporting_metadata_atp values('','openOrders','NA','','2005-01-01T00:00:00');
# MAGIC select * from reporting.reporting_metadata_atp

# COMMAND ----------

# MAGIC %sql
# MAGIC desc Atpdev.D4AlgoOutput
# MAGIC --select customData from  Atpdev.D4AlgoOutput limit 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select customData from  Atpdev.D4AlgoOutput limit 1

# COMMAND ----------

# MAGIC %sql
# MAGIC --select HoldLeadTime from  Atpdev.D4AlgoOutput
# MAGIC describe table Atpdev.D4AlgoOutput

# COMMAND ----------

3_calculatedPickpackProcessDate
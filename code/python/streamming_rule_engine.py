#################################################################################################################################################################
#
# SimpleStreamingRuleEngine- This is simple rule engine for streaming data.
# This version is compatible with Spark 1.x and python 2.x (tested with Spark 1.6 and python 2.7)
#
# @author Neeraj Maheshwari
#
#################################################################################################################################################################

import sys

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.sql import HiveContext, SQLContext

def ruleEngine(rdd, rule_config_db, rule_config_tbl, invalid_rec_db, invalid_rec_tbl):
	#Initiate rule_engine if we get atleast one record
	if(rdd.count() > 0):
		#parse json data in dataframe
		parsed_data = sqlContext.read.json(rdd)
		#register dataframe in temp table
		parsed_data.registerTempTable('source_data')
		
		#reading rules from hive table
		rules = sqlContext.table('{0}.{1}'.format(rule_config_db, rule_config_tbl))
		rules.cache()
		
		#extract rules in list
		rules_list = rules.collect()
		
		#getting field details from source data
		src_columns = parsed_data.columns
		#Converting source columns in string
		src_col_str = ' ,'.join(src_columns)
		
		#bulding SQL query for input rules
		sql = ''
		for rule in rules_list:
			sql = sql + 'WHEN ' + rule.rule_expr + " THEN '" + rule.rule_description + "' "
		sql = 'CASE ' + sql + 'END AS reason_flag'
		final_sql = src_col_str + ', ' + sql
		
		#applying SQL query on parsed json data
		df_src = sqlContext.sql("select {0} from source_data".format(final_sql))
		
		#joining source data with rules to get full information
		df_full_data = df_src.join(rules, df_src.reason_flag == rules.rule_description, 'left_outer')
		df_full_data.cache()
		
		#extracting table details for invalid records
		invalid_table_columns = sqlContext.table('{0}.{1}'.format(invalid_rec_db, invalid_rec_tbl)).columns
		
		#filtring invalid records from data
		df_invalid = df_full_data.where("reason_flag is not null and log_level = 'error'").select(invalid_table_columns)
		df_invalid.cache()
		
		#displaying first 100 invalid records on console
		df_invalid.show(100)
		#dumping invalid records in respective hive table
		df_invalid.write.insertInto('{0}.{1}'.format(invalid_rec_db, invalid_rec_tbl))
		
		#unpersisting df_invalid dataframe as it have no further use
		df_invalid.unpersist()
		
		#similarly we can extract valid records set and persist data in respective table by extending ruleEngine definition
		#df_valid = df_full_data.where("reason_flag is null or log_level = 'warn'")
		
		#unpersisting df_full_data and rules dataframes
		df_full_data.unpersist()
		rules.unpersist()

if __name__ == "__main__":
	
	#initilizing SparkConf
	conf = SparkConf().setAppName('Streaming Rule Engine')
	
	#reading property file
	try:
		batch_time = int(conf.get('spark.ruleEngine.batchTimeInSec'))
	except ValueError:
		print 'ERROR: batch time must be integer value, found string'
		exit()
	file_dir_path = conf.get('spark.ruleEngine.streamingFilesDirath')
	rule_config_db_name = conf.get('spark.ruleEngine.ruleConfigDbName')
	rule_config_tbl_name = conf.get('spark.ruleEngine.ruleConfigTableName')
	invalid_rec_db_name = conf.get('spark.ruleEngine.invalidRecordDbName')
	invalid_rec_tbl_name = conf.get('spark.ruleEngine.invalidRecordTableName')
	
	#initilizing spark context, streaming context and hive enabled sql context
	sc = SparkContext(conf = conf)
	ssc = StreamingContext(sc, batch_time)
	sqlContext = HiveContext(sc)
	
	#reading files from input file dir (HDFS Loaction)
	data = ssc.textFileStream(file_dir_path)
	
	#invoking ruleEngine on input records
	data.foreachRDD(lambda rdd: ruleEngine(rdd, rule_config_db_name, rule_config_tbl_name, invalid_rec_db_name, invalid_rec_tbl_name))
	
	#starting streaming data processing
	ssc.start()
	#wait for the computation to terminate
	ssc.awaitTermination()
# SimpleStreamingRuleEngine

This application is a rule engine for streaming data. It is implemented to read streaming files from user specified HDFS location and apply set of rules defined by user in rule configuration table. In current implementation:

Rule Configuration Table: 
	A rule_config table is created in hive in which user can insert new rules at any point of time. For more advance application either rule_config table should present in some RDBMS or it should be an ORC hive table with enabled ACID properties so that user can update any existing rule(s).

Rules: 
	User can specify any number of rules for input messages in rule_config table. Along with each rule user have to provide an equivalent expression for that rule for instance, for a rule 'ATL2 value should never be LOW' equivalent expression would be "signal = 'ATL2' and lower(value_type) = 'string' and lower(value) = 'low'". Also user should specify log_level for each rule. Current implementation supports two log_levels error and warn. In case of error record will get dropped from source data and will be redirected to a separate table for such invalid records and a dashboard can point to this table to generate alerts. Whereas in case of warn log level record will not get dropped from dataset but it will have warning flag, this warning flag could to helpful to generate early warning messages for given source which may go in danger in near future. For example for a source ATL1 value is continuously increasing and 250 represent a critical condition of this source so we can start warning when it value crosses 200 or 210 mark.
	As in current implementation user need to provide expression for each rule, for better user experience we can have a static parser implementation that convert text rules in expressions.

Streaming Data Source: 
	Current application supports reading data files from user specified HDFS location, once application is started it will continuously monitor specified location for new files & as and when new file(s) arrive it starts processing input messages from file(s), if no file arriving at specified location app will run in idle state. To evolve application at next level we can add feature to read data from multiple streaming data sources like Kafka queue, socket connections etc. user just need to provide data source name as an argument and app will automatically configure and start for that data source.
	
Data Sink:
	Current application writing dumping data in hive tables which is not a right candidate to handle streaming data. We can extend application to write data in some time-series databases like influxdb, open TSDB, Cassandra etc. using either available connector between spark and database or by implementing a custom connector.
	
Perforance:
	In testing application was successfully processing input file(present in test data folder) in a batch of 5 seconds.

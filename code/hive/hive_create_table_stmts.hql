--RULE_CONFIG TABLE
CREATE EXTERNAL TABLE <<db_name>>.rule_config(
 rule_expr string COMMENT 'rule in exression fromat',
 rule_description string COMMENT 'rule descrition',
 log_level string COMMENT 'rule logging level error / warn')
COMMENT 'This is rule configuration table for streaming rule engine';


--INVALID_RECORD_TABLE
CREATE EXTERNAL TABLE <<db_name>>.invalid_record_table(
 signal string,
 value string,
 reason_flag string)
COMMENT 'This table contains invalid records dropped from streaming rule engine';


CREATE TABLE `tmp_tos.vsa_nd_nds_r1`(
  `code_period` string, 
  `code_present_place` string, 
  `date_receipt` date, 
  `fid` decimal(38,0), 
  `s40` decimal(38,0), 
  `year` decimal(38,0), 
  `quarter` decimal(38,0) 
)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'

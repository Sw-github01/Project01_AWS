CREATE EXTERNAL TABLE IF NOT EXISTS `stdi-lake-house-prjct01`.`accelerometer_landing` (
  `timeStamp` timestamp,
  `X` float,
  `Y` float,
  `user` string
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://stdi-lake-house-prjct01/accelerometer/landing/'
TBLPROPERTIES ('classification' = 'json');
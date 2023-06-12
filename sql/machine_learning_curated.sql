CREATE EXTERNAL TABLE IF NOT EXISTS `stdi-lake-house-prjct01`.`machine_learning_curated` (
  `phone` string,
  `birthDay` string,
  `serialNumber` string,
  `registrationDate` bigint,
  `lastUpdateDate` bigint,
  `shareWithResearchAsOfDate` bigint,
  `shareWithPublicAsOfDate` bigint,
  `X` double,
  `Y` double,
  `Z` double,
  'timeStamp' bigint
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'ignore.malformed.json' = 'FALSE',
    'dots.in.keys' = 'FALSE',
    'case.insensitive' = 'TRUE',
    'mapping' = 'TRUE'
)
         STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://stdi-lake-house-prjct01/machine_learning_curated'
TBLPROPERTIES ('classification' = 'json');
-- opendental_analytics_opendentalbackup_01_03_2025.zipcode definition

CREATE TABLE `zipcode` (
  `ZipCodeNum` bigint(20) NOT NULL AUTO_INCREMENT,
  `ZipCodeDigits` varchar(20) DEFAULT '',
  `City` varchar(100) DEFAULT '',
  `State` varchar(20) DEFAULT '',
  `IsFrequent` tinyint(3) unsigned NOT NULL DEFAULT 0,
  PRIMARY KEY (`ZipCodeNum`)
) ENGINE=MyISAM AUTO_INCREMENT=1303 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_uca1400_ai_ci;
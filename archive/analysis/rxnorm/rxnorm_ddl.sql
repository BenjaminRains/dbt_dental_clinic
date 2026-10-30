-- opendental_analytics_opendentalbackup_01_03_2025.rxnorm definition

CREATE TABLE `rxnorm` (
  `RxNormNum` bigint(20) NOT NULL AUTO_INCREMENT,
  `RxCui` varchar(255) NOT NULL,
  `MmslCode` varchar(255) NOT NULL,
  `Description` text NOT NULL,
  PRIMARY KEY (`RxNormNum`),
  KEY `RxCui` (`RxCui`)
) ENGINE=MyISAM AUTO_INCREMENT=261119 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_uca1400_ai_ci;
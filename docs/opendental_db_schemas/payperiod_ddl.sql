-- opendental_analytics_opendentalbackup_01_03_2025.payperiod definition

CREATE TABLE `payperiod` (
  `PayPeriodNum` bigint(20) NOT NULL AUTO_INCREMENT,
  `DateStart` date NOT NULL DEFAULT '0001-01-01',
  `DateStop` date NOT NULL DEFAULT '0001-01-01',
  `DatePaycheck` date NOT NULL DEFAULT '0001-01-01',
  PRIMARY KEY (`PayPeriodNum`)
) ENGINE=MyISAM AUTO_INCREMENT=151 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_uca1400_ai_ci;
-- opendental_analytics_opendentalbackup_01_03_2025.feeschedgroup definition

CREATE TABLE `feeschedgroup` (
  `FeeSchedGroupNum` bigint(20) NOT NULL AUTO_INCREMENT,
  `Description` varchar(255) NOT NULL,
  `FeeSchedNum` bigint(20) NOT NULL,
  `ClinicNums` varchar(255) NOT NULL,
  PRIMARY KEY (`FeeSchedGroupNum`),
  KEY `FeeSchedNum` (`FeeSchedNum`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_uca1400_ai_ci;
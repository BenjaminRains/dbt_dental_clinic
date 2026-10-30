-- opendental_analytics_opendentalbackup_01_03_2025.recalltype definition

CREATE TABLE `recalltype` (
  `RecallTypeNum` bigint(20) NOT NULL AUTO_INCREMENT,
  `Description` varchar(255) DEFAULT NULL,
  `DefaultInterval` int(11) NOT NULL,
  `TimePattern` varchar(255) DEFAULT NULL,
  `Procedures` varchar(255) DEFAULT NULL,
  `AppendToSpecial` tinyint(4) NOT NULL,
  PRIMARY KEY (`RecallTypeNum`)
) ENGINE=MyISAM AUTO_INCREMENT=9 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_uca1400_ai_ci;
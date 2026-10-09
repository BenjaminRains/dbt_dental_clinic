-- opendental_analytics_opendentalbackup_01_03_2025.periomeasure definition

CREATE TABLE `periomeasure` (
  `PerioMeasureNum` bigint(20) NOT NULL AUTO_INCREMENT,
  `PerioExamNum` bigint(20) NOT NULL,
  `SequenceType` tinyint(3) unsigned NOT NULL DEFAULT 0,
  `IntTooth` smallint(6) NOT NULL,
  `ToothValue` smallint(6) NOT NULL,
  `MBvalue` smallint(6) NOT NULL,
  `Bvalue` smallint(6) NOT NULL,
  `DBvalue` smallint(6) NOT NULL,
  `MLvalue` smallint(6) NOT NULL,
  `Lvalue` smallint(6) NOT NULL,
  `DLvalue` smallint(6) NOT NULL,
  `SecDateTEntry` datetime NOT NULL DEFAULT '0001-01-01 00:00:00',
  `SecDateTEdit` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  PRIMARY KEY (`PerioMeasureNum`),
  KEY `PerioExamNum` (`PerioExamNum`),
  KEY `SecDateTEntry` (`SecDateTEntry`),
  KEY `SecDateTEdit` (`SecDateTEdit`)
) ENGINE=MyISAM AUTO_INCREMENT=805220 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_uca1400_ai_ci;
-- opendental_analytics_opendentalbackup_01_03_2025.perioexam definition

CREATE TABLE `perioexam` (
  `PerioExamNum` bigint(20) NOT NULL AUTO_INCREMENT,
  `PatNum` bigint(20) NOT NULL,
  `ExamDate` date NOT NULL DEFAULT '0001-01-01',
  `ProvNum` bigint(20) NOT NULL,
  `DateTMeasureEdit` datetime NOT NULL DEFAULT '0001-01-01 00:00:00',
  `Note` text NOT NULL,
  PRIMARY KEY (`PerioExamNum`),
  KEY `PatNum` (`PatNum`)
) ENGINE=MyISAM AUTO_INCREMENT=12576 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_uca1400_ai_ci;
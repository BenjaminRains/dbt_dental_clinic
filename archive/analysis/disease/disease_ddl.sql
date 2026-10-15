-- opendental_analytics_opendentalbackup_02_28_2025.disease definition

CREATE TABLE `disease` (
  `DiseaseNum` bigint(20) NOT NULL AUTO_INCREMENT,
  `PatNum` bigint(20) NOT NULL,
  `DiseaseDefNum` bigint(20) NOT NULL,
  `PatNote` text DEFAULT NULL,
  `DateTStamp` timestamp /* mariadb-5.3 */ NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `ProbStatus` tinyint(4) NOT NULL,
  `DateStart` date NOT NULL DEFAULT '0001-01-01',
  `DateStop` date NOT NULL DEFAULT '0001-01-01',
  `SnomedProblemType` varchar(255) NOT NULL,
  `FunctionStatus` tinyint(4) NOT NULL,
  PRIMARY KEY (`DiseaseNum`),
  KEY `indexPatNum` (`PatNum`),
  KEY `DiseaseDefNum` (`DiseaseDefNum`)
) ENGINE=MyISAM AUTO_INCREMENT=20096 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
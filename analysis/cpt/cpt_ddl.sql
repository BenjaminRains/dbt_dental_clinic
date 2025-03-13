-- opendental_analytics_opendentalbackup_01_03_2025.cpt definition

CREATE TABLE `cpt` (
  `CptNum` bigint(20) NOT NULL AUTO_INCREMENT,
  `CptCode` varchar(255) NOT NULL,
  `Description` varchar(4000) NOT NULL,
  `VersionIDs` varchar(255) NOT NULL,
  PRIMARY KEY (`CptNum`),
  KEY `CptCode` (`CptCode`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_uca1400_ai_ci;
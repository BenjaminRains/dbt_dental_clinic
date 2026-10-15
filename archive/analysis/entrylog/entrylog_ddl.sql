-- opendental_analytics_opendentalbackup_02_28_2025.entrylog definition

CREATE TABLE `entrylog` (
  `EntryLogNum` bigint(20) NOT NULL AUTO_INCREMENT,
  `UserNum` bigint(20) NOT NULL,
  `FKeyType` tinyint(4) NOT NULL,
  `FKey` bigint(20) NOT NULL,
  `LogSource` tinyint(4) NOT NULL,
  `EntryDateTime` datetime /* mariadb-5.3 */ NOT NULL DEFAULT '0001-01-01 00:00:00',
  PRIMARY KEY (`EntryLogNum`),
  KEY `UserNum` (`UserNum`),
  KEY `FKey` (`FKey`),
  KEY `EntryDateTime` (`EntryDateTime`)
) ENGINE=MyISAM AUTO_INCREMENT=44035 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
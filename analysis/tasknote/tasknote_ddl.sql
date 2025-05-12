-- opendental_analytics_opendentalbackup_02_28_2025.tasknote definition

CREATE TABLE `tasknote` (
  `TaskNoteNum` bigint(20) NOT NULL AUTO_INCREMENT,
  `TaskNum` bigint(20) NOT NULL,
  `UserNum` bigint(20) NOT NULL,
  `DateTimeNote` datetime /* mariadb-5.3 */ NOT NULL DEFAULT '0001-01-01 00:00:00',
  `Note` text NOT NULL,
  PRIMARY KEY (`TaskNoteNum`),
  KEY `TaskNum` (`TaskNum`),
  KEY `UserNum` (`UserNum`)
) ENGINE=MyISAM AUTO_INCREMENT=295 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
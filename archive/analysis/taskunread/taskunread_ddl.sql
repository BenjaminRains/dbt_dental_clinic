-- opendental_analytics_opendentalbackup_02_28_2025.taskunread definition

CREATE TABLE `taskunread` (
  `TaskUnreadNum` bigint(20) NOT NULL AUTO_INCREMENT,
  `TaskNum` bigint(20) NOT NULL,
  `UserNum` bigint(20) NOT NULL,
  PRIMARY KEY (`TaskUnreadNum`),
  KEY `TaskNum` (`TaskNum`),
  KEY `UserNum` (`UserNum`)
) ENGINE=MyISAM AUTO_INCREMENT=1445 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
-- opendental_analytics_opendentalbackup_02_28_2025.tasklist definition

CREATE TABLE `tasklist` (
  `TaskListNum` bigint(20) NOT NULL AUTO_INCREMENT,
  `Descript` varchar(255) DEFAULT '',
  `Parent` bigint(20) NOT NULL,
  `DateTL` date NOT NULL DEFAULT '0001-01-01',
  `IsRepeating` tinyint(3) unsigned NOT NULL,
  `DateType` tinyint(3) unsigned NOT NULL,
  `FromNum` bigint(20) NOT NULL,
  `ObjectType` tinyint(3) unsigned NOT NULL,
  `DateTimeEntry` datetime /* mariadb-5.3 */ NOT NULL DEFAULT '0001-01-01 00:00:00',
  `GlobalTaskFilterType` tinyint(4) NOT NULL,
  `TaskListStatus` tinyint(4) NOT NULL,
  PRIMARY KEY (`TaskListNum`),
  KEY `indexParent` (`Parent`)
) ENGINE=MyISAM AUTO_INCREMENT=55 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
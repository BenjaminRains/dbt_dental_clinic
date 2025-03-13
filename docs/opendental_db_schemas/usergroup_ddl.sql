-- opendental_analytics_opendentalbackup_02_28_2025.usergroup definition

CREATE TABLE `usergroup` (
  `UserGroupNum` bigint(20) NOT NULL AUTO_INCREMENT,
  `Description` varchar(255) DEFAULT '',
  `UserGroupNumCEMT` bigint(20) NOT NULL,
  PRIMARY KEY (`UserGroupNum`),
  KEY `UserGroupNumCEMT` (`UserGroupNumCEMT`)
) ENGINE=MyISAM AUTO_INCREMENT=12 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
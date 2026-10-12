-- opendental_analytics_opendentalbackup_02_28_2025.usergroupattach definition

CREATE TABLE `usergroupattach` (
  `UserGroupAttachNum` bigint(20) NOT NULL AUTO_INCREMENT,
  `UserNum` bigint(20) NOT NULL,
  `UserGroupNum` bigint(20) NOT NULL,
  PRIMARY KEY (`UserGroupAttachNum`),
  KEY `UserGroupNum` (`UserGroupNum`),
  KEY `UserNum` (`UserNum`)
) ENGINE=MyISAM AUTO_INCREMENT=155 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
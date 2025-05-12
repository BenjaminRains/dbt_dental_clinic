-- opendental_analytics_opendentalbackup_02_28_2025.employer definition

CREATE TABLE `employer` (
  `EmployerNum` bigint(20) NOT NULL AUTO_INCREMENT,
  `EmpName` varchar(255) DEFAULT '',
  `Address` varchar(255) DEFAULT '',
  `Address2` varchar(255) DEFAULT '',
  `City` varchar(255) DEFAULT '',
  `State` varchar(255) DEFAULT '',
  `Zip` varchar(255) DEFAULT '',
  `Phone` varchar(255) DEFAULT '',
  PRIMARY KEY (`EmployerNum`)
) ENGINE=MyISAM AUTO_INCREMENT=195469 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
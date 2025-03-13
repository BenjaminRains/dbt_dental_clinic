-- opendental_analytics_opendentalbackup_01_03_2025.procgroupitem definition

CREATE TABLE `procgroupitem` (
  `ProcGroupItemNum` bigint(20) NOT NULL AUTO_INCREMENT,
  `ProcNum` bigint(20) NOT NULL,
  `GroupNum` bigint(20) NOT NULL,
  PRIMARY KEY (`ProcGroupItemNum`),
  KEY `ProcNum` (`ProcNum`),
  KEY `GroupNum` (`GroupNum`)
) ENGINE=MyISAM AUTO_INCREMENT=26632 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_uca1400_ai_ci;
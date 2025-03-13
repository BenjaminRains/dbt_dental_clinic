-- opendental_analytics_opendentalbackup_01_03_2025.insbluebooklog definition

CREATE TABLE `insbluebooklog` (
  `InsBlueBookLogNum` bigint(20) NOT NULL AUTO_INCREMENT,
  `ClaimProcNum` bigint(20) NOT NULL,
  `AllowedFee` double NOT NULL,
  `DateTEntry` datetime NOT NULL DEFAULT '0001-01-01 00:00:00',
  `Description` text NOT NULL,
  PRIMARY KEY (`InsBlueBookLogNum`),
  KEY `ClaimProcNum` (`ClaimProcNum`)
) ENGINE=MyISAM AUTO_INCREMENT=186638 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_uca1400_ai_ci;
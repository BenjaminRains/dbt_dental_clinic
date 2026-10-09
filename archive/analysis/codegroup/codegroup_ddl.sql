-- opendental_analytics_opendentalbackup_01_03_2025.codegroup definition

CREATE TABLE `codegroup` (
  `CodeGroupNum` bigint(20) NOT NULL AUTO_INCREMENT,
  `GroupName` varchar(50) NOT NULL,
  `ProcCodes` text NOT NULL,
  `ItemOrder` int(11) NOT NULL,
  `CodeGroupFixed` tinyint(4) NOT NULL,
  `IsHidden` tinyint(4) NOT NULL,
  `ShowInAgeLimit` tinyint(4) NOT NULL,
  PRIMARY KEY (`CodeGroupNum`)
) ENGINE=MyISAM AUTO_INCREMENT=15 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_uca1400_ai_ci;
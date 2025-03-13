-- opendental_analytics_opendentalbackup_01_03_2025.programproperty definition

CREATE TABLE `programproperty` (
  `ProgramPropertyNum` bigint(20) NOT NULL AUTO_INCREMENT,
  `ProgramNum` bigint(20) NOT NULL,
  `PropertyDesc` varchar(255) DEFAULT '',
  `PropertyValue` text DEFAULT NULL,
  `ComputerName` varchar(255) NOT NULL,
  `ClinicNum` bigint(20) NOT NULL,
  `IsMasked` tinyint(4) NOT NULL,
  `IsHighSecurity` tinyint(4) NOT NULL,
  PRIMARY KEY (`ProgramPropertyNum`),
  KEY `ClinicNum` (`ClinicNum`)
) ENGINE=MyISAM AUTO_INCREMENT=351 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_uca1400_ai_ci;
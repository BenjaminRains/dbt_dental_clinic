-- opendental_analytics_opendentalbackup_02_28_2025.userodapptview definition

CREATE TABLE `userodapptview` (
  `UserodApptViewNum` bigint(20) NOT NULL AUTO_INCREMENT,
  `UserNum` bigint(20) NOT NULL,
  `ClinicNum` bigint(20) NOT NULL,
  `ApptViewNum` bigint(20) NOT NULL,
  PRIMARY KEY (`UserodApptViewNum`),
  KEY `UserNum` (`UserNum`),
  KEY `ClinicNum` (`ClinicNum`),
  KEY `ApptViewNum` (`ApptViewNum`)
) ENGINE=MyISAM AUTO_INCREMENT=34 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
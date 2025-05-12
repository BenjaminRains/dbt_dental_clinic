-- opendental_analytics_opendentalbackup_02_28_2025.userodpref definition

CREATE TABLE `userodpref` (
  `UserOdPrefNum` bigint(20) NOT NULL AUTO_INCREMENT,
  `UserNum` bigint(20) NOT NULL,
  `Fkey` bigint(20) NOT NULL,
  `FkeyType` tinyint(4) NOT NULL,
  `ValueString` text NOT NULL,
  `ClinicNum` bigint(20) NOT NULL,
  PRIMARY KEY (`UserOdPrefNum`),
  KEY `UserNum` (`UserNum`),
  KEY `Fkey` (`Fkey`),
  KEY `ClinicNum` (`ClinicNum`)
) ENGINE=MyISAM AUTO_INCREMENT=11019 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
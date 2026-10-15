-- opendental_analytics_opendentalbackup_01_03_2025.fee definition

CREATE TABLE `fee` (
  `FeeNum` bigint(20) NOT NULL AUTO_INCREMENT,
  `Amount` double NOT NULL DEFAULT 0,
  `OldCode` varchar(15) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin NOT NULL DEFAULT '',
  `FeeSched` bigint(20) NOT NULL,
  `UseDefaultFee` tinyint(3) unsigned NOT NULL DEFAULT 0,
  `UseDefaultCov` tinyint(3) unsigned NOT NULL DEFAULT 0,
  `CodeNum` bigint(20) NOT NULL,
  `ClinicNum` bigint(20) NOT NULL,
  `ProvNum` bigint(20) NOT NULL,
  `SecUserNumEntry` bigint(20) NOT NULL,
  `SecDateEntry` date NOT NULL DEFAULT '0001-01-01',
  `SecDateTEdit` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  PRIMARY KEY (`FeeNum`),
  KEY `indexADACode` (`OldCode`),
  KEY `CodeNum` (`CodeNum`),
  KEY `SecUserNumEntry` (`SecUserNumEntry`),
  KEY `ClinicNum` (`ClinicNum`),
  KEY `ProvNum` (`ProvNum`),
  KEY `FeeSchedCodeClinicProv` (`FeeSched`,`CodeNum`,`ClinicNum`,`ProvNum`)
) ENGINE=MyISAM AUTO_INCREMENT=217148 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_uca1400_ai_ci;
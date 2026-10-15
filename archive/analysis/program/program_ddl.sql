-- opendental_analytics_opendentalbackup_01_03_2025.program definition

CREATE TABLE `program` (
  `ProgramNum` bigint(20) NOT NULL AUTO_INCREMENT,
  `ProgName` varchar(100) DEFAULT '',
  `ProgDesc` varchar(100) DEFAULT '',
  `Enabled` tinyint(3) unsigned NOT NULL DEFAULT 0,
  `Path` text NOT NULL,
  `CommandLine` text NOT NULL,
  `Note` text DEFAULT NULL,
  `PluginDllName` varchar(255) NOT NULL,
  `ButtonImage` text NOT NULL,
  `FileTemplate` text NOT NULL,
  `FilePath` varchar(255) NOT NULL,
  `IsDisabledByHq` tinyint(4) NOT NULL,
  `CustErr` varchar(255) NOT NULL,
  PRIMARY KEY (`ProgramNum`)
) ENGINE=MyISAM AUTO_INCREMENT=147 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_uca1400_ai_ci;
-- opendental_analytics_opendentalbackup_01_03_2025.rxdef definition

CREATE TABLE `rxdef` (
  `RxDefNum` bigint(20) NOT NULL AUTO_INCREMENT,
  `Drug` varchar(255) DEFAULT '',
  `Sig` varchar(255) DEFAULT '',
  `Disp` varchar(255) DEFAULT '',
  `Refills` varchar(30) DEFAULT '',
  `Notes` varchar(255) DEFAULT '',
  `IsControlled` tinyint(4) NOT NULL,
  `RxCui` bigint(20) NOT NULL,
  `IsProcRequired` tinyint(4) NOT NULL,
  `PatientInstruction` text NOT NULL,
  PRIMARY KEY (`RxDefNum`),
  KEY `RxCui` (`RxCui`)
) ENGINE=MyISAM AUTO_INCREMENT=92 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_uca1400_ai_ci;
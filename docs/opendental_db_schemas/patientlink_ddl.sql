-- opendental_analytics_opendentalbackup_01_03_2025.patientlink definition

CREATE TABLE `patientlink` (
  `PatientLinkNum` bigint(20) NOT NULL AUTO_INCREMENT,
  `PatNumFrom` bigint(20) NOT NULL,
  `PatNumTo` bigint(20) NOT NULL,
  `LinkType` tinyint(4) NOT NULL,
  `DateTimeLink` datetime NOT NULL DEFAULT '0001-01-01 00:00:00',
  PRIMARY KEY (`PatientLinkNum`),
  KEY `PatNumFrom` (`PatNumFrom`),
  KEY `PatNumTo` (`PatNumTo`)
) ENGINE=MyISAM AUTO_INCREMENT=63 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_uca1400_ai_ci;
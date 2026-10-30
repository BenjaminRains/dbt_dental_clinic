-- opendental_analytics_opendentalbackup_01_03_2025.patplan definition

CREATE TABLE `patplan` (
  `PatPlanNum` bigint(20) NOT NULL AUTO_INCREMENT,
  `PatNum` bigint(20) NOT NULL,
  `Ordinal` tinyint(3) unsigned NOT NULL,
  `IsPending` tinyint(3) unsigned NOT NULL,
  `Relationship` tinyint(3) unsigned NOT NULL,
  `PatID` varchar(100) DEFAULT '',
  `InsSubNum` bigint(20) NOT NULL,
  `OrthoAutoFeeBilledOverride` double NOT NULL DEFAULT -1,
  `OrthoAutoNextClaimDate` date NOT NULL DEFAULT '0001-01-01',
  `SecDateTEntry` datetime NOT NULL DEFAULT '0001-01-01 00:00:00',
  `SecDateTEdit` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  PRIMARY KEY (`PatPlanNum`),
  KEY `indexPatNum` (`PatNum`),
  KEY `InsSubNum` (`InsSubNum`),
  KEY `SecDateTEdit` (`SecDateTEdit`),
  KEY `SecDateTEntry` (`SecDateTEntry`)
) ENGINE=MyISAM AUTO_INCREMENT=24077 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_uca1400_ai_ci;
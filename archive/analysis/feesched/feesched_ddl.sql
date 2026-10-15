-- opendental_analytics_opendentalbackup_01_03_2025.feesched definition

CREATE TABLE `feesched` (
  `FeeSchedNum` bigint(20) NOT NULL AUTO_INCREMENT,
  `Description` varchar(255) DEFAULT NULL,
  `FeeSchedType` int(11) NOT NULL,
  `ItemOrder` int(11) NOT NULL,
  `IsHidden` tinyint(1) NOT NULL,
  `IsGlobal` tinyint(4) NOT NULL,
  `SecUserNumEntry` bigint(20) NOT NULL,
  `SecDateEntry` date NOT NULL DEFAULT '0001-01-01',
  `SecDateTEdit` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  PRIMARY KEY (`FeeSchedNum`),
  KEY `SecUserNumEntry` (`SecUserNumEntry`)
) ENGINE=MyISAM AUTO_INCREMENT=8292 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_uca1400_ai_ci;
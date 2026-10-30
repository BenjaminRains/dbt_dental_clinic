-- opendental_analytics_opendentalbackup_01_03_2025.pharmacy definition

CREATE TABLE `pharmacy` (
  `PharmacyNum` bigint(20) NOT NULL AUTO_INCREMENT,
  `PharmID` varchar(255) DEFAULT NULL,
  `StoreName` varchar(255) DEFAULT NULL,
  `Phone` varchar(255) DEFAULT NULL,
  `Fax` varchar(255) DEFAULT NULL,
  `Address` varchar(255) DEFAULT NULL,
  `Address2` varchar(255) DEFAULT NULL,
  `City` varchar(255) DEFAULT NULL,
  `State` varchar(255) DEFAULT NULL,
  `Zip` varchar(255) DEFAULT NULL,
  `Note` text DEFAULT NULL,
  `DateTStamp` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  PRIMARY KEY (`PharmacyNum`)
) ENGINE=MyISAM AUTO_INCREMENT=94 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_uca1400_ai_ci;
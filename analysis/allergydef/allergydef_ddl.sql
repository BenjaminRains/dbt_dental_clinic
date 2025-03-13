-- opendental_analytics_opendentalbackup_02_28_2025.allergydef definition

CREATE TABLE `allergydef` (
  `AllergyDefNum` bigint(20) NOT NULL AUTO_INCREMENT,
  `Description` varchar(255) NOT NULL,
  `IsHidden` tinyint(4) NOT NULL,
  `DateTStamp` timestamp /* mariadb-5.3 */ NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `SnomedType` tinyint(4) DEFAULT NULL,
  `MedicationNum` bigint(20) NOT NULL,
  `UniiCode` varchar(255) NOT NULL,
  PRIMARY KEY (`AllergyDefNum`),
  KEY `MedicationNum` (`MedicationNum`)
) ENGINE=MyISAM AUTO_INCREMENT=80 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
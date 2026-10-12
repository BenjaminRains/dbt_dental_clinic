-- opendental_analytics_opendentalbackup_02_28_2025.medication definition

CREATE TABLE `medication` (
  `MedicationNum` bigint(20) NOT NULL AUTO_INCREMENT,
  `MedName` varchar(255) DEFAULT '',
  `GenericNum` bigint(20) NOT NULL,
  `Notes` text DEFAULT NULL,
  `DateTStamp` timestamp /* mariadb-5.3 */ NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `RxCui` bigint(20) NOT NULL,
  PRIMARY KEY (`MedicationNum`),
  KEY `RxCui` (`RxCui`)
) ENGINE=MyISAM AUTO_INCREMENT=1124 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
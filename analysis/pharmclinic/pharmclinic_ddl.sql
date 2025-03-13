-- opendental_analytics_opendentalbackup_01_03_2025.pharmclinic definition

CREATE TABLE `pharmclinic` (
  `PharmClinicNum` bigint(20) NOT NULL AUTO_INCREMENT,
  `PharmacyNum` bigint(20) NOT NULL,
  `ClinicNum` bigint(20) NOT NULL,
  PRIMARY KEY (`PharmClinicNum`),
  KEY `PharmacyNum` (`PharmacyNum`),
  KEY `ClinicNum` (`ClinicNum`)
) ENGINE=MyISAM AUTO_INCREMENT=20 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_uca1400_ai_ci;
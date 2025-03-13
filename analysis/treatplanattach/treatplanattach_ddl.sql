-- opendental_analytics_opendentalbackup_01_03_2025.treatplanattach definition

CREATE TABLE `treatplanattach` (
  `TreatPlanAttachNum` bigint(20) NOT NULL AUTO_INCREMENT,
  `TreatPlanNum` bigint(20) NOT NULL,
  `ProcNum` bigint(20) NOT NULL,
  `Priority` bigint(20) NOT NULL,
  PRIMARY KEY (`TreatPlanAttachNum`),
  KEY `TreatPlanNum` (`TreatPlanNum`),
  KEY `ProcNum` (`ProcNum`),
  KEY `Priority` (`Priority`)
) ENGINE=MyISAM AUTO_INCREMENT=185952 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_uca1400_ai_ci;
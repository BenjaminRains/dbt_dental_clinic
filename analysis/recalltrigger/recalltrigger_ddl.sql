-- opendental_analytics_opendentalbackup_01_03_2025.recalltrigger definition

CREATE TABLE `recalltrigger` (
  `RecallTriggerNum` bigint(20) NOT NULL AUTO_INCREMENT,
  `RecallTypeNum` bigint(20) NOT NULL,
  `CodeNum` bigint(20) NOT NULL,
  PRIMARY KEY (`RecallTriggerNum`),
  KEY `CodeNum` (`CodeNum`),
  KEY `RecallTypeNum` (`RecallTypeNum`)
) ENGINE=MyISAM AUTO_INCREMENT=78 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_uca1400_ai_ci;
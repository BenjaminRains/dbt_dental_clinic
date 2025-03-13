-- opendental_analytics_opendentalbackup_01_03_2025.scheduleop definition

CREATE TABLE `scheduleop` (
  `ScheduleOpNum` bigint(20) NOT NULL AUTO_INCREMENT,
  `ScheduleNum` bigint(20) NOT NULL,
  `OperatoryNum` bigint(20) NOT NULL,
  PRIMARY KEY (`ScheduleOpNum`),
  KEY `ScheduleNum` (`ScheduleNum`),
  KEY `OperatoryNum` (`OperatoryNum`)
) ENGINE=MyISAM AUTO_INCREMENT=210157 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_uca1400_ai_ci;
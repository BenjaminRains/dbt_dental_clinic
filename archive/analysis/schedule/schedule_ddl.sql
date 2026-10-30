-- opendental_analytics_opendentalbackup_01_03_2025.schedule definition

CREATE TABLE `schedule` (
  `ScheduleNum` bigint(20) NOT NULL AUTO_INCREMENT,
  `SchedDate` date NOT NULL DEFAULT '0001-01-01',
  `StartTime` time NOT NULL DEFAULT '00:00:00',
  `StopTime` time NOT NULL DEFAULT '00:00:00',
  `SchedType` tinyint(3) unsigned NOT NULL DEFAULT 0,
  `ProvNum` bigint(20) NOT NULL,
  `BlockoutType` bigint(20) NOT NULL,
  `Note` text DEFAULT NULL,
  `Status` tinyint(3) unsigned NOT NULL DEFAULT 0,
  `EmployeeNum` bigint(20) NOT NULL,
  `DateTStamp` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `ClinicNum` bigint(20) NOT NULL,
  PRIMARY KEY (`ScheduleNum`),
  KEY `ProvNum` (`ProvNum`),
  KEY `SchedDate` (`SchedDate`),
  KEY `ClinicNumSchedType` (`ClinicNum`,`SchedType`),
  KEY `BlockoutType` (`BlockoutType`),
  KEY `EmpDateTypeStopTime` (`EmployeeNum`,`SchedDate`,`SchedType`,`StopTime`)
) ENGINE=MyISAM AUTO_INCREMENT=201698 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_uca1400_ai_ci;
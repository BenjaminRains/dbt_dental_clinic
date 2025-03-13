-- opendental_analytics_opendentalbackup_01_03_2025.appointmenttype definition

CREATE TABLE `appointmenttype` (
  `AppointmentTypeNum` bigint(20) NOT NULL AUTO_INCREMENT,
  `AppointmentTypeName` varchar(255) NOT NULL,
  `AppointmentTypeColor` int(11) NOT NULL,
  `ItemOrder` int(11) NOT NULL,
  `IsHidden` tinyint(4) NOT NULL,
  `Pattern` varchar(255) NOT NULL,
  `CodeStr` varchar(4000) NOT NULL,
  `CodeStrRequired` varchar(4000) NOT NULL,
  `RequiredProcCodesNeeded` tinyint(4) NOT NULL,
  `BlockoutTypes` varchar(255) NOT NULL,
  PRIMARY KEY (`AppointmentTypeNum`)
) ENGINE=MyISAM AUTO_INCREMENT=14 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_uca1400_ai_ci;
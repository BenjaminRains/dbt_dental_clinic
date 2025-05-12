-- opendental_analytics_opendentalbackup_02_28_2025.employee definition

CREATE TABLE `employee` (
  `EmployeeNum` bigint(20) NOT NULL AUTO_INCREMENT,
  `LName` varchar(255) DEFAULT '',
  `FName` varchar(255) DEFAULT '',
  `MiddleI` varchar(255) DEFAULT '',
  `IsHidden` tinyint(1) unsigned NOT NULL DEFAULT 0,
  `ClockStatus` varchar(255) DEFAULT '',
  `PhoneExt` int(11) NOT NULL,
  `PayrollID` varchar(255) NOT NULL,
  `WirelessPhone` varchar(255) NOT NULL,
  `EmailWork` varchar(255) NOT NULL,
  `EmailPersonal` varchar(255) NOT NULL,
  `IsFurloughed` tinyint(4) NOT NULL,
  `IsWorkingHome` tinyint(4) NOT NULL,
  `ReportsTo` bigint(20) NOT NULL,
  PRIMARY KEY (`EmployeeNum`)
) ENGINE=MyISAM AUTO_INCREMENT=9283 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
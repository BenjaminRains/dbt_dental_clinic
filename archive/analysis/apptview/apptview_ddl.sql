-- opendental_analytics_opendentalbackup_02_28_2025.apptview definition

CREATE TABLE `apptview` (
  `ApptViewNum` bigint(20) NOT NULL AUTO_INCREMENT,
  `Description` varchar(255) DEFAULT '',
  `ItemOrder` smallint(5) unsigned NOT NULL DEFAULT 0,
  `RowsPerIncr` tinyint(3) unsigned NOT NULL DEFAULT 1,
  `OnlyScheduledProvs` tinyint(3) unsigned NOT NULL,
  `OnlySchedBeforeTime` time /* mariadb-5.3 */ NOT NULL,
  `OnlySchedAfterTime` time /* mariadb-5.3 */ NOT NULL,
  `StackBehavUR` tinyint(4) NOT NULL,
  `StackBehavLR` tinyint(4) NOT NULL,
  `ClinicNum` bigint(20) NOT NULL,
  `ApptTimeScrollStart` time /* mariadb-5.3 */ NOT NULL,
  `IsScrollStartDynamic` tinyint(4) NOT NULL,
  `IsApptBubblesDisabled` tinyint(4) NOT NULL,
  `WidthOpMinimum` smallint(5) unsigned NOT NULL,
  `WaitingRmName` tinyint(4) NOT NULL,
  `OnlyScheduledProvDays` tinyint(4) NOT NULL,
  PRIMARY KEY (`ApptViewNum`),
  KEY `ClinicNum` (`ClinicNum`)
) ENGINE=MyISAM AUTO_INCREMENT=10 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
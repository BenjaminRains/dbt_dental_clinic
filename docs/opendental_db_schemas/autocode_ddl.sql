-- opendental_analytics_opendentalbackup_01_03_2025.autocode definition

CREATE TABLE `autocode` (
  `AutoCodeNum` bigint(20) NOT NULL AUTO_INCREMENT,
  `Description` varchar(255) DEFAULT '',
  `IsHidden` tinyint(3) unsigned NOT NULL DEFAULT 0,
  `LessIntrusive` tinyint(3) unsigned NOT NULL DEFAULT 0,
  PRIMARY KEY (`AutoCodeNum`)
) ENGINE=MyISAM AUTO_INCREMENT=55 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_uca1400_ai_ci;


-- opendental_analytics_opendentalbackup_01_03_2025.toothinitial definition

CREATE TABLE `toothinitial` (
  `ToothInitialNum` bigint(20) NOT NULL AUTO_INCREMENT,
  `PatNum` bigint(20) NOT NULL,
  `ToothNum` varchar(2) DEFAULT '',
  `InitialType` tinyint(3) unsigned NOT NULL,
  `Movement` float NOT NULL,
  `DrawingSegment` text DEFAULT NULL,
  `ColorDraw` int(11) NOT NULL,
  `SecDateTEntry` datetime NOT NULL DEFAULT '0001-01-01 00:00:00',
  `SecDateTEdit` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `DrawText` varchar(255) NOT NULL,
  PRIMARY KEY (`ToothInitialNum`),
  KEY `PatNum` (`PatNum`),
  KEY `SecDateTEntry` (`SecDateTEntry`),
  KEY `SecDateTEdit` (`SecDateTEdit`)
) ENGINE=MyISAM AUTO_INCREMENT=201011 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_uca1400_ai_ci;
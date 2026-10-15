-- opendental_analytics_opendentalbackup_02_28_2025.allergy definition

CREATE TABLE `allergy` (
  `AllergyNum` bigint(20) NOT NULL AUTO_INCREMENT,
  `AllergyDefNum` bigint(20) NOT NULL,
  `PatNum` bigint(20) NOT NULL,
  `Reaction` varchar(255) NOT NULL,
  `StatusIsActive` tinyint(4) NOT NULL,
  `DateTStamp` timestamp /* mariadb-5.3 */ NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `DateAdverseReaction` date NOT NULL DEFAULT '0001-01-01',
  `SnomedReaction` varchar(255) NOT NULL,
  PRIMARY KEY (`AllergyNum`),
  KEY `AllergyDefNum` (`AllergyDefNum`),
  KEY `PatNum` (`PatNum`)
) ENGINE=MyISAM AUTO_INCREMENT=4651 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
-- opendental_analytics_opendentalbackup_02_28_2025.tasksubscription definition

CREATE TABLE `tasksubscription` (
  `TaskSubscriptionNum` bigint(20) NOT NULL AUTO_INCREMENT,
  `UserNum` bigint(20) NOT NULL,
  `TaskListNum` bigint(20) NOT NULL,
  `TaskNum` bigint(20) NOT NULL,
  PRIMARY KEY (`TaskSubscriptionNum`),
  KEY `UserNum` (`UserNum`),
  KEY `TaskNum` (`TaskNum`),
  KEY `TaskListNum` (`TaskListNum`)
) ENGINE=MyISAM AUTO_INCREMENT=78 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
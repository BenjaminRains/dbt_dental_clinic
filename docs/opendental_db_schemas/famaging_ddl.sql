-- opendental_analytics_opendentalbackup_01_03_2025.famaging definition

CREATE TABLE `famaging` (
  `PatNum` bigint(20) NOT NULL AUTO_INCREMENT,
  `Bal_0_30` double NOT NULL,
  `Bal_31_60` double NOT NULL,
  `Bal_61_90` double NOT NULL,
  `BalOver90` double NOT NULL,
  `InsEst` double NOT NULL,
  `BalTotal` double NOT NULL,
  `PayPlanDue` double NOT NULL,
  PRIMARY KEY (`PatNum`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_uca1400_ai_ci;
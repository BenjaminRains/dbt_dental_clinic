-- opendental_analytics_opendentalbackup_01_03_2025.statementprod definition

CREATE TABLE `statementprod` (
  `StatementProdNum` bigint(20) NOT NULL AUTO_INCREMENT,
  `StatementNum` bigint(20) NOT NULL,
  `FKey` bigint(20) NOT NULL,
  `ProdType` tinyint(4) NOT NULL,
  `LateChargeAdjNum` bigint(20) NOT NULL,
  `DocNum` bigint(20) NOT NULL,
  PRIMARY KEY (`StatementProdNum`),
  KEY `StatementNum` (`StatementNum`),
  KEY `FKey` (`FKey`),
  KEY `ProdType` (`ProdType`),
  KEY `LateChargeAdjNum` (`LateChargeAdjNum`),
  KEY `DocNum` (`DocNum`)
) ENGINE=MyISAM AUTO_INCREMENT=180275 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_uca1400_ai_ci;
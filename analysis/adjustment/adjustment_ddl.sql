-- opendental_analytics_opendentalbackup_01_03_2025.adjustment definition

CREATE TABLE `adjustment` (
  `AdjNum` bigint(20) NOT NULL AUTO_INCREMENT,
  `AdjDate` date NOT NULL DEFAULT '0001-01-01',
  `AdjAmt` double NOT NULL DEFAULT 0,
  `PatNum` bigint(20) NOT NULL,
  `AdjType` bigint(20) NOT NULL,
  `ProvNum` bigint(20) NOT NULL,
  `AdjNote` text DEFAULT NULL,
  `ProcDate` date NOT NULL DEFAULT '0001-01-01',
  `ProcNum` bigint(20) NOT NULL,
  `DateEntry` date NOT NULL DEFAULT '0001-01-01',
  `ClinicNum` bigint(20) NOT NULL,
  `StatementNum` bigint(20) NOT NULL,
  `SecUserNumEntry` bigint(20) NOT NULL,
  `SecDateTEdit` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `TaxTransID` bigint(20) NOT NULL,
  PRIMARY KEY (`AdjNum`),
  KEY `indexPatNum` (`PatNum`),
  KEY `ClinicNum` (`ClinicNum`),
  KEY `StatementNum` (`StatementNum`),
  KEY `ProcNum` (`ProcNum`),
  KEY `indexProvNum` (`ProvNum`),
  KEY `SecUserNumEntry` (`SecUserNumEntry`),
  KEY `indexPNAmt` (`ProcNum`,`AdjAmt`),
  KEY `AdjDatePN` (`AdjDate`,`PatNum`),
  KEY `TaxTransID` (`TaxTransID`),
  KEY `SecDateTEditPN` (`SecDateTEdit`,`PatNum`),
  KEY `idx_adj_proc_amt` (`ProcNum`,`AdjAmt`,`AdjDate`)
) ENGINE=MyISAM AUTO_INCREMENT=280583 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_uca1400_ai_ci;
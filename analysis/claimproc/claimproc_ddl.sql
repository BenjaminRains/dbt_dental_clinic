-- opendental_analytics_opendentalbackup_01_03_2025.claimproc definition

CREATE TABLE `claimproc` (
  `ClaimProcNum` bigint(20) NOT NULL AUTO_INCREMENT,
  `ProcNum` bigint(20) NOT NULL,
  `ClaimNum` bigint(20) NOT NULL,
  `PatNum` bigint(20) NOT NULL,
  `ProvNum` bigint(20) NOT NULL,
  `FeeBilled` double NOT NULL DEFAULT 0,
  `InsPayEst` double NOT NULL DEFAULT 0,
  `DedApplied` double NOT NULL DEFAULT 0,
  `Status` tinyint(3) unsigned NOT NULL DEFAULT 0,
  `InsPayAmt` double NOT NULL DEFAULT 0,
  `Remarks` varchar(255) DEFAULT '',
  `ClaimPaymentNum` bigint(20) NOT NULL,
  `PlanNum` bigint(20) NOT NULL,
  `DateCP` date NOT NULL DEFAULT '0001-01-01',
  `WriteOff` double NOT NULL DEFAULT 0,
  `CodeSent` varchar(15) DEFAULT '',
  `AllowedOverride` double NOT NULL,
  `Percentage` tinyint(4) NOT NULL DEFAULT -1,
  `PercentOverride` tinyint(4) NOT NULL DEFAULT -1,
  `CopayAmt` double NOT NULL DEFAULT -1,
  `NoBillIns` tinyint(3) unsigned NOT NULL DEFAULT 0,
  `PaidOtherIns` double NOT NULL DEFAULT -1,
  `BaseEst` double NOT NULL DEFAULT 0,
  `CopayOverride` double NOT NULL DEFAULT -1,
  `ProcDate` date NOT NULL DEFAULT '0001-01-01',
  `DateEntry` date NOT NULL DEFAULT '0001-01-01',
  `LineNumber` tinyint(3) unsigned NOT NULL,
  `DedEst` double NOT NULL,
  `DedEstOverride` double NOT NULL,
  `InsEstTotal` double NOT NULL,
  `InsEstTotalOverride` double NOT NULL,
  `PaidOtherInsOverride` double NOT NULL,
  `EstimateNote` varchar(255) NOT NULL,
  `WriteOffEst` double NOT NULL,
  `WriteOffEstOverride` double NOT NULL,
  `ClinicNum` bigint(20) NOT NULL,
  `InsSubNum` bigint(20) NOT NULL,
  `PaymentRow` int(11) NOT NULL,
  `PayPlanNum` bigint(20) NOT NULL,
  `ClaimPaymentTracking` bigint(20) NOT NULL,
  `SecUserNumEntry` bigint(20) NOT NULL,
  `SecDateEntry` date NOT NULL DEFAULT '0001-01-01',
  `SecDateTEdit` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `DateSuppReceived` date NOT NULL DEFAULT '0001-01-01',
  `DateInsFinalized` date NOT NULL DEFAULT '0001-01-01',
  `IsTransfer` tinyint(4) NOT NULL,
  `ClaimAdjReasonCodes` varchar(255) NOT NULL,
  `IsOverpay` tinyint(4) NOT NULL,
  `SecurityHash` varchar(255) NOT NULL,
  PRIMARY KEY (`ClaimProcNum`),
  KEY `indexPatNum` (`PatNum`),
  KEY `indexPlanNum` (`PlanNum`),
  KEY `indexClaimNum` (`ClaimNum`),
  KEY `indexProvNum` (`ProvNum`),
  KEY `indexProcNum` (`ProcNum`),
  KEY `indexClaimPaymentNum` (`ClaimPaymentNum`),
  KEY `ClinicNum` (`ClinicNum`),
  KEY `InsSubNum` (`InsSubNum`),
  KEY `Status` (`Status`),
  KEY `PayPlanNum` (`PayPlanNum`),
  KEY `indexCPNSIPA` (`ClaimPaymentNum`,`Status`,`InsPayAmt`),
  KEY `indexPNPD` (`ProvNum`,`ProcDate`),
  KEY `indexPNDCP` (`ProvNum`,`DateCP`),
  KEY `ClaimPaymentTracking` (`ClaimPaymentTracking`),
  KEY `SecUserNumEntry` (`SecUserNumEntry`),
  KEY `indexAcctCov` (`ProcNum`,`PlanNum`,`Status`,`InsPayAmt`,`InsPayEst`,`WriteOff`,`NoBillIns`),
  KEY `DateCP` (`DateCP`),
  KEY `DateSuppReceived` (`DateSuppReceived`),
  KEY `indexAgingCovering` (`Status`,`PatNum`,`DateCP`,`PayPlanNum`,`InsPayAmt`,`WriteOff`,`InsPayEst`,`ProcDate`,`ProcNum`),
  KEY `indexTxFinder` (`InsSubNum`,`ProcNum`,`Status`,`ProcDate`,`PatNum`,`InsPayAmt`,`InsPayEst`),
  KEY `indexOutClaimCovering` (`ClaimNum`,`ClaimPaymentNum`,`InsPayAmt`,`DateCP`,`IsTransfer`),
  KEY `SecDateTEditPN` (`SecDateTEdit`,`PatNum`)
) ENGINE=MyISAM AUTO_INCREMENT=267616 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_uca1400_ai_ci;
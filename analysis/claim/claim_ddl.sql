-- opendental_analytics_opendentalbackup_01_03_2025.claim definition

CREATE TABLE `claim` (
  `ClaimNum` bigint(20) NOT NULL AUTO_INCREMENT,
  `PatNum` bigint(20) NOT NULL,
  `DateService` date NOT NULL DEFAULT '0001-01-01',
  `DateSent` date NOT NULL DEFAULT '0001-01-01',
  `ClaimStatus` char(1) DEFAULT '',
  `DateReceived` date NOT NULL DEFAULT '0001-01-01',
  `PlanNum` bigint(20) NOT NULL,
  `ProvTreat` bigint(20) NOT NULL,
  `ClaimFee` double NOT NULL DEFAULT 0,
  `InsPayEst` double NOT NULL DEFAULT 0,
  `InsPayAmt` double NOT NULL DEFAULT 0,
  `DedApplied` double NOT NULL DEFAULT 0,
  `PreAuthString` varchar(40) DEFAULT '',
  `IsProsthesis` char(1) DEFAULT '',
  `PriorDate` date NOT NULL DEFAULT '0001-01-01',
  `ReasonUnderPaid` varchar(255) DEFAULT '',
  `ClaimNote` varchar(400) DEFAULT NULL,
  `ClaimType` varchar(255) DEFAULT '',
  `ProvBill` bigint(20) NOT NULL,
  `ReferringProv` bigint(20) NOT NULL,
  `RefNumString` varchar(40) DEFAULT '',
  `PlaceService` tinyint(3) unsigned NOT NULL DEFAULT 0,
  `AccidentRelated` char(1) DEFAULT '',
  `AccidentDate` date NOT NULL DEFAULT '0001-01-01',
  `AccidentST` varchar(2) DEFAULT '',
  `EmployRelated` tinyint(3) unsigned NOT NULL DEFAULT 0,
  `IsOrtho` tinyint(3) unsigned NOT NULL DEFAULT 0,
  `OrthoRemainM` tinyint(3) unsigned NOT NULL DEFAULT 0,
  `OrthoDate` date NOT NULL DEFAULT '0001-01-01',
  `PatRelat` tinyint(3) unsigned NOT NULL DEFAULT 0,
  `PlanNum2` bigint(20) NOT NULL,
  `PatRelat2` tinyint(3) unsigned NOT NULL DEFAULT 0,
  `WriteOff` double NOT NULL DEFAULT 0,
  `Radiographs` tinyint(3) unsigned NOT NULL DEFAULT 0,
  `ClinicNum` bigint(20) NOT NULL,
  `ClaimForm` bigint(20) NOT NULL,
  `AttachedImages` int(11) NOT NULL,
  `AttachedModels` int(11) NOT NULL,
  `AttachedFlags` varchar(255) DEFAULT NULL,
  `AttachmentID` varchar(255) DEFAULT NULL,
  `CanadianMaterialsForwarded` varchar(10) NOT NULL,
  `CanadianReferralProviderNum` varchar(20) NOT NULL,
  `CanadianReferralReason` tinyint(4) NOT NULL,
  `CanadianIsInitialLower` varchar(5) NOT NULL,
  `CanadianDateInitialLower` date NOT NULL DEFAULT '0001-01-01',
  `CanadianMandProsthMaterial` tinyint(4) NOT NULL,
  `CanadianIsInitialUpper` varchar(5) NOT NULL,
  `CanadianDateInitialUpper` date NOT NULL DEFAULT '0001-01-01',
  `CanadianMaxProsthMaterial` tinyint(4) NOT NULL,
  `InsSubNum` bigint(20) NOT NULL,
  `InsSubNum2` bigint(20) NOT NULL,
  `CanadaTransRefNum` varchar(255) NOT NULL,
  `CanadaEstTreatStartDate` date NOT NULL DEFAULT '0001-01-01',
  `CanadaInitialPayment` double NOT NULL,
  `CanadaPaymentMode` tinyint(3) unsigned NOT NULL,
  `CanadaTreatDuration` tinyint(3) unsigned NOT NULL,
  `CanadaNumAnticipatedPayments` tinyint(3) unsigned NOT NULL,
  `CanadaAnticipatedPayAmount` double NOT NULL,
  `PriorAuthorizationNumber` varchar(255) NOT NULL,
  `SpecialProgramCode` tinyint(4) NOT NULL,
  `UniformBillType` varchar(255) NOT NULL,
  `MedType` tinyint(4) NOT NULL,
  `AdmissionTypeCode` varchar(255) NOT NULL,
  `AdmissionSourceCode` varchar(255) NOT NULL,
  `PatientStatusCode` varchar(255) NOT NULL,
  `CustomTracking` bigint(20) NOT NULL,
  `DateResent` date NOT NULL DEFAULT '0001-01-01',
  `CorrectionType` tinyint(4) NOT NULL,
  `ClaimIdentifier` varchar(255) NOT NULL,
  `OrigRefNum` varchar(255) NOT NULL,
  `ProvOrderOverride` bigint(20) NOT NULL,
  `OrthoTotalM` tinyint(3) unsigned NOT NULL,
  `ShareOfCost` double NOT NULL,
  `SecUserNumEntry` bigint(20) NOT NULL,
  `SecDateEntry` date NOT NULL DEFAULT '0001-01-01',
  `SecDateTEdit` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `OrderingReferralNum` bigint(20) NOT NULL,
  `DateSentOrig` date NOT NULL DEFAULT '0001-01-01',
  `DateIllnessInjuryPreg` date NOT NULL DEFAULT '0001-01-01',
  `DateIllnessInjuryPregQualifier` smallint(6) NOT NULL,
  `DateOther` date NOT NULL DEFAULT '0001-01-01',
  `DateOtherQualifier` smallint(6) NOT NULL,
  `IsOutsideLab` tinyint(4) NOT NULL,
  `SecurityHash` varchar(255) NOT NULL,
  `Narrative` text NOT NULL,
  PRIMARY KEY (`ClaimNum`),
  KEY `indexPlanNum` (`PlanNum`),
  KEY `InsSubNum` (`InsSubNum`),
  KEY `InsSubNum2` (`InsSubNum2`),
  KEY `CustomTracking` (`CustomTracking`),
  KEY `ProvOrderOverride` (`ProvOrderOverride`),
  KEY `SecUserNumEntry` (`SecUserNumEntry`),
  KEY `indexOutClaimCovering` (`PlanNum`,`ClaimStatus`,`ClaimType`,`PatNum`,`ClaimNum`,`DateService`,`ProvTreat`,`ClaimFee`,`ClinicNum`),
  KEY `OrderingReferralNum` (`OrderingReferralNum`),
  KEY `ProvBill` (`ProvBill`),
  KEY `ProvTreat` (`ProvTreat`),
  KEY `ClinicNum` (`ClinicNum`),
  KEY `PatStatusTypeDate` (`PatNum`,`ClaimStatus`,`ClaimType`,`DateSent`)
) ENGINE=MyISAM AUTO_INCREMENT=25721 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_uca1400_ai_ci;
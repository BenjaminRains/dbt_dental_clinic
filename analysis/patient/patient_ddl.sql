-- opendental_analytics_opendentalbackup_01_03_2025.patient definition

CREATE TABLE `patient` (
  `PatNum` bigint(20) NOT NULL AUTO_INCREMENT,
  `LName` varchar(100) DEFAULT '',
  `FName` varchar(100) DEFAULT '',
  `MiddleI` varchar(100) DEFAULT '',
  `Preferred` varchar(100) DEFAULT '',
  `PatStatus` tinyint(3) unsigned NOT NULL DEFAULT 0,
  `Gender` tinyint(3) unsigned NOT NULL DEFAULT 0,
  `Position` tinyint(3) unsigned NOT NULL DEFAULT 0,
  `Birthdate` date NOT NULL DEFAULT '0001-01-01',
  `SSN` varchar(100) DEFAULT '',
  `Address` varchar(100) DEFAULT '',
  `Address2` varchar(100) DEFAULT '',
  `City` varchar(100) DEFAULT '',
  `State` varchar(100) DEFAULT '',
  `Zip` varchar(100) DEFAULT '',
  `HmPhone` varchar(30) DEFAULT '',
  `WkPhone` varchar(30) DEFAULT '',
  `WirelessPhone` varchar(30) DEFAULT '',
  `Guarantor` bigint(20) NOT NULL,
  `CreditType` char(1) DEFAULT '',
  `Email` varchar(100) DEFAULT '',
  `Salutation` varchar(100) DEFAULT '',
  `EstBalance` double NOT NULL DEFAULT 0,
  `PriProv` bigint(20) NOT NULL,
  `SecProv` bigint(20) NOT NULL,
  `FeeSched` bigint(20) NOT NULL,
  `BillingType` bigint(20) NOT NULL,
  `ImageFolder` varchar(100) DEFAULT '',
  `AddrNote` text DEFAULT NULL,
  `FamFinUrgNote` text DEFAULT NULL,
  `MedUrgNote` varchar(255) DEFAULT '',
  `ApptModNote` varchar(255) DEFAULT '',
  `StudentStatus` char(1) DEFAULT '',
  `SchoolName` varchar(255) NOT NULL,
  `ChartNumber` varchar(100) NOT NULL DEFAULT '',
  `MedicaidID` varchar(20) DEFAULT '',
  `Bal_0_30` double NOT NULL DEFAULT 0,
  `Bal_31_60` double NOT NULL DEFAULT 0,
  `Bal_61_90` double NOT NULL DEFAULT 0,
  `BalOver90` double NOT NULL DEFAULT 0,
  `InsEst` double NOT NULL DEFAULT 0,
  `BalTotal` double NOT NULL DEFAULT 0,
  `EmployerNum` bigint(20) NOT NULL,
  `EmploymentNote` varchar(255) DEFAULT '',
  `County` varchar(255) DEFAULT '',
  `GradeLevel` tinyint(4) NOT NULL DEFAULT 0,
  `Urgency` tinyint(4) NOT NULL DEFAULT 0,
  `DateFirstVisit` date NOT NULL DEFAULT '0001-01-01',
  `ClinicNum` bigint(20) NOT NULL,
  `HasIns` varchar(255) DEFAULT '',
  `TrophyFolder` varchar(255) DEFAULT '',
  `PlannedIsDone` tinyint(3) unsigned NOT NULL,
  `Premed` tinyint(3) unsigned NOT NULL,
  `Ward` varchar(255) DEFAULT '',
  `PreferConfirmMethod` tinyint(3) unsigned NOT NULL,
  `PreferContactMethod` tinyint(3) unsigned NOT NULL,
  `PreferRecallMethod` tinyint(3) unsigned NOT NULL,
  `SchedBeforeTime` time DEFAULT NULL,
  `SchedAfterTime` time DEFAULT NULL,
  `SchedDayOfWeek` tinyint(3) unsigned NOT NULL,
  `Language` varchar(100) DEFAULT '',
  `AdmitDate` date NOT NULL DEFAULT '0001-01-01',
  `Title` varchar(15) DEFAULT NULL,
  `PayPlanDue` double NOT NULL,
  `SiteNum` bigint(20) NOT NULL,
  `DateTStamp` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `ResponsParty` bigint(20) NOT NULL,
  `CanadianEligibilityCode` tinyint(4) NOT NULL,
  `AskToArriveEarly` int(11) NOT NULL,
  `PreferContactConfidential` tinyint(4) NOT NULL,
  `SuperFamily` bigint(20) NOT NULL,
  `TxtMsgOk` tinyint(4) NOT NULL,
  `SmokingSnoMed` varchar(32) NOT NULL,
  `Country` varchar(255) NOT NULL,
  `DateTimeDeceased` datetime NOT NULL DEFAULT '0001-01-01 00:00:00',
  `BillingCycleDay` int(11) NOT NULL DEFAULT 1,
  `SecUserNumEntry` bigint(20) NOT NULL,
  `SecDateEntry` date NOT NULL DEFAULT '0001-01-01',
  `HasSuperBilling` tinyint(4) NOT NULL,
  `PatNumCloneFrom` bigint(20) NOT NULL,
  `DiscountPlanNum` bigint(20) NOT NULL,
  `HasSignedTil` tinyint(4) NOT NULL,
  `ShortCodeOptIn` tinyint(4) NOT NULL,
  `SecurityHash` varchar(255) NOT NULL,
  PRIMARY KEY (`PatNum`),
  KEY `indexLName` (`LName`(10)),
  KEY `indexFName` (`FName`(10)),
  KEY `indexLFName` (`LName`,`FName`),
  KEY `indexGuarantor` (`Guarantor`),
  KEY `ResponsParty` (`ResponsParty`),
  KEY `SuperFamily` (`SuperFamily`),
  KEY `SiteNum` (`SiteNum`),
  KEY `PatStatus` (`PatStatus`),
  KEY `Email` (`Email`),
  KEY `ChartNumber` (`ChartNumber`),
  KEY `SecUserNumEntry` (`SecUserNumEntry`),
  KEY `HmPhone` (`HmPhone`),
  KEY `WirelessPhone` (`WirelessPhone`),
  KEY `WkPhone` (`WkPhone`),
  KEY `PatNumCloneFrom` (`PatNumCloneFrom`),
  KEY `DiscountPlanNum` (`DiscountPlanNum`),
  KEY `FeeSched` (`FeeSched`),
  KEY `SecDateEntry` (`SecDateEntry`),
  KEY `PriProv` (`PriProv`),
  KEY `SecProv` (`SecProv`),
  KEY `ClinicPatStatus` (`ClinicNum`,`PatStatus`),
  KEY `BirthdateStatus` (`Birthdate`,`PatStatus`),
  KEY `idx_pat_guarantor` (`Guarantor`),
  KEY `idx_pat_birth` (`Birthdate`)
) ENGINE=MyISAM AUTO_INCREMENT=32852 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_uca1400_ai_ci;
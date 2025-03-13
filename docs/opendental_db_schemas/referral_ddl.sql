-- opendental_analytics_opendentalbackup_01_03_2025.referral definition

CREATE TABLE `referral` (
  `ReferralNum` bigint(20) NOT NULL AUTO_INCREMENT,
  `LName` varchar(100) DEFAULT '',
  `FName` varchar(100) DEFAULT '',
  `MName` varchar(100) DEFAULT '',
  `SSN` varchar(9) DEFAULT '',
  `UsingTIN` tinyint(3) unsigned NOT NULL DEFAULT 0,
  `Specialty` bigint(20) NOT NULL,
  `ST` varchar(2) DEFAULT '',
  `Telephone` varchar(10) DEFAULT '',
  `Address` varchar(100) DEFAULT '',
  `Address2` varchar(100) DEFAULT '',
  `City` varchar(100) DEFAULT '',
  `Zip` varchar(10) DEFAULT '',
  `Note` text DEFAULT NULL,
  `Phone2` varchar(30) DEFAULT '',
  `IsHidden` tinyint(3) unsigned NOT NULL DEFAULT 0,
  `NotPerson` tinyint(3) unsigned NOT NULL DEFAULT 0,
  `Title` varchar(255) DEFAULT '',
  `EMail` varchar(255) DEFAULT '',
  `PatNum` bigint(20) NOT NULL,
  `NationalProvID` varchar(255) DEFAULT NULL,
  `Slip` bigint(20) NOT NULL,
  `IsDoctor` tinyint(4) NOT NULL,
  `IsTrustedDirect` tinyint(4) NOT NULL,
  `DateTStamp` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `IsPreferred` tinyint(4) NOT NULL,
  `BusinessName` varchar(255) NOT NULL,
  `DisplayNote` varchar(4000) NOT NULL,
  PRIMARY KEY (`ReferralNum`)
) ENGINE=MyISAM AUTO_INCREMENT=1141 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_uca1400_ai_ci;
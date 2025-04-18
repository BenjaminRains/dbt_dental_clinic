-- opendental_analytics_opendentalbackup_01_03_2025.document definition

CREATE TABLE `document` (
  `DocNum` bigint(20) NOT NULL AUTO_INCREMENT,
  `Description` varchar(255) DEFAULT '',
  `DateCreated` datetime NOT NULL,
  `DocCategory` bigint(20) NOT NULL,
  `PatNum` bigint(20) NOT NULL,
  `FileName` varchar(255) DEFAULT '',
  `ImgType` tinyint(3) unsigned NOT NULL DEFAULT 0,
  `IsFlipped` tinyint(3) unsigned NOT NULL DEFAULT 0,
  `DegreesRotated` float NOT NULL,
  `ToothNumbers` varchar(255) DEFAULT '',
  `Note` mediumtext NOT NULL,
  `SigIsTopaz` tinyint(3) unsigned NOT NULL,
  `Signature` text DEFAULT NULL,
  `CropX` int(11) NOT NULL,
  `CropY` int(11) NOT NULL,
  `CropW` int(11) NOT NULL,
  `CropH` int(11) NOT NULL,
  `WindowingMin` int(11) NOT NULL,
  `WindowingMax` int(11) NOT NULL,
  `MountItemNum` bigint(20) NOT NULL,
  `DateTStamp` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `RawBase64` mediumtext NOT NULL,
  `Thumbnail` text NOT NULL,
  `ExternalGUID` varchar(255) NOT NULL,
  `ExternalSource` varchar(255) NOT NULL,
  `ProvNum` bigint(20) NOT NULL,
  `IsCropOld` tinyint(3) unsigned NOT NULL,
  `OcrResponseData` text NOT NULL,
  `ImageCaptureType` tinyint(4) NOT NULL DEFAULT 0,
  `PrintHeading` tinyint(4) NOT NULL,
  PRIMARY KEY (`DocNum`),
  KEY `PatNum` (`PatNum`),
  KEY `PNDC` (`PatNum`,`DocCategory`),
  KEY `MountItemNum` (`MountItemNum`)
) ENGINE=MyISAM AUTO_INCREMENT=234688 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_uca1400_ai_ci;
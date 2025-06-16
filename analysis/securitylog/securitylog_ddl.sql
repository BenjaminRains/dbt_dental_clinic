-- opendental.securitylog definition

CREATE TABLE `securitylog` (
  `SecurityLogNum` bigint(20) NOT NULL AUTO_INCREMENT,
  `PermType` smallint(6) NOT NULL,
  `UserNum` bigint(20) NOT NULL,
  `LogDateTime` datetime NOT NULL DEFAULT '0001-01-01 00:00:00',
  `LogText` text,
  `PatNum` bigint(20) NOT NULL,
  `CompName` varchar(255) NOT NULL,
  `FKey` bigint(20) NOT NULL,
  `LogSource` tinyint(4) NOT NULL,
  `DefNum` bigint(20) NOT NULL,
  `DefNumError` bigint(20) NOT NULL,
  `DateTPrevious` datetime NOT NULL DEFAULT '0001-01-01 00:00:00',
  PRIMARY KEY (`SecurityLogNum`),
  KEY `PatNum` (`PatNum`),
  KEY `FKey` (`FKey`),
  KEY `DefNum` (`DefNum`),
  KEY `DefNumError` (`DefNumError`),
  KEY `UserNum` (`UserNum`),
  KEY `LogDateTime` (`LogDateTime`),
  KEY `PermType` (`PermType`)
) ENGINE=MyISAM AUTO_INCREMENT=3715101 DEFAULT CHARSET=utf8;
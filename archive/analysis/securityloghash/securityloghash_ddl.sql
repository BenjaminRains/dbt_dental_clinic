-- opendental.securityloghash definition

CREATE TABLE `securityloghash` (
  `SecurityLogHashNum` bigint(20) NOT NULL AUTO_INCREMENT,
  `SecurityLogNum` bigint(20) NOT NULL,
  `LogHash` varchar(255) NOT NULL,
  PRIMARY KEY (`SecurityLogHashNum`),
  KEY `SecurityLogNum` (`SecurityLogNum`)
) ENGINE=MyISAM AUTO_INCREMENT=3714940 DEFAULT CHARSET=utf8
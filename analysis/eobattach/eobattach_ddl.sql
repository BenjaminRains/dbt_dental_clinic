-- opendental_analytics_opendentalbackup_02_28_2025.eobattach definition

CREATE TABLE `eobattach` (
  `EobAttachNum` bigint(20) NOT NULL AUTO_INCREMENT,
  `ClaimPaymentNum` bigint(20) NOT NULL,
  `DateTCreated` datetime /* mariadb-5.3 */ NOT NULL,
  `FileName` varchar(255) NOT NULL,
  `RawBase64` text NOT NULL,
  PRIMARY KEY (`EobAttachNum`),
  KEY `ClaimPaymentNum` (`ClaimPaymentNum`)
) ENGINE=MyISAM AUTO_INCREMENT=17651 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
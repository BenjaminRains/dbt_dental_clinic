-- opendental_analytics_opendentalbackup_01_03_2025.sheetfielddef definition

CREATE TABLE `sheetfielddef` (
  `SheetFieldDefNum` bigint(20) NOT NULL AUTO_INCREMENT,
  `SheetDefNum` bigint(20) NOT NULL,
  `FieldType` int(11) NOT NULL,
  `FieldName` varchar(255) DEFAULT NULL,
  `FieldValue` text NOT NULL,
  `FontSize` float NOT NULL,
  `FontName` varchar(255) DEFAULT NULL,
  `FontIsBold` tinyint(4) NOT NULL,
  `XPos` int(11) NOT NULL,
  `YPos` int(11) NOT NULL,
  `Width` int(11) NOT NULL,
  `Height` int(11) NOT NULL,
  `GrowthBehavior` int(11) NOT NULL,
  `RadioButtonValue` varchar(255) NOT NULL,
  `RadioButtonGroup` varchar(255) NOT NULL,
  `IsRequired` tinyint(4) NOT NULL,
  `TabOrder` int(11) NOT NULL,
  `ReportableName` varchar(255) DEFAULT NULL,
  `TextAlign` tinyint(4) NOT NULL,
  `IsPaymentOption` tinyint(4) NOT NULL,
  `ItemColor` int(11) NOT NULL DEFAULT -16777216,
  `IsLocked` tinyint(4) NOT NULL,
  `TabOrderMobile` int(11) NOT NULL,
  `UiLabelMobile` text NOT NULL,
  `UiLabelMobileRadioButton` text NOT NULL,
  `LayoutMode` tinyint(4) NOT NULL,
  `Language` varchar(255) NOT NULL,
  `CanElectronicallySign` tinyint(4) NOT NULL,
  `IsSigProvRestricted` tinyint(4) NOT NULL,
  PRIMARY KEY (`SheetFieldDefNum`),
  KEY `SheetDefNum` (`SheetDefNum`)
) ENGINE=MyISAM AUTO_INCREMENT=2137 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_uca1400_ai_ci;
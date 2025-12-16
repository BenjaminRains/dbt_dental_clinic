-- DROP SCHEMA raw;

CREATE SCHEMA IF NOT EXISTS raw AUTHORIZATION postgres;

-- DROP TYPE raw.gtrgm;

-- Note: gtrgm type is provided by pg_trgm extension, no need to create manually

-- DROP SEQUENCE raw."appointment_AptNum_seq";

CREATE SEQUENCE raw."appointment_AptNum_seq"
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 9223372036854775807
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE raw.etl_load_status_id_seq;

CREATE SEQUENCE raw.etl_load_status_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE raw.etl_pipeline_metrics_id_seq;

CREATE SEQUENCE raw.etl_pipeline_metrics_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE raw.etl_table_metrics_id_seq;

CREATE SEQUENCE raw.etl_table_metrics_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE raw.etl_transform_status_id_seq;

CREATE SEQUENCE raw.etl_transform_status_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE raw."patient_PatNum_seq";

CREATE SEQUENCE raw."patient_PatNum_seq"
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 9223372036854775807
	START 1
	CACHE 1
	NO CYCLE;-- raw.account definition

-- Drop table

-- DROP TABLE raw.account;

CREATE TABLE raw.account (
	"AccountNum" int8 NOT NULL,
	"Description" varchar(255) NULL,
	"AcctType" int2 NULL,
	"BankNumber" varchar(255) NULL,
	"Inactive" bool NULL,
	"AccountColor" int4 NULL,
	"IsRetainedEarnings" bool NULL,
	CONSTRAINT account_pkey PRIMARY KEY ("AccountNum")
);


-- raw.accountingautopay definition

-- Drop table

-- DROP TABLE raw.accountingautopay;

CREATE TABLE raw.accountingautopay (
	"AccountingAutoPayNum" int8 NOT NULL,
	"PayType" int8 NULL,
	"PickList" varchar(255) NULL,
	CONSTRAINT accountingautopay_pkey PRIMARY KEY ("AccountingAutoPayNum")
);


-- raw.activeinstance definition

-- Drop table

-- DROP TABLE raw.activeinstance;

CREATE TABLE raw.activeinstance (
	"ActiveInstanceNum" int8 NOT NULL,
	"ComputerNum" int8 NULL,
	"UserNum" int8 NULL,
	"ProcessId" int8 NULL,
	"DateTimeLastActive" timestamp NULL,
	"DateTRecorded" timestamp NULL,
	"ConnectionType" bool NULL,
	CONSTRAINT activeinstance_pkey PRIMARY KEY ("ActiveInstanceNum")
);


-- raw.adjustment definition

-- Drop table

-- DROP TABLE raw.adjustment;

CREATE TABLE raw.adjustment (
	"AdjNum" int8 NOT NULL,
	"AdjDate" date NULL,
	"AdjAmt" float8 NULL,
	"PatNum" int8 NULL,
	"AdjType" int8 NULL,
	"ProvNum" int8 NULL,
	"AdjNote" text NULL,
	"ProcDate" date NULL,
	"ProcNum" int8 NULL,
	"DateEntry" date NULL,
	"ClinicNum" int8 NULL,
	"StatementNum" int8 NULL,
	"SecUserNumEntry" int8 NULL,
	"SecDateTEdit" timestamp NULL,
	"TaxTransID" int8 NULL,
	CONSTRAINT adjustment_pkey PRIMARY KEY ("AdjNum")
);


-- raw.alertcategory definition

-- Drop table

-- DROP TABLE raw.alertcategory;

CREATE TABLE raw.alertcategory (
	"AlertCategoryNum" int8 NOT NULL,
	"IsHQCategory" bool NULL,
	"InternalName" varchar(255) NULL,
	"Description" varchar(255) NULL,
	CONSTRAINT alertcategory_pkey PRIMARY KEY ("AlertCategoryNum")
);


-- raw.alertcategorylink definition

-- Drop table

-- DROP TABLE raw.alertcategorylink;

CREATE TABLE raw.alertcategorylink (
	"AlertCategoryLinkNum" int8 NOT NULL,
	"AlertCategoryNum" int8 NULL,
	"AlertType" int2 NULL,
	CONSTRAINT alertcategorylink_pkey PRIMARY KEY ("AlertCategoryLinkNum")
);


-- raw.alertitem definition

-- Drop table

-- DROP TABLE raw.alertitem;

CREATE TABLE raw.alertitem (
	"AlertItemNum" int8 NOT NULL,
	"ClinicNum" int8 NULL,
	"Description" varchar(2000) NULL,
	"Type" int2 NULL,
	"Severity" bool NULL,
	"Actions" int2 NULL,
	"FormToOpen" int2 NULL,
	"FKey" int8 NULL,
	"ItemValue" varchar(4000) NULL,
	"UserNum" int8 NULL,
	"SecDateTEntry" timestamp NULL,
	CONSTRAINT alertitem_pkey PRIMARY KEY ("AlertItemNum")
);


-- raw.alertread definition

-- Drop table

-- DROP TABLE raw.alertread;

CREATE TABLE raw.alertread (
	"AlertReadNum" int8 NOT NULL,
	"AlertItemNum" int8 NULL,
	"UserNum" int8 NULL,
	CONSTRAINT alertread_pkey PRIMARY KEY ("AlertReadNum")
);


-- raw.alertsub definition

-- Drop table

-- DROP TABLE raw.alertsub;

CREATE TABLE raw.alertsub (
	"AlertSubNum" int8 NOT NULL,
	"UserNum" int8 NULL,
	"ClinicNum" int8 NULL,
	"Type" bool NULL,
	"AlertCategoryNum" int8 NULL,
	CONSTRAINT alertsub_pkey PRIMARY KEY ("AlertSubNum")
);


-- raw.allergy definition

-- Drop table

-- DROP TABLE raw.allergy;

CREATE TABLE raw.allergy (
	"AllergyNum" int8 NOT NULL,
	"AllergyDefNum" int8 NULL,
	"PatNum" int8 NULL,
	"Reaction" varchar(255) NULL,
	"StatusIsActive" bool NULL,
	"DateTStamp" timestamp NULL,
	"DateAdverseReaction" date NULL,
	"SnomedReaction" varchar(255) NULL,
	CONSTRAINT allergy_pkey PRIMARY KEY ("AllergyNum")
);


-- raw.allergydef definition

-- Drop table

-- DROP TABLE raw.allergydef;

CREATE TABLE raw.allergydef (
	"AllergyDefNum" int8 NOT NULL,
	"Description" varchar(255) NULL,
	"IsHidden" bool NULL,
	"DateTStamp" timestamp NULL,
	"SnomedType" int2 NULL,
	"MedicationNum" int8 NULL,
	"UniiCode" varchar(255) NULL,
	CONSTRAINT allergydef_pkey PRIMARY KEY ("AllergyDefNum")
);


-- raw.anestheticdata definition

-- Drop table

-- DROP TABLE raw.anestheticdata;

CREATE TABLE raw.anestheticdata (
	"AnestheticDataNum" int4 NOT NULL,
	"AnestheticRecordNum" int4 NULL,
	"AnesthOpen" bpchar(32) NULL,
	"AnesthClose" bpchar(32) NULL,
	"SurgOpen" bpchar(32) NULL,
	"SurgClose" bpchar(32) NULL,
	"Anesthetist" bpchar(32) NULL,
	"Surgeon" bpchar(32) NULL,
	"Asst" bpchar(32) NULL,
	"Circulator" bpchar(32) NULL,
	"VSMName" bpchar(20) NULL,
	"VSMSerNum" bpchar(20) NULL,
	"ASA" bpchar(3) NULL,
	"ASA_EModifier" bpchar(1) NULL,
	"O2LMin" int2 NULL,
	"N2OLMin" int2 NULL,
	"RteNasCan" bool NULL,
	"RteNasHood" bool NULL,
	"RteETT" bool NULL,
	"MedRouteIVCath" bool NULL,
	"MedRouteIVButtFly" bool NULL,
	"MedRouteIM" bool NULL,
	"MedRoutePO" bool NULL,
	"MedRouteNasal" bool NULL,
	"MedRouteRectal" bool NULL,
	"IVSite" bpchar(20) NULL,
	"IVGauge" int2 NULL,
	"IVSideR" int2 NULL,
	"IVSideL" int2 NULL,
	"IVAtt" int2 NULL,
	"IVF" bpchar(20) NULL,
	"IVFVol" float4 NULL,
	"MonBP" bool NULL,
	"MonSpO2" bool NULL,
	"MonEtCO2" bool NULL,
	"MonTemp" bool NULL,
	"MonPrecordial" bool NULL,
	"MonEKG" bool NULL,
	"Notes" text NULL,
	"PatWgt" int2 NULL,
	"WgtUnitsLbs" bool NULL,
	"WgtUnitsKgs" bool NULL,
	"PatHgt" int2 NULL,
	"EscortName" bpchar(32) NULL,
	"EscortCellNum" bpchar(13) NULL,
	"EscortRel" bpchar(16) NULL,
	"NPOTime" bpchar(5) NULL,
	"HgtUnitsIn" bool NULL,
	"HgtUnitsCm" bool NULL,
	"Signature" text NULL,
	"SigIsTopaz" bool NULL,
	CONSTRAINT anestheticdata_pkey PRIMARY KEY ("AnestheticDataNum")
);


-- raw.anestheticrecord definition

-- Drop table

-- DROP TABLE raw.anestheticrecord;

CREATE TABLE raw.anestheticrecord (
	"AnestheticRecordNum" int4 NOT NULL,
	"PatNum" int4 NULL,
	"AnestheticDate" timestamp NULL,
	"ProvNum" int2 NULL,
	CONSTRAINT anestheticrecord_pkey PRIMARY KEY ("AnestheticRecordNum")
);


-- raw.anesthmedsgiven definition

-- Drop table

-- DROP TABLE raw.anesthmedsgiven;

CREATE TABLE raw.anesthmedsgiven (
	"AnestheticMedNum" int4 NOT NULL,
	"AnestheticRecordNum" int4 NULL,
	"AnesthMedName" bpchar(32) NULL,
	"QtyGiven" float8 NULL,
	"QtyWasted" float8 NULL,
	"DoseTimeStamp" bpchar(32) NULL,
	"QtyOnHandOld" float8 NULL,
	"AnesthMedNum" int4 NULL,
	CONSTRAINT anesthmedsgiven_pkey PRIMARY KEY ("AnestheticMedNum")
);


-- raw.anesthmedsintake definition

-- Drop table

-- DROP TABLE raw.anesthmedsintake;

CREATE TABLE raw.anesthmedsintake (
	"AnestheticMedNum" int4 NOT NULL,
	"IntakeDate" timestamp NULL,
	"AnesthMedName" bpchar(32) NULL,
	"Qty" int4 NULL,
	"SupplierIDNum" bpchar(11) NULL,
	"InvoiceNum" bpchar(20) NULL,
	CONSTRAINT anesthmedsintake_pkey PRIMARY KEY ("AnestheticMedNum")
);


-- raw.anesthmedsinventory definition

-- Drop table

-- DROP TABLE raw.anesthmedsinventory;

CREATE TABLE raw.anesthmedsinventory (
	"AnestheticMedNum" int4 NOT NULL,
	"AnesthMedName" bpchar(30) NULL,
	"AnesthHowSupplied" bpchar(20) NULL,
	"QtyOnHand" float8 NULL,
	"DEASchedule" bpchar(3) NULL,
	CONSTRAINT anesthmedsinventory_pkey PRIMARY KEY ("AnestheticMedNum")
);


-- raw.anesthmedsinventoryadj definition

-- Drop table

-- DROP TABLE raw.anesthmedsinventoryadj;

CREATE TABLE raw.anesthmedsinventoryadj (
	"AdjustNum" int4 NOT NULL,
	"AnestheticMedNum" int4 NULL,
	"QtyAdj" float8 NULL,
	"UserNum" int4 NULL,
	"Notes" varchar(255) NULL,
	"TimeStamp" timestamp NULL,
	CONSTRAINT anesthmedsinventoryadj_pkey PRIMARY KEY ("AdjustNum")
);


-- raw.anesthmedsuppliers definition

-- Drop table

-- DROP TABLE raw.anesthmedsuppliers;

CREATE TABLE raw.anesthmedsuppliers (
	"SupplierIDNum" int4 NOT NULL,
	"SupplierName" varchar(255) NULL,
	"Phone" bpchar(13) NULL,
	"PhoneExt" bpchar(6) NULL,
	"Fax" bpchar(13) NULL,
	"Addr1" varchar(48) NULL,
	"Addr2" bpchar(32) NULL,
	"City" varchar(48) NULL,
	"State" bpchar(20) NULL,
	"Zip" bpchar(10) NULL,
	"Contact" bpchar(32) NULL,
	"WebSite" varchar(48) NULL,
	"Notes" text NULL,
	CONSTRAINT anesthmedsuppliers_pkey PRIMARY KEY ("SupplierIDNum")
);


-- raw.anesthscore definition

-- Drop table

-- DROP TABLE raw.anesthscore;

CREATE TABLE raw.anesthscore (
	"AnesthScoreNum" int4 NOT NULL,
	"AnestheticRecordNum" int4 NULL,
	"QActivity" int2 NULL,
	"QResp" int2 NULL,
	"QCirc" int2 NULL,
	"QConc" int2 NULL,
	"QColor" int2 NULL,
	"AnesthesiaScore" int2 NULL,
	"DischAmb" bool NULL,
	"DischWheelChr" bool NULL,
	"DischAmbulance" bool NULL,
	"DischCondStable" bool NULL,
	"DischCondUnStable" bool NULL,
	CONSTRAINT anesthscore_pkey PRIMARY KEY ("AnesthScoreNum")
);


-- raw.anesthvsdata definition

-- Drop table

-- DROP TABLE raw.anesthvsdata;

CREATE TABLE raw.anesthvsdata (
	"AnesthVSDataNum" int4 NOT NULL,
	"AnestheticRecordNum" int4 NULL,
	"PatNum" int4 NULL,
	"VSMName" bpchar(20) NULL,
	"VSMSerNum" bpchar(32) NULL,
	"BPSys" int2 NULL,
	"BPDias" int2 NULL,
	"BPMAP" int2 NULL,
	"HR" int2 NULL,
	"SpO2" int2 NULL,
	"EtCo2" int2 NULL,
	"Temp" int2 NULL,
	"VSTimeStamp" bpchar(32) NULL,
	"MessageID" varchar(50) NULL,
	"HL7Message" text NULL,
	CONSTRAINT anesthvsdata_pkey PRIMARY KEY ("AnesthVSDataNum")
);


-- raw.apikey definition

-- Drop table

-- DROP TABLE raw.apikey;

CREATE TABLE raw.apikey (
	"APIKeyNum" int8 NOT NULL,
	"CustApiKey" varchar(255) NULL,
	"DevName" varchar(255) NULL,
	CONSTRAINT apikey_pkey PRIMARY KEY ("APIKeyNum")
);


-- raw.apisubscription definition

-- Drop table

-- DROP TABLE raw.apisubscription;

CREATE TABLE raw.apisubscription (
	"ApiSubscriptionNum" int8 NOT NULL,
	"EndPointUrl" varchar(255) NULL,
	"Workstation" varchar(255) NULL,
	"CustomerKey" varchar(255) NULL,
	"WatchTable" varchar(255) NULL,
	"PollingSeconds" int4 NULL,
	"UiEventType" varchar(255) NULL,
	"DateTimeStart" timestamp NULL,
	"DateTimeStop" timestamp NULL,
	"Note" varchar(255) NULL,
	CONSTRAINT apisubscription_pkey PRIMARY KEY ("ApiSubscriptionNum")
);


-- raw.appointment definition

-- Drop table

-- DROP TABLE raw.appointment;

CREATE TABLE raw.appointment (
	"AptNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	"AptStatus" int2 NULL,
	"Pattern" varchar(255) NULL,
	"Confirmed" int8 NULL,
	"TimeLocked" bool NULL,
	"Op" int8 NULL,
	"Note" text NULL,
	"ProvNum" int8 NULL,
	"ProvHyg" int8 NULL,
	"AptDateTime" timestamp NULL,
	"NextAptNum" int8 NULL,
	"UnschedStatus" int8 NULL,
	"IsNewPatient" bool NULL,
	"ProcDescript" text NULL,
	"Assistant" int8 NULL,
	"ClinicNum" int8 NULL,
	"IsHygiene" bool NULL,
	"DateTStamp" timestamp NULL,
	"DateTimeArrived" timestamp NULL,
	"DateTimeSeated" timestamp NULL,
	"DateTimeDismissed" timestamp NULL,
	"InsPlan1" int8 NULL,
	"InsPlan2" int8 NULL,
	"DateTimeAskedToArrive" timestamp NULL,
	"ProcsColored" text NULL,
	"ColorOverride" int4 NULL,
	"AppointmentTypeNum" int8 NULL,
	"SecUserNumEntry" int8 NULL,
	"SecDateTEntry" timestamp NULL,
	"Priority" bool NULL,
	"ProvBarText" varchar(60) NULL,
	"PatternSecondary" varchar(255) NULL,
	"SecurityHash" varchar(255) NULL,
	"ItemOrderPlanned" int4 NULL,
	"IsMirrored" bool NULL,
	CONSTRAINT appointment_pkey PRIMARY KEY ("AptNum")
);


-- raw.appointmentrule definition

-- Drop table

-- DROP TABLE raw.appointmentrule;

CREATE TABLE raw.appointmentrule (
	"AppointmentRuleNum" int8 NOT NULL,
	"RuleDesc" varchar(255) NULL,
	"CodeStart" varchar(15) NULL,
	"CodeEnd" varchar(15) NULL,
	"IsEnabled" bool NULL,
	CONSTRAINT appointmentrule_pkey PRIMARY KEY ("AppointmentRuleNum")
);


-- raw.appointmenttype definition

-- Drop table

-- DROP TABLE raw.appointmenttype;

CREATE TABLE raw.appointmenttype (
	"AppointmentTypeNum" int8 NOT NULL,
	"AppointmentTypeName" varchar(255) NULL,
	"AppointmentTypeColor" int4 NULL,
	"ItemOrder" int4 NULL,
	"IsHidden" bool NULL,
	"Pattern" varchar(255) NULL,
	"CodeStr" varchar(4000) NULL,
	"CodeStrRequired" varchar(4000) NULL,
	"RequiredProcCodesNeeded" int2 NULL,
	"BlockoutTypes" varchar(255) NULL,
	CONSTRAINT appointmenttype_pkey PRIMARY KEY ("AppointmentTypeNum")
);


-- raw.apptfield definition

-- Drop table

-- DROP TABLE raw.apptfield;

CREATE TABLE raw.apptfield (
	"ApptFieldNum" int8 NOT NULL,
	"AptNum" int8 NULL,
	"FieldName" varchar(255) NULL,
	"FieldValue" text NULL,
	CONSTRAINT apptfield_pkey PRIMARY KEY ("ApptFieldNum")
);


-- raw.apptfielddef definition

-- Drop table

-- DROP TABLE raw.apptfielddef;

CREATE TABLE raw.apptfielddef (
	"ApptFieldDefNum" int8 NOT NULL,
	"FieldName" varchar(255) NULL,
	"FieldType" bool NULL,
	"PickList" text NULL,
	"ItemOrder" int4 NULL,
	CONSTRAINT apptfielddef_pkey PRIMARY KEY ("ApptFieldDefNum")
);


-- raw.apptgeneralmessagesent definition

-- Drop table

-- DROP TABLE raw.apptgeneralmessagesent;

CREATE TABLE raw.apptgeneralmessagesent (
	"ApptGeneralMessageSentNum" int8 NOT NULL,
	"ApptNum" int8 NULL,
	"PatNum" int8 NULL,
	"ClinicNum" int8 NULL,
	"DateTimeEntry" timestamp NULL,
	"TSPrior" int8 NULL,
	"ApptReminderRuleNum" int8 NULL,
	"SendStatus" int2 NULL,
	"ApptDateTime" timestamp NULL,
	"MessageType" int2 NULL,
	"MessageFk" int8 NULL,
	"DateTimeSent" timestamp NULL,
	"ResponseDescript" text NULL,
	CONSTRAINT apptgeneralmessagesent_pkey PRIMARY KEY ("ApptGeneralMessageSentNum")
);


-- raw.apptnewpatthankyousent definition

-- Drop table

-- DROP TABLE raw.apptnewpatthankyousent;

CREATE TABLE raw.apptnewpatthankyousent (
	"ApptNewPatThankYouSentNum" int8 NOT NULL,
	"ApptNum" int8 NULL,
	"ApptDateTime" timestamp NULL,
	"ApptSecDateTEntry" timestamp NULL,
	"TSPrior" int8 NULL,
	"ApptReminderRuleNum" int8 NULL,
	"ClinicNum" int8 NULL,
	"PatNum" int8 NULL,
	"ResponseDescript" text NULL,
	"DateTimeNewPatThankYouTransmit" timestamp NULL,
	"ShortGUID" varchar(255) NULL,
	"SendStatus" bool NULL,
	"MessageType" bool NULL,
	"MessageFk" int8 NULL,
	"DateTimeEntry" timestamp NULL,
	"DateTimeSent" timestamp NULL,
	CONSTRAINT apptnewpatthankyousent_pkey PRIMARY KEY ("ApptNewPatThankYouSentNum")
);


-- raw.apptreminderrule definition

-- Drop table

-- DROP TABLE raw.apptreminderrule;

CREATE TABLE raw.apptreminderrule (
	"ApptReminderRuleNum" int8 NOT NULL,
	"TypeCur" int2 NULL,
	"TSPrior" int8 NULL,
	"SendOrder" varchar(255) NULL,
	"IsSendAll" bool NULL,
	"TemplateSMS" text NULL,
	"TemplateEmailSubject" text NULL,
	"TemplateEmail" text NULL,
	"ClinicNum" int8 NULL,
	"TemplateSMSAggShared" text NULL,
	"TemplateSMSAggPerAppt" text NULL,
	"TemplateEmailSubjAggShared" text NULL,
	"TemplateEmailAggShared" text NULL,
	"TemplateEmailAggPerAppt" text NULL,
	"DoNotSendWithin" int8 NULL,
	"IsEnabled" bool NULL,
	"TemplateAutoReply" text NULL,
	"TemplateAutoReplyAgg" text NULL,
	"IsAutoReplyEnabled" bool NULL,
	"Language" varchar(255) NULL,
	"TemplateComeInMessage" text NULL,
	"EmailTemplateType" varchar(255) NULL,
	"AggEmailTemplateType" varchar(255) NULL,
	"IsSendForMinorsBirthday" bool NULL,
	"EmailHostingTemplateNum" int8 NULL,
	"MinorAge" int4 NULL,
	"TemplateFailureAutoReply" text NULL,
	"SendMultipleInvites" bool NULL,
	"TimeSpanMultipleInvites" int8 NULL,
	CONSTRAINT apptreminderrule_pkey PRIMARY KEY ("ApptReminderRuleNum")
);


-- raw.apptremindersent definition

-- Drop table

-- DROP TABLE raw.apptremindersent;

CREATE TABLE raw.apptremindersent (
	"ApptReminderSentNum" int8 NOT NULL,
	"ApptNum" int8 NULL,
	"ApptDateTime" timestamp NULL,
	"DateTimeSent" timestamp NULL,
	"TSPrior" int8 NULL,
	"ApptReminderRuleNum" int8 NULL,
	"PatNum" int8 NULL,
	"ClinicNum" int8 NULL,
	"SendStatus" bool NULL,
	"MessageType" bool NULL,
	"MessageFk" int8 NULL,
	"DateTimeEntry" timestamp NULL,
	"ResponseDescript" text NULL,
	CONSTRAINT apptremindersent_pkey PRIMARY KEY ("ApptReminderSentNum")
);


-- raw.apptthankyousent definition

-- Drop table

-- DROP TABLE raw.apptthankyousent;

CREATE TABLE raw.apptthankyousent (
	"ApptThankYouSentNum" int8 NOT NULL,
	"ApptNum" int8 NULL,
	"ApptDateTime" timestamp NULL,
	"ApptSecDateTEntry" timestamp NULL,
	"TSPrior" int8 NULL,
	"ApptReminderRuleNum" int8 NULL,
	"ClinicNum" int8 NULL,
	"PatNum" int8 NULL,
	"ResponseDescript" text NULL,
	"DateTimeThankYouTransmit" timestamp NULL,
	"ShortGUID" varchar(255) NULL,
	"SendStatus" int2 NULL,
	"DoNotResend" bool NULL,
	"MessageType" int2 NULL,
	"MessageFk" int8 NULL,
	"DateTimeEntry" timestamp NULL,
	"DateTimeSent" timestamp NULL,
	CONSTRAINT apptthankyousent_pkey PRIMARY KEY ("ApptThankYouSentNum")
);


-- raw.apptview definition

-- Drop table

-- DROP TABLE raw.apptview;

CREATE TABLE raw.apptview (
	"ApptViewNum" int8 NOT NULL,
	"Description" varchar(255) NULL,
	"ItemOrder" int2 NULL,
	"RowsPerIncr" bool NULL,
	"OnlyScheduledProvs" bool NULL,
	"OnlySchedBeforeTime" time NULL,
	"OnlySchedAfterTime" time NULL,
	"StackBehavUR" int2 NULL,
	"StackBehavLR" int2 NULL,
	"ClinicNum" int8 NULL,
	"ApptTimeScrollStart" time NULL,
	"IsScrollStartDynamic" bool NULL,
	"IsApptBubblesDisabled" bool NULL,
	"WidthOpMinimum" int2 NULL,
	"WaitingRmName" bool NULL,
	"OnlyScheduledProvDays" bool NULL,
	CONSTRAINT apptview_pkey PRIMARY KEY ("ApptViewNum")
);


-- raw.apptviewitem definition

-- Drop table

-- DROP TABLE raw.apptviewitem;

CREATE TABLE raw.apptviewitem (
	"ApptViewItemNum" int8 NOT NULL,
	"ApptViewNum" int8 NULL,
	"OpNum" int8 NULL,
	"ProvNum" int8 NULL,
	"ElementDesc" varchar(255) NULL,
	"ElementOrder" int2 NULL,
	"ElementColor" int4 NULL,
	"ElementAlignment" int2 NULL,
	"ApptFieldDefNum" int8 NULL,
	"PatFieldDefNum" int8 NULL,
	"IsMobile" bool NULL,
	CONSTRAINT apptviewitem_pkey PRIMARY KEY ("ApptViewItemNum")
);


-- raw.asapcomm definition

-- Drop table

-- DROP TABLE raw.asapcomm;

CREATE TABLE raw.asapcomm (
	"AsapCommNum" int8 NOT NULL,
	"FKey" int8 NULL,
	"FKeyType" bool NULL,
	"ScheduleNum" int8 NULL,
	"PatNum" int8 NULL,
	"ClinicNum" int8 NULL,
	"ShortGUID" varchar(255) NULL,
	"DateTimeEntry" timestamp NULL,
	"DateTimeExpire" timestamp NULL,
	"DateTimeSmsScheduled" timestamp NULL,
	"SmsSendStatus" bool NULL,
	"EmailSendStatus" int2 NULL,
	"DateTimeSmsSent" timestamp NULL,
	"DateTimeEmailSent" timestamp NULL,
	"EmailMessageNum" int8 NULL,
	"ResponseStatus" int2 NULL,
	"DateTimeOrig" timestamp NULL,
	"TemplateText" text NULL,
	"TemplateEmail" text NULL,
	"TemplateEmailSubj" varchar(100) NULL,
	"Note" text NULL,
	"GuidMessageToMobile" text NULL,
	"EmailTemplateType" varchar(255) NULL,
	CONSTRAINT asapcomm_pkey PRIMARY KEY ("AsapCommNum")
);


-- raw.autocode definition

-- Drop table

-- DROP TABLE raw.autocode;

CREATE TABLE raw.autocode (
	"AutoCodeNum" int8 NOT NULL,
	"Description" varchar(255) NULL,
	"IsHidden" bool NULL,
	"LessIntrusive" bool NULL,
	CONSTRAINT autocode_pkey PRIMARY KEY ("AutoCodeNum")
);


-- raw.autocodecond definition

-- Drop table

-- DROP TABLE raw.autocodecond;

CREATE TABLE raw.autocodecond (
	"AutoCodeCondNum" int8 NOT NULL,
	"AutoCodeItemNum" int8 NULL,
	"Cond" int2 NULL,
	CONSTRAINT autocodecond_pkey PRIMARY KEY ("AutoCodeCondNum")
);


-- raw.autocodeitem definition

-- Drop table

-- DROP TABLE raw.autocodeitem;

CREATE TABLE raw.autocodeitem (
	"AutoCodeItemNum" int8 NOT NULL,
	"AutoCodeNum" int8 NULL,
	"OldCode" varchar(15) NULL,
	"CodeNum" int8 NULL,
	CONSTRAINT autocodeitem_pkey PRIMARY KEY ("AutoCodeItemNum")
);


-- raw.autocommexcludedate definition

-- Drop table

-- DROP TABLE raw.autocommexcludedate;

CREATE TABLE raw.autocommexcludedate (
	"AutoCommExcludeDateNum" int8 NOT NULL,
	"ClinicNum" int8 NULL,
	"DateExclude" timestamp NULL,
	CONSTRAINT autocommexcludedate_pkey PRIMARY KEY ("AutoCommExcludeDateNum")
);


-- raw.automation definition

-- Drop table

-- DROP TABLE raw.automation;

CREATE TABLE raw.automation (
	"AutomationNum" int8 NOT NULL,
	"Description" text NULL,
	"Autotrigger" int2 NULL,
	"ProcCodes" text NULL,
	"AutoAction" int2 NULL,
	"SheetDefNum" int8 NULL,
	"CommType" int8 NULL,
	"MessageContent" text NULL,
	"AptStatus" bool NULL,
	"AppointmentTypeNum" int8 NULL,
	"PatStatus" bool NULL,
	CONSTRAINT automation_pkey PRIMARY KEY ("AutomationNum")
);


-- raw.automationcondition definition

-- Drop table

-- DROP TABLE raw.automationcondition;

CREATE TABLE raw.automationcondition (
	"AutomationConditionNum" int8 NOT NULL,
	"AutomationNum" int8 NULL,
	"CompareField" bool NULL,
	"Comparison" bool NULL,
	"CompareString" varchar(255) NULL,
	CONSTRAINT automationcondition_pkey PRIMARY KEY ("AutomationConditionNum")
);


-- raw.autonote definition

-- Drop table

-- DROP TABLE raw.autonote;

CREATE TABLE raw.autonote (
	"AutoNoteNum" int8 NOT NULL,
	"AutoNoteName" varchar(50) NULL,
	"MainText" text NULL,
	"Category" int8 NULL,
	CONSTRAINT autonote_pkey PRIMARY KEY ("AutoNoteNum")
);


-- raw.autonotecontrol definition

-- Drop table

-- DROP TABLE raw.autonotecontrol;

CREATE TABLE raw.autonotecontrol (
	"AutoNoteControlNum" int8 NOT NULL,
	"Descript" varchar(50) NULL,
	"ControlType" varchar(50) NULL,
	"ControlLabel" varchar(255) NULL,
	"ControlOptions" text NULL,
	CONSTRAINT autonotecontrol_pkey PRIMARY KEY ("AutoNoteControlNum")
);


-- raw.benefit definition

-- Drop table

-- DROP TABLE raw.benefit;

CREATE TABLE raw.benefit (
	"BenefitNum" int8 NOT NULL,
	"PlanNum" int8 NULL,
	"PatPlanNum" int8 NULL,
	"CovCatNum" int8 NULL,
	"BenefitType" int2 NULL,
	"Percent" int2 NULL,
	"MonetaryAmt" float8 NULL,
	"TimePeriod" int2 NULL,
	"QuantityQualifier" int2 NULL,
	"Quantity" int2 NULL,
	"CodeNum" int8 NULL,
	"CoverageLevel" int4 NULL,
	"SecDateTEntry" timestamp NULL,
	"SecDateTEdit" timestamp NULL,
	"CodeGroupNum" int8 NULL,
	"TreatArea" int2 NULL,
	CONSTRAINT benefit_pkey PRIMARY KEY ("BenefitNum")
);


-- raw.branding definition

-- Drop table

-- DROP TABLE raw.branding;

CREATE TABLE raw.branding (
	"BrandingNum" int8 NOT NULL,
	"BrandingType" int2 NULL,
	"ClinicNum" int8 NULL,
	"ValueString" text NULL,
	"DateTimeUpdated" timestamp NULL,
	CONSTRAINT branding_pkey PRIMARY KEY ("BrandingNum")
);


-- raw.canadiannetwork definition

-- Drop table

-- DROP TABLE raw.canadiannetwork;

CREATE TABLE raw.canadiannetwork (
	"CanadianNetworkNum" int8 NOT NULL,
	"Abbrev" varchar(20) NULL,
	"Descript" varchar(255) NULL,
	"CanadianTransactionPrefix" varchar(255) NULL,
	"CanadianIsRprHandler" bool NULL,
	CONSTRAINT canadiannetwork_pkey PRIMARY KEY ("CanadianNetworkNum")
);


-- raw.carecreditwebresponse definition

-- Drop table

-- DROP TABLE raw.carecreditwebresponse;

CREATE TABLE raw.carecreditwebresponse (
	"CareCreditWebResponseNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	"PayNum" int8 NULL,
	"RefNumber" varchar(255) NULL,
	"Amount" float8 NULL,
	"WebToken" varchar(255) NULL,
	"ProcessingStatus" varchar(255) NULL,
	"DateTimeEntry" timestamp NULL,
	"DateTimePending" timestamp NULL,
	"DateTimeCompleted" timestamp NULL,
	"DateTimeExpired" timestamp NULL,
	"DateTimeLastError" timestamp NULL,
	"LastResponseStr" text NULL,
	"ClinicNum" int8 NULL,
	"ServiceType" varchar(255) NULL,
	"TransType" varchar(255) NULL,
	"MerchantNumber" varchar(20) NULL,
	"HasLogged" bool NULL,
	CONSTRAINT carecreditwebresponse_pkey PRIMARY KEY ("CareCreditWebResponseNum")
);


-- raw.carrier definition

-- Drop table

-- DROP TABLE raw.carrier;

CREATE TABLE raw.carrier (
	"CarrierNum" int8 NOT NULL,
	"CarrierName" varchar(255) NULL,
	"Address" varchar(255) NULL,
	"Address2" varchar(255) NULL,
	"City" varchar(255) NULL,
	"State" varchar(255) NULL,
	"Zip" varchar(255) NULL,
	"Phone" varchar(255) NULL,
	"ElectID" varchar(255) NULL,
	"NoSendElect" int2 NULL,
	"IsCDA" bool NULL,
	"CDAnetVersion" varchar(100) NULL,
	"CanadianNetworkNum" int8 NULL,
	"IsHidden" bool NULL,
	"CanadianEncryptionMethod" bool NULL,
	"CanadianSupportedTypes" int4 NULL,
	"SecUserNumEntry" int8 NULL,
	"SecDateEntry" date NULL,
	"SecDateTEdit" timestamp NULL,
	"TIN" varchar(255) NULL,
	"CarrierGroupName" int8 NULL,
	"ApptTextBackColor" int4 NULL,
	"IsCoinsuranceInverted" bool NULL,
	"TrustedEtransFlags" bool NULL,
	"CobInsPaidBehaviorOverride" bool NULL,
	"EraAutomationOverride" bool NULL,
	"OrthoInsPayConsolidate" bool NULL,
	CONSTRAINT carrier_pkey PRIMARY KEY ("CarrierNum")
);


-- raw.cdcrec definition

-- Drop table

-- DROP TABLE raw.cdcrec;

CREATE TABLE raw.cdcrec (
	"CdcrecNum" int8 NOT NULL,
	"CdcrecCode" varchar(255) NULL,
	"HeirarchicalCode" varchar(255) NULL,
	"Description" varchar(255) NULL,
	CONSTRAINT cdcrec_pkey PRIMARY KEY ("CdcrecNum")
);


-- raw.cdspermission definition

-- Drop table

-- DROP TABLE raw.cdspermission;

CREATE TABLE raw.cdspermission (
	"CDSPermissionNum" int8 NOT NULL,
	"UserNum" int8 NULL,
	"SetupCDS" bool NULL,
	"ShowCDS" bool NULL,
	"ShowInfobutton" bool NULL,
	"EditBibliography" bool NULL,
	"ProblemCDS" bool NULL,
	"MedicationCDS" bool NULL,
	"AllergyCDS" bool NULL,
	"DemographicCDS" bool NULL,
	"LabTestCDS" bool NULL,
	"VitalCDS" bool NULL,
	CONSTRAINT cdspermission_pkey PRIMARY KEY ("CDSPermissionNum")
);


-- raw.centralconnection definition

-- Drop table

-- DROP TABLE raw.centralconnection;

CREATE TABLE raw.centralconnection (
	"CentralConnectionNum" int8 NOT NULL,
	"ServerName" varchar(255) NULL,
	"DatabaseName" varchar(255) NULL,
	"MySqlUser" varchar(255) NULL,
	"MySqlPassword" varchar(255) NULL,
	"ServiceURI" varchar(255) NULL,
	"OdUser" varchar(255) NULL,
	"OdPassword" varchar(255) NULL,
	"Note" text NULL,
	"ItemOrder" int4 NULL,
	"WebServiceIsEcw" bool NULL,
	"ConnectionStatus" varchar(255) NULL,
	"HasClinicBreakdownReports" bool NULL,
	CONSTRAINT centralconnection_pkey PRIMARY KEY ("CentralConnectionNum")
);


-- raw.cert definition

-- Drop table

-- DROP TABLE raw.cert;

CREATE TABLE raw.cert (
	"CertNum" int8 NOT NULL,
	"Description" varchar(255) NULL,
	"WikiPageLink" varchar(255) NULL,
	"ItemOrder" int4 NULL,
	"IsHidden" bool NULL,
	"CertCategoryNum" int8 NULL,
	CONSTRAINT cert_pkey PRIMARY KEY ("CertNum")
);


-- raw.certemployee definition

-- Drop table

-- DROP TABLE raw.certemployee;

CREATE TABLE raw.certemployee (
	"CertEmployeeNum" int8 NOT NULL,
	"CertNum" int8 NULL,
	"EmployeeNum" int8 NULL,
	"DateCompleted" date NULL,
	"Note" varchar(255) NULL,
	"UserNum" int8 NULL,
	CONSTRAINT certemployee_pkey PRIMARY KEY ("CertEmployeeNum")
);


-- raw.chartview definition

-- Drop table

-- DROP TABLE raw.chartview;

CREATE TABLE raw.chartview (
	"ChartViewNum" int8 NOT NULL,
	"Description" varchar(255) NULL,
	"ItemOrder" int4 NULL,
	"ProcStatuses" int2 NULL,
	"ObjectTypes" int2 NULL,
	"ShowProcNotes" bool NULL,
	"IsAudit" bool NULL,
	"SelectedTeethOnly" bool NULL,
	"OrionStatusFlags" int4 NULL,
	"DatesShowing" bool NULL,
	"IsTpCharting" bool NULL,
	CONSTRAINT chartview_pkey PRIMARY KEY ("ChartViewNum")
);


-- raw.claim definition

-- Drop table

-- DROP TABLE raw.claim;

CREATE TABLE raw.claim (
	"ClaimNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	"DateService" date NULL,
	"DateSent" date NULL,
	"ClaimStatus" bpchar(1) NULL,
	"DateReceived" date NULL,
	"PlanNum" int8 NULL,
	"ProvTreat" int8 NULL,
	"ClaimFee" float8 NULL,
	"InsPayEst" float8 NULL,
	"InsPayAmt" float8 NULL,
	"DedApplied" float8 NULL,
	"PreAuthString" varchar(40) NULL,
	"IsProsthesis" bpchar(1) NULL,
	"PriorDate" date NULL,
	"ReasonUnderPaid" varchar(255) NULL,
	"ClaimNote" varchar(400) NULL,
	"ClaimType" varchar(255) NULL,
	"ProvBill" int8 NULL,
	"ReferringProv" int8 NULL,
	"RefNumString" varchar(40) NULL,
	"PlaceService" int2 NULL,
	"AccidentRelated" bpchar(1) NULL,
	"AccidentDate" date NULL,
	"AccidentST" varchar(2) NULL,
	"EmployRelated" int2 NULL,
	"IsOrtho" bool NULL,
	"OrthoRemainM" bool NULL,
	"OrthoDate" date NULL,
	"PatRelat" int2 NULL,
	"PlanNum2" int8 NULL,
	"PatRelat2" int2 NULL,
	"WriteOff" float8 NULL,
	"Radiographs" int2 NULL,
	"ClinicNum" int8 NULL,
	"ClaimForm" int8 NULL,
	"AttachedImages" int4 NULL,
	"AttachedModels" int4 NULL,
	"AttachedFlags" varchar(255) NULL,
	"AttachmentID" varchar(255) NULL,
	"CanadianMaterialsForwarded" varchar(10) NULL,
	"CanadianReferralProviderNum" varchar(20) NULL,
	"CanadianReferralReason" bool NULL,
	"CanadianIsInitialLower" varchar(5) NULL,
	"CanadianDateInitialLower" date NULL,
	"CanadianMandProsthMaterial" bool NULL,
	"CanadianIsInitialUpper" varchar(5) NULL,
	"CanadianDateInitialUpper" date NULL,
	"CanadianMaxProsthMaterial" bool NULL,
	"InsSubNum" int8 NULL,
	"InsSubNum2" int8 NULL,
	"CanadaTransRefNum" varchar(255) NULL,
	"CanadaEstTreatStartDate" date NULL,
	"CanadaInitialPayment" float8 NULL,
	"CanadaPaymentMode" bool NULL,
	"CanadaTreatDuration" bool NULL,
	"CanadaNumAnticipatedPayments" bool NULL,
	"CanadaAnticipatedPayAmount" float8 NULL,
	"PriorAuthorizationNumber" varchar(255) NULL,
	"SpecialProgramCode" bool NULL,
	"UniformBillType" varchar(255) NULL,
	"MedType" bool NULL,
	"AdmissionTypeCode" varchar(255) NULL,
	"AdmissionSourceCode" varchar(255) NULL,
	"PatientStatusCode" varchar(255) NULL,
	"CustomTracking" int8 NULL,
	"DateResent" date NULL,
	"CorrectionType" bool NULL,
	"ClaimIdentifier" varchar(255) NULL,
	"OrigRefNum" varchar(255) NULL,
	"ProvOrderOverride" int8 NULL,
	"OrthoTotalM" bool NULL,
	"ShareOfCost" float8 NULL,
	"SecUserNumEntry" int8 NULL,
	"SecDateEntry" date NULL,
	"SecDateTEdit" timestamp NULL,
	"OrderingReferralNum" int8 NULL,
	"DateSentOrig" date NULL,
	"DateIllnessInjuryPreg" date NULL,
	"DateIllnessInjuryPregQualifier" int2 NULL,
	"DateOther" date NULL,
	"DateOtherQualifier" int2 NULL,
	"IsOutsideLab" bool NULL,
	"SecurityHash" varchar(255) NULL,
	"Narrative" text NULL,
	CONSTRAINT claim_pkey PRIMARY KEY ("ClaimNum")
);


-- raw.claimattach definition

-- Drop table

-- DROP TABLE raw.claimattach;

CREATE TABLE raw.claimattach (
	"ClaimAttachNum" int8 NOT NULL,
	"ClaimNum" int8 NULL,
	"DisplayedFileName" varchar(255) NULL,
	"ActualFileName" varchar(255) NULL,
	"ImageReferenceId" int4 NULL,
	CONSTRAINT claimattach_pkey PRIMARY KEY ("ClaimAttachNum")
);


-- raw.claimcondcodelog definition

-- Drop table

-- DROP TABLE raw.claimcondcodelog;

CREATE TABLE raw.claimcondcodelog (
	"ClaimCondCodeLogNum" int8 NOT NULL,
	"ClaimNum" int8 NULL,
	"Code0" varchar(2) NULL,
	"Code1" varchar(2) NULL,
	"Code2" varchar(2) NULL,
	"Code3" varchar(2) NULL,
	"Code4" varchar(2) NULL,
	"Code5" varchar(2) NULL,
	"Code6" varchar(2) NULL,
	"Code7" varchar(2) NULL,
	"Code8" varchar(2) NULL,
	"Code9" varchar(2) NULL,
	"Code10" varchar(2) NULL,
	CONSTRAINT claimcondcodelog_pkey PRIMARY KEY ("ClaimCondCodeLogNum")
);


-- raw.claimform definition

-- Drop table

-- DROP TABLE raw.claimform;

CREATE TABLE raw.claimform (
	"ClaimFormNum" int8 NOT NULL,
	"Description" varchar(50) NULL,
	"IsHidden" bool NULL,
	"FontName" varchar(255) NULL,
	"FontSize" float4 NULL,
	"UniqueID" varchar(255) NULL,
	"PrintImages" bool NULL,
	"OffsetX" int2 NULL,
	"OffsetY" int2 NULL,
	"Width" int4 NULL,
	"Height" int4 NULL,
	CONSTRAINT claimform_pkey PRIMARY KEY ("ClaimFormNum")
);


-- raw.claimformitem definition

-- Drop table

-- DROP TABLE raw.claimformitem;

CREATE TABLE raw.claimformitem (
	"ClaimFormItemNum" int8 NOT NULL,
	"ClaimFormNum" int8 NULL,
	"ImageFileName" varchar(255) NULL,
	"FieldName" varchar(255) NULL,
	"FormatString" varchar(255) NULL,
	"XPos" float4 NULL,
	"YPos" float4 NULL,
	"Width" float4 NULL,
	"Height" float4 NULL,
	CONSTRAINT claimformitem_pkey PRIMARY KEY ("ClaimFormItemNum")
);


-- raw.claimpayment definition

-- Drop table

-- DROP TABLE raw.claimpayment;

CREATE TABLE raw.claimpayment (
	"ClaimPaymentNum" int8 NOT NULL,
	"CheckDate" date NULL,
	"CheckAmt" float8 NULL,
	"CheckNum" varchar(25) NULL,
	"BankBranch" varchar(25) NULL,
	"Note" varchar(255) NULL,
	"ClinicNum" int8 NULL,
	"DepositNum" int8 NULL,
	"CarrierName" varchar(255) NULL,
	"DateIssued" date NULL,
	"IsPartial" bool NULL,
	"PayType" int8 NULL,
	"SecUserNumEntry" int8 NULL,
	"SecDateEntry" date NULL,
	"SecDateTEdit" timestamp NULL,
	"PayGroup" int8 NULL,
	CONSTRAINT claimpayment_pkey PRIMARY KEY ("ClaimPaymentNum")
);


-- raw.claimproc definition

-- Drop table

-- DROP TABLE raw.claimproc;

CREATE TABLE raw.claimproc (
	"ClaimProcNum" int8 NOT NULL,
	"ProcNum" int8 NULL,
	"ClaimNum" int8 NULL,
	"PatNum" int8 NULL,
	"ProvNum" int8 NULL,
	"FeeBilled" float8 NULL,
	"InsPayEst" float8 NULL,
	"DedApplied" float8 NULL,
	"Status" int2 NULL,
	"InsPayAmt" float8 NULL,
	"Remarks" varchar(255) NULL,
	"ClaimPaymentNum" int8 NULL,
	"PlanNum" int8 NULL,
	"DateCP" date NULL,
	"WriteOff" float8 NULL,
	"CodeSent" varchar(15) NULL,
	"AllowedOverride" float8 NULL,
	"Percentage" int2 NULL,
	"PercentOverride" int2 NULL,
	"CopayAmt" float8 NULL,
	"NoBillIns" bool NULL,
	"PaidOtherIns" float8 NULL,
	"BaseEst" float8 NULL,
	"CopayOverride" float8 NULL,
	"ProcDate" date NULL,
	"DateEntry" date NULL,
	"LineNumber" int2 NULL,
	"DedEst" float8 NULL,
	"DedEstOverride" float8 NULL,
	"InsEstTotal" float8 NULL,
	"InsEstTotalOverride" float8 NULL,
	"PaidOtherInsOverride" float8 NULL,
	"EstimateNote" varchar(255) NULL,
	"WriteOffEst" float8 NULL,
	"WriteOffEstOverride" float8 NULL,
	"ClinicNum" int8 NULL,
	"InsSubNum" int8 NULL,
	"PaymentRow" int4 NULL,
	"PayPlanNum" int8 NULL,
	"ClaimPaymentTracking" int8 NULL,
	"SecUserNumEntry" int8 NULL,
	"SecDateEntry" date NULL,
	"SecDateTEdit" timestamp NULL,
	"DateSuppReceived" date NULL,
	"DateInsFinalized" date NULL,
	"IsTransfer" bool NULL,
	"ClaimAdjReasonCodes" varchar(255) NULL,
	"IsOverpay" bool NULL,
	"SecurityHash" varchar(255) NULL,
	CONSTRAINT claimproc_pkey PRIMARY KEY ("ClaimProcNum")
);


-- raw.claimsnapshot definition

-- Drop table

-- DROP TABLE raw.claimsnapshot;

CREATE TABLE raw.claimsnapshot (
	"ClaimSnapshotNum" int8 NOT NULL,
	"ProcNum" int8 NULL,
	"ClaimType" varchar(255) NULL,
	"Writeoff" float8 NULL,
	"InsPayEst" float8 NULL,
	"Fee" float8 NULL,
	"DateTEntry" timestamp NULL,
	"ClaimProcNum" int8 NULL,
	"SnapshotTrigger" bool NULL,
	CONSTRAINT claimsnapshot_pkey PRIMARY KEY ("ClaimSnapshotNum")
);


-- raw.claimtracking definition

-- Drop table

-- DROP TABLE raw.claimtracking;

CREATE TABLE raw.claimtracking (
	"ClaimTrackingNum" int8 NOT NULL,
	"ClaimNum" int8 NULL,
	"TrackingType" varchar(255) NULL,
	"UserNum" int8 NULL,
	"DateTimeEntry" timestamp NULL,
	"Note" text NULL,
	"TrackingDefNum" int8 NULL,
	"TrackingErrorDefNum" int8 NULL,
	CONSTRAINT claimtracking_pkey PRIMARY KEY ("ClaimTrackingNum")
);


-- raw.claimvalcodelog definition

-- Drop table

-- DROP TABLE raw.claimvalcodelog;

CREATE TABLE raw.claimvalcodelog (
	"ClaimValCodeLogNum" int8 NOT NULL,
	"ClaimNum" int8 NULL,
	"ClaimField" varchar(5) NULL,
	"ValCode" bpchar(2) NULL,
	"ValAmount" float8 NULL,
	"Ordinal" int4 NULL,
	CONSTRAINT claimvalcodelog_pkey PRIMARY KEY ("ClaimValCodeLogNum")
);


-- raw.clearinghouse definition

-- Drop table

-- DROP TABLE raw.clearinghouse;

CREATE TABLE raw.clearinghouse (
	"ClearinghouseNum" int8 NOT NULL,
	"Description" varchar(255) NULL,
	"ExportPath" text NULL,
	"Payors" text NULL,
	"Eformat" int2 NULL,
	"ISA05" varchar(255) NULL,
	"SenderTIN" varchar(255) NULL,
	"ISA07" varchar(255) NULL,
	"ISA08" varchar(255) NULL,
	"ISA15" varchar(255) NULL,
	"Password" varchar(255) NULL,
	"ResponsePath" varchar(255) NULL,
	"CommBridge" int2 NULL,
	"ClientProgram" varchar(255) NULL,
	"LastBatchNumber" int2 NULL,
	"ModemPort" bool NULL,
	"LoginID" varchar(255) NULL,
	"SenderName" varchar(255) NULL,
	"SenderTelephone" varchar(255) NULL,
	"GS03" varchar(255) NULL,
	"ISA02" varchar(10) NULL,
	"ISA04" varchar(10) NULL,
	"ISA16" varchar(2) NULL,
	"SeparatorData" varchar(2) NULL,
	"SeparatorSegment" varchar(2) NULL,
	"ClinicNum" int8 NULL,
	"HqClearinghouseNum" int8 NULL,
	"IsEraDownloadAllowed" int2 NULL,
	"IsClaimExportAllowed" bool NULL,
	"IsAttachmentSendAllowed" bool NULL,
	"LocationID" varchar(255) NULL,
	CONSTRAINT clearinghouse_pkey PRIMARY KEY ("ClearinghouseNum")
);


-- raw.clinic definition

-- Drop table

-- DROP TABLE raw.clinic;

CREATE TABLE raw.clinic (
	"ClinicNum" int8 NOT NULL,
	"Description" varchar(255) NULL,
	"Address" varchar(255) NULL,
	"Address2" varchar(255) NULL,
	"City" varchar(255) NULL,
	"State" varchar(255) NULL,
	"Zip" varchar(255) NULL,
	"Phone" varchar(255) NULL,
	"BankNumber" varchar(255) NULL,
	"DefaultPlaceService" bool NULL,
	"InsBillingProv" int8 NULL,
	"Fax" varchar(50) NULL,
	"EmailAddressNum" int8 NULL,
	"DefaultProv" int8 NULL,
	"SmsContractDate" timestamp NULL,
	"SmsMonthlyLimit" float8 NULL,
	"IsMedicalOnly" bool NULL,
	"BillingAddress" varchar(255) NULL,
	"BillingAddress2" varchar(255) NULL,
	"BillingCity" varchar(255) NULL,
	"BillingState" varchar(255) NULL,
	"BillingZip" varchar(255) NULL,
	"PayToAddress" varchar(255) NULL,
	"PayToAddress2" varchar(255) NULL,
	"PayToCity" varchar(255) NULL,
	"PayToState" varchar(255) NULL,
	"PayToZip" varchar(255) NULL,
	"UseBillAddrOnClaims" bool NULL,
	"Region" int8 NULL,
	"ItemOrder" int4 NULL,
	"IsInsVerifyExcluded" bool NULL,
	"Abbr" varchar(255) NULL,
	"MedLabAccountNum" varchar(16) NULL,
	"IsConfirmEnabled" bool NULL,
	"IsConfirmDefault" bool NULL,
	"IsNewPatApptExcluded" bool NULL,
	"IsHidden" bool NULL,
	"ExternalID" int8 NULL,
	"SchedNote" varchar(255) NULL,
	"HasProcOnRx" bool NULL,
	"TimeZone" varchar(75) NULL,
	"EmailAliasOverride" varchar(255) NULL,
	CONSTRAINT clinic_pkey PRIMARY KEY ("ClinicNum")
);


-- raw.clinicerx definition

-- Drop table

-- DROP TABLE raw.clinicerx;

CREATE TABLE raw.clinicerx (
	"ClinicErxNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	"ClinicDesc" varchar(255) NULL,
	"ClinicNum" int8 NULL,
	"EnabledStatus" bool NULL,
	"ClinicId" varchar(255) NULL,
	"ClinicKey" varchar(255) NULL,
	"AccountId" varchar(25) NULL,
	"RegistrationKeyNum" int8 NULL,
	CONSTRAINT clinicerx_pkey PRIMARY KEY ("ClinicErxNum")
);


-- raw.clinicpref definition

-- Drop table

-- DROP TABLE raw.clinicpref;

CREATE TABLE raw.clinicpref (
	"ClinicPrefNum" int8 NOT NULL,
	"ClinicNum" int8 NULL,
	"PrefName" varchar(255) NULL,
	"ValueString" text NULL,
	CONSTRAINT clinicpref_pkey PRIMARY KEY ("ClinicPrefNum")
);


-- raw.clockevent definition

-- Drop table

-- DROP TABLE raw.clockevent;

CREATE TABLE raw.clockevent (
	"ClockEventNum" int8 NOT NULL,
	"EmployeeNum" int8 NULL,
	"TimeEntered1" timestamp NULL,
	"TimeDisplayed1" timestamp NULL,
	"ClockStatus" int2 NULL,
	"Note" text NULL,
	"TimeEntered2" timestamp NULL,
	"TimeDisplayed2" timestamp NULL,
	"OTimeHours" time NULL,
	"OTimeAuto" time NULL,
	"Adjust" time NULL,
	"AdjustAuto" time NULL,
	"AdjustIsOverridden" bool NULL,
	"Rate2Hours" time NULL,
	"Rate2Auto" time NULL,
	"ClinicNum" int8 NULL,
	"Rate3Hours" time NULL,
	"Rate3Auto" time NULL,
	"IsWorkingHome" bool NULL,
	CONSTRAINT clockevent_pkey PRIMARY KEY ("ClockEventNum")
);


-- raw.cloudaddress definition

-- Drop table

-- DROP TABLE raw.cloudaddress;

CREATE TABLE raw.cloudaddress (
	"CloudAddressNum" int8 NOT NULL,
	"IpAddress" varchar(50) NULL,
	"UserNumLastConnect" int8 NULL,
	"DateTimeLastConnect" timestamp NULL,
	CONSTRAINT cloudaddress_pkey PRIMARY KEY ("CloudAddressNum")
);


-- raw.codegroup definition

-- Drop table

-- DROP TABLE raw.codegroup;

CREATE TABLE raw.codegroup (
	"CodeGroupNum" int8 NOT NULL,
	"GroupName" varchar(50) NULL,
	"ProcCodes" text NULL,
	"ItemOrder" int4 NULL,
	"CodeGroupFixed" int2 NULL,
	"IsHidden" bool NULL,
	"ShowInAgeLimit" bool NULL,
	CONSTRAINT codegroup_pkey PRIMARY KEY ("CodeGroupNum")
);


-- raw.codesystem definition

-- Drop table

-- DROP TABLE raw.codesystem;

CREATE TABLE raw.codesystem (
	"CodeSystemNum" int8 NOT NULL,
	"CodeSystemName" varchar(255) NULL,
	"VersionCur" varchar(255) NULL,
	"VersionAvail" varchar(255) NULL,
	"HL7OID" varchar(255) NULL,
	"Note" varchar(255) NULL,
	CONSTRAINT codesystem_pkey PRIMARY KEY ("CodeSystemNum")
);


-- raw.commlog definition

-- Drop table

-- DROP TABLE raw.commlog;

CREATE TABLE raw.commlog (
	"CommlogNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	"CommDateTime" timestamp NULL,
	"CommType" int8 NULL,
	"Note" text NULL,
	"Mode_" int2 NULL,
	"SentOrReceived" int2 NULL,
	"UserNum" int8 NULL,
	"Signature" text NULL,
	"SigIsTopaz" bool NULL,
	"DateTStamp" timestamp NULL,
	"DateTimeEnd" timestamp NULL,
	"CommSource" int2 NULL,
	"ProgramNum" int8 NULL,
	"DateTEntry" timestamp NULL,
	"ReferralNum" int8 NULL,
	"CommReferralBehavior" bool NULL,
	CONSTRAINT commlog_pkey PRIMARY KEY ("CommlogNum")
);


-- raw.commoptout definition

-- Drop table

-- DROP TABLE raw.commoptout;

CREATE TABLE raw.commoptout (
	"CommOptOutNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	"OptOutSms" int4 NULL,
	"OptOutEmail" int4 NULL,
	CONSTRAINT commoptout_pkey PRIMARY KEY ("CommOptOutNum")
);


-- raw.computer definition

-- Drop table

-- DROP TABLE raw.computer;

CREATE TABLE raw.computer (
	"ComputerNum" int8 NOT NULL,
	"CompName" varchar(100) NULL,
	"LastHeartBeat" timestamp NULL,
	CONSTRAINT computer_pkey PRIMARY KEY ("ComputerNum")
);


-- raw.computerpref definition

-- Drop table

-- DROP TABLE raw.computerpref;

CREATE TABLE raw.computerpref (
	"ComputerPrefNum" int8 NOT NULL,
	"ComputerName" varchar(64) NULL,
	"GraphicsUseHardware" bool NULL,
	"GraphicsSimple" int2 NULL,
	"SensorType" varchar(255) NULL,
	"SensorBinned" bool NULL,
	"SensorPort" int4 NULL,
	"SensorExposure" int4 NULL,
	"GraphicsDoubleBuffering" bool NULL,
	"PreferredPixelFormatNum" int4 NULL,
	"AtoZpath" varchar(255) NULL,
	"TaskKeepListHidden" bool NULL,
	"TaskDock" int4 NULL,
	"TaskX" int4 NULL,
	"TaskY" int4 NULL,
	"DirectXFormat" varchar(255) NULL,
	"ScanDocSelectSource" bool NULL,
	"ScanDocShowOptions" bool NULL,
	"ScanDocDuplex" bool NULL,
	"ScanDocGrayscale" bool NULL,
	"ScanDocResolution" int4 NULL,
	"ScanDocQuality" int2 NULL,
	"ClinicNum" int8 NULL,
	"ApptViewNum" int8 NULL,
	"RecentApptView" bool NULL,
	"PatSelectSearchMode" int2 NULL,
	"NoShowLanguage" bool NULL,
	"NoShowDecimal" bool NULL,
	"ComputerOS" varchar(255) NULL,
	"HelpButtonXAdjustment" float8 NULL,
	"GraphicsUseDirectX11" bool NULL,
	"Zoom" int4 NULL,
	"VideoRectangle" varchar(255) NULL,
	"CreditCardTerminalId" varchar(255) NULL,
	CONSTRAINT computerpref_pkey PRIMARY KEY ("ComputerPrefNum")
);


-- raw.confirmationrequest definition

-- Drop table

-- DROP TABLE raw.confirmationrequest;

CREATE TABLE raw.confirmationrequest (
	"ConfirmationRequestNum" int8 NOT NULL,
	"ClinicNum" int8 NULL,
	"PatNum" int8 NULL,
	"ApptNum" int8 NULL,
	"DateTimeConfirmExpire" timestamp NULL,
	"ShortGUID" varchar(255) NULL,
	"ConfirmCode" varchar(255) NULL,
	"DateTimeEntry" timestamp NULL,
	"DateTimeConfirmTransmit" timestamp NULL,
	"DateTimeRSVP" timestamp NULL,
	"RSVPStatus" int2 NULL,
	"ResponseDescript" text NULL,
	"GuidMessageFromMobile" text NULL,
	"ApptDateTime" timestamp NULL,
	"TSPrior" int8 NULL,
	"DoNotResend" bool NULL,
	"SendStatus" int2 NULL,
	"ApptReminderRuleNum" int8 NULL,
	"MessageType" int2 NULL,
	"MessageFk" int8 NULL,
	"DateTimeSent" timestamp NULL,
	CONSTRAINT confirmationrequest_pkey PRIMARY KEY ("ConfirmationRequestNum")
);


-- raw.connectiongroup definition

-- Drop table

-- DROP TABLE raw.connectiongroup;

CREATE TABLE raw.connectiongroup (
	"ConnectionGroupNum" int8 NOT NULL,
	"Description" varchar(255) NULL,
	CONSTRAINT connectiongroup_pkey PRIMARY KEY ("ConnectionGroupNum")
);


-- raw.conngroupattach definition

-- Drop table

-- DROP TABLE raw.conngroupattach;

CREATE TABLE raw.conngroupattach (
	"ConnGroupAttachNum" int8 NOT NULL,
	"ConnectionGroupNum" int8 NULL,
	"CentralConnectionNum" int8 NULL,
	CONSTRAINT conngroupattach_pkey PRIMARY KEY ("ConnGroupAttachNum")
);


-- raw.contact definition

-- Drop table

-- DROP TABLE raw.contact;

CREATE TABLE raw.contact (
	"ContactNum" int8 NOT NULL,
	"LName" varchar(255) NULL,
	"FName" varchar(255) NULL,
	"WkPhone" varchar(255) NULL,
	"Fax" varchar(255) NULL,
	"Category" int8 NULL,
	"Notes" text NULL,
	CONSTRAINT contact_pkey PRIMARY KEY ("ContactNum")
);


-- raw.county definition

-- Drop table

-- DROP TABLE raw.county;

CREATE TABLE raw.county (
	"CountyNum" int8 NOT NULL,
	"CountyName" varchar(255) NULL,
	"CountyCode" varchar(255) NULL,
	CONSTRAINT county_pkey PRIMARY KEY ("CountyNum")
);


-- raw.covcat definition

-- Drop table

-- DROP TABLE raw.covcat;

CREATE TABLE raw.covcat (
	"CovCatNum" int8 NOT NULL,
	"Description" varchar(50) NULL,
	"DefaultPercent" int2 NULL,
	"CovOrder" int4 NULL,
	"IsHidden" bool NULL,
	"EbenefitCat" int2 NULL,
	CONSTRAINT covcat_pkey PRIMARY KEY ("CovCatNum")
);


-- raw.covspan definition

-- Drop table

-- DROP TABLE raw.covspan;

CREATE TABLE raw.covspan (
	"CovSpanNum" int8 NOT NULL,
	"CovCatNum" int8 NULL,
	"FromCode" varchar(15) NULL,
	"ToCode" varchar(15) NULL,
	CONSTRAINT covspan_pkey PRIMARY KEY ("CovSpanNum")
);


-- raw.cpt definition

-- Drop table

-- DROP TABLE raw.cpt;

CREATE TABLE raw.cpt (
	"CptNum" int8 NOT NULL,
	"CptCode" varchar(255) NULL,
	"Description" varchar(4000) NULL,
	"VersionIDs" varchar(255) NULL,
	CONSTRAINT cpt_pkey PRIMARY KEY ("CptNum")
);


-- raw.creditcard definition

-- Drop table

-- DROP TABLE raw.creditcard;

CREATE TABLE raw.creditcard (
	"CreditCardNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	"Address" varchar(255) NULL,
	"Zip" varchar(255) NULL,
	"XChargeToken" varchar(255) NULL,
	"CCNumberMasked" varchar(255) NULL,
	"CCExpiration" date NULL,
	"ItemOrder" int4 NULL,
	"ChargeAmt" float8 NULL,
	"DateStart" date NULL,
	"DateStop" date NULL,
	"Note" varchar(255) NULL,
	"PayPlanNum" int8 NULL,
	"PayConnectToken" varchar(255) NULL,
	"PayConnectTokenExp" date NULL,
	"Procedures" text NULL,
	"CCSource" bool NULL,
	"ClinicNum" int8 NULL,
	"ExcludeProcSync" bool NULL,
	"PaySimpleToken" varchar(255) NULL,
	"ChargeFrequency" varchar(150) NULL,
	"CanChargeWhenNoBal" bool NULL,
	"PaymentType" int8 NULL,
	"IsRecurringActive" bool NULL,
	"Nickname" varchar(255) NULL,
	CONSTRAINT creditcard_pkey PRIMARY KEY ("CreditCardNum")
);


-- raw.custrefentry definition

-- Drop table

-- DROP TABLE raw.custrefentry;

CREATE TABLE raw.custrefentry (
	"CustRefEntryNum" int8 NOT NULL,
	"PatNumCust" int8 NULL,
	"PatNumRef" int8 NULL,
	"DateEntry" date NULL,
	"Note" varchar(255) NULL,
	CONSTRAINT custrefentry_pkey PRIMARY KEY ("CustRefEntryNum")
);


-- raw.custreference definition

-- Drop table

-- DROP TABLE raw.custreference;

CREATE TABLE raw.custreference (
	"CustReferenceNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	"DateMostRecent" date NULL,
	"Note" varchar(255) NULL,
	"IsBadRef" bool NULL,
	CONSTRAINT custreference_pkey PRIMARY KEY ("CustReferenceNum")
);


-- raw.cvx definition

-- Drop table

-- DROP TABLE raw.cvx;

CREATE TABLE raw.cvx (
	"CvxNum" int8 NOT NULL,
	"CvxCode" varchar(255) NULL,
	"Description" varchar(255) NULL,
	"IsActive" varchar(255) NULL,
	CONSTRAINT cvx_pkey PRIMARY KEY ("CvxNum")
);


-- raw.dashboardar definition

-- Drop table

-- DROP TABLE raw.dashboardar;

CREATE TABLE raw.dashboardar (
	"DashboardARNum" int8 NOT NULL,
	"DateCalc" date NULL,
	"BalTotal" float8 NULL,
	"InsEst" float8 NULL,
	CONSTRAINT dashboardar_pkey PRIMARY KEY ("DashboardARNum")
);


-- raw.dashboardcell definition

-- Drop table

-- DROP TABLE raw.dashboardcell;

CREATE TABLE raw.dashboardcell (
	"DashboardCellNum" int8 NOT NULL,
	"DashboardLayoutNum" int8 NULL,
	"CellRow" int4 NULL,
	"CellColumn" int4 NULL,
	"CellType" varchar(255) NULL,
	"CellSettings" text NULL,
	"LastQueryTime" timestamp NULL,
	"LastQueryData" text NULL,
	"RefreshRateSeconds" int4 NULL,
	CONSTRAINT dashboardcell_pkey PRIMARY KEY ("DashboardCellNum")
);


-- raw.dashboardlayout definition

-- Drop table

-- DROP TABLE raw.dashboardlayout;

CREATE TABLE raw.dashboardlayout (
	"DashboardLayoutNum" int8 NOT NULL,
	"UserNum" int8 NULL,
	"UserGroupNum" int8 NULL,
	"DashboardTabName" varchar(255) NULL,
	"DashboardTabOrder" int4 NULL,
	"DashboardRows" int4 NULL,
	"DashboardColumns" int4 NULL,
	"DashboardGroupName" varchar(255) NULL,
	CONSTRAINT dashboardlayout_pkey PRIMARY KEY ("DashboardLayoutNum")
);


-- raw.databasemaintenance definition

-- Drop table

-- DROP TABLE raw.databasemaintenance;

CREATE TABLE raw.databasemaintenance (
	"DatabaseMaintenanceNum" int8 NOT NULL,
	"MethodName" varchar(255) NULL,
	"IsHidden" bool NULL,
	"IsOld" bool NULL,
	"DateLastRun" timestamp NULL,
	CONSTRAINT databasemaintenance_pkey PRIMARY KEY ("DatabaseMaintenanceNum")
);


-- raw.dbmlog definition

-- Drop table

-- DROP TABLE raw.dbmlog;

CREATE TABLE raw.dbmlog (
	"DbmLogNum" int8 NOT NULL,
	"UserNum" int8 NULL,
	"FKey" int8 NULL,
	"FKeyType" int2 NULL,
	"ActionType" int2 NULL,
	"DateTimeEntry" timestamp NULL,
	"MethodName" varchar(255) NULL,
	"LogText" text NULL,
	CONSTRAINT dbmlog_pkey PRIMARY KEY ("DbmLogNum")
);


-- raw.definition definition

-- Drop table

-- DROP TABLE raw.definition;

CREATE TABLE raw.definition (
	"DefNum" int8 NOT NULL,
	"Category" int2 NULL,
	"ItemOrder" int2 NULL,
	"ItemName" varchar(255) NULL,
	"ItemValue" varchar(255) NULL,
	"ItemColor" int4 NULL,
	"IsHidden" bool NULL,
	CONSTRAINT definition_pkey PRIMARY KEY ("DefNum")
);


-- raw.deflink definition

-- Drop table

-- DROP TABLE raw.deflink;

CREATE TABLE raw.deflink (
	"DefLinkNum" int8 NOT NULL,
	"DefNum" int8 NULL,
	"FKey" int8 NULL,
	"LinkType" int2 NULL,
	CONSTRAINT deflink_pkey PRIMARY KEY ("DefLinkNum")
);


-- raw.deletedobject definition

-- Drop table

-- DROP TABLE raw.deletedobject;

CREATE TABLE raw.deletedobject (
	"DeletedObjectNum" int8 NOT NULL,
	"ObjectNum" int8 NULL,
	"ObjectType" int4 NULL,
	"DateTStamp" timestamp NULL,
	CONSTRAINT deletedobject_pkey PRIMARY KEY ("DeletedObjectNum")
);


-- raw.deposit definition

-- Drop table

-- DROP TABLE raw.deposit;

CREATE TABLE raw.deposit (
	"DepositNum" int8 NOT NULL,
	"DateDeposit" date NULL,
	"BankAccountInfo" text NULL,
	"Amount" float8 NULL,
	"Memo" varchar(255) NULL,
	"Batch" varchar(25) NULL,
	"DepositAccountNum" int8 NULL,
	"IsSentToQuickBooksOnline" bool NULL,
	CONSTRAINT deposit_pkey PRIMARY KEY ("DepositNum")
);


-- raw.dictcustom definition

-- Drop table

-- DROP TABLE raw.dictcustom;

CREATE TABLE raw.dictcustom (
	"DictCustomNum" int8 NOT NULL,
	"WordText" varchar(255) NULL,
	CONSTRAINT dictcustom_pkey PRIMARY KEY ("DictCustomNum")
);


-- raw.discountplan definition

-- Drop table

-- DROP TABLE raw.discountplan;

CREATE TABLE raw.discountplan (
	"DiscountPlanNum" int8 NOT NULL,
	"Description" varchar(255) NULL,
	"FeeSchedNum" int8 NULL,
	"DefNum" int8 NULL,
	"IsHidden" bool NULL,
	"PlanNote" text NULL,
	"ExamFreqLimit" int4 NULL,
	"XrayFreqLimit" int4 NULL,
	"ProphyFreqLimit" int4 NULL,
	"FluorideFreqLimit" int4 NULL,
	"PerioFreqLimit" int4 NULL,
	"LimitedExamFreqLimit" int4 NULL,
	"PAFreqLimit" int4 NULL,
	"AnnualMax" float8 NULL,
	CONSTRAINT discountplan_pkey PRIMARY KEY ("DiscountPlanNum")
);


-- raw.discountplansub definition

-- Drop table

-- DROP TABLE raw.discountplansub;

CREATE TABLE raw.discountplansub (
	"DiscountSubNum" int8 NOT NULL,
	"DiscountPlanNum" int8 NULL,
	"PatNum" int8 NULL,
	"DateEffective" date NULL,
	"DateTerm" date NULL,
	"SubNote" text NULL,
	CONSTRAINT discountplansub_pkey PRIMARY KEY ("DiscountSubNum")
);


-- raw.disease definition

-- Drop table

-- DROP TABLE raw.disease;

CREATE TABLE raw.disease (
	"DiseaseNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	"DiseaseDefNum" int8 NULL,
	"PatNote" text NULL,
	"DateTStamp" timestamp NULL,
	"ProbStatus" int2 NULL,
	"DateStart" date NULL,
	"DateStop" date NULL,
	"SnomedProblemType" varchar(255) NULL,
	"FunctionStatus" bool NULL,
	CONSTRAINT disease_pkey PRIMARY KEY ("DiseaseNum")
);


-- raw.diseasedef definition

-- Drop table

-- DROP TABLE raw.diseasedef;

CREATE TABLE raw.diseasedef (
	"DiseaseDefNum" int8 NOT NULL,
	"DiseaseName" varchar(255) NULL,
	"ItemOrder" int2 NULL,
	"IsHidden" bool NULL,
	"DateTStamp" timestamp NULL,
	"ICD9Code" varchar(255) NULL,
	"SnomedCode" varchar(255) NULL,
	"Icd10Code" varchar(255) NULL,
	CONSTRAINT diseasedef_pkey PRIMARY KEY ("DiseaseDefNum")
);


-- raw.displayfield definition

-- Drop table

-- DROP TABLE raw.displayfield;

CREATE TABLE raw.displayfield (
	"DisplayFieldNum" int8 NOT NULL,
	"InternalName" varchar(255) NULL,
	"ItemOrder" int4 NULL,
	"Description" varchar(255) NULL,
	"ColumnWidth" int4 NULL,
	"Category" int4 NULL,
	"ChartViewNum" int8 NULL,
	"PickList" text NULL,
	"DescriptionOverride" varchar(255) NULL,
	CONSTRAINT displayfield_pkey PRIMARY KEY ("DisplayFieldNum")
);


-- raw.displayreport definition

-- Drop table

-- DROP TABLE raw.displayreport;

CREATE TABLE raw.displayreport (
	"DisplayReportNum" int8 NOT NULL,
	"InternalName" varchar(255) NULL,
	"ItemOrder" int4 NULL,
	"Description" varchar(255) NULL,
	"Category" int2 NULL,
	"IsHidden" bool NULL,
	"IsVisibleInSubMenu" bool NULL,
	CONSTRAINT displayreport_pkey PRIMARY KEY ("DisplayReportNum")
);


-- raw.dispsupply definition

-- Drop table

-- DROP TABLE raw.dispsupply;

CREATE TABLE raw.dispsupply (
	"DispSupplyNum" int8 NOT NULL,
	"SupplyNum" int8 NULL,
	"ProvNum" int8 NULL,
	"DateDispensed" date NULL,
	"DispQuantity" float4 NULL,
	"Note" text NULL,
	CONSTRAINT dispsupply_pkey PRIMARY KEY ("DispSupplyNum")
);


-- raw."document" definition

-- Drop table

-- DROP TABLE raw."document";

CREATE TABLE raw."document" (
	"DocNum" int8 NOT NULL,
	"Description" varchar(255) NULL,
	"DateCreated" timestamp NULL,
	"DocCategory" int8 NULL,
	"PatNum" int8 NULL,
	"FileName" varchar(255) NULL,
	"ImgType" int2 NULL,
	"IsFlipped" bool NULL,
	"DegreesRotated" float4 NULL,
	"ToothNumbers" varchar(255) NULL,
	"Note" text NULL,
	"SigIsTopaz" bool NULL,
	"Signature" text NULL,
	"CropX" int4 NULL,
	"CropY" int4 NULL,
	"CropW" int4 NULL,
	"CropH" int4 NULL,
	"WindowingMin" int4 NULL,
	"WindowingMax" int4 NULL,
	"MountItemNum" int8 NULL,
	"DateTStamp" timestamp NULL,
	"RawBase64" text NULL,
	"Thumbnail" text NULL,
	"ExternalGUID" varchar(255) NULL,
	"ExternalSource" varchar(255) NULL,
	"ProvNum" int8 NULL,
	"IsCropOld" bool NULL,
	"OcrResponseData" text NULL,
	"ImageCaptureType" bool NULL,
	"PrintHeading" bool NULL,
	"ChartLetterStatus" bool NULL,
	"UserNum" int8 NULL,
	"ChartLetterHash" varchar(255) NULL,
	CONSTRAINT document_pkey PRIMARY KEY ("DocNum")
);


-- raw.documentmisc definition

-- Drop table

-- DROP TABLE raw.documentmisc;

CREATE TABLE raw.documentmisc (
	"DocMiscNum" int8 NOT NULL,
	"DateCreated" date NULL,
	"FileName" varchar(255) NULL,
	"DocMiscType" bool NULL,
	"RawBase64" text NULL,
	CONSTRAINT documentmisc_pkey PRIMARY KEY ("DocMiscNum")
);


-- raw.drugmanufacturer definition

-- Drop table

-- DROP TABLE raw.drugmanufacturer;

CREATE TABLE raw.drugmanufacturer (
	"DrugManufacturerNum" int8 NOT NULL,
	"ManufacturerName" varchar(255) NULL,
	"ManufacturerCode" varchar(20) NULL,
	CONSTRAINT drugmanufacturer_pkey PRIMARY KEY ("DrugManufacturerNum")
);


-- raw.drugunit definition

-- Drop table

-- DROP TABLE raw.drugunit;

CREATE TABLE raw.drugunit (
	"DrugUnitNum" int8 NOT NULL,
	"UnitIdentifier" varchar(20) NULL,
	"UnitText" varchar(255) NULL,
	CONSTRAINT drugunit_pkey PRIMARY KEY ("DrugUnitNum")
);


-- raw.dunning definition

-- Drop table

-- DROP TABLE raw.dunning;

CREATE TABLE raw.dunning (
	"DunningNum" int8 NOT NULL,
	"DunMessage" text NULL,
	"BillingType" int8 NULL,
	"AgeAccount" bool NULL,
	"InsIsPending" bool NULL,
	"MessageBold" text NULL,
	"EmailSubject" varchar(255) NULL,
	"EmailBody" text NULL,
	"DaysInAdvance" int4 NULL,
	"ClinicNum" int8 NULL,
	"IsSuperFamily" bool NULL,
	CONSTRAINT dunning_pkey PRIMARY KEY ("DunningNum")
);


-- raw.ebill definition

-- Drop table

-- DROP TABLE raw.ebill;

CREATE TABLE raw.ebill (
	"EbillNum" int8 NOT NULL,
	"ClinicNum" int8 NULL,
	"ClientAcctNumber" varchar(255) NULL,
	"ElectUserName" varchar(255) NULL,
	"ElectPassword" varchar(255) NULL,
	"PracticeAddress" bool NULL,
	"RemitAddress" bool NULL,
	CONSTRAINT ebill_pkey PRIMARY KEY ("EbillNum")
);


-- raw.eclipboardimagecapture definition

-- Drop table

-- DROP TABLE raw.eclipboardimagecapture;

CREATE TABLE raw.eclipboardimagecapture (
	"EClipboardImageCaptureNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	"DefNum" int8 NULL,
	"IsSelfPortrait" bool NULL,
	"DateTimeUpserted" timestamp NULL,
	"DocNum" int8 NULL,
	"OcrCaptureType" bool NULL,
	CONSTRAINT eclipboardimagecapture_pkey PRIMARY KEY ("EClipboardImageCaptureNum")
);


-- raw.eclipboardimagecapturedef definition

-- Drop table

-- DROP TABLE raw.eclipboardimagecapturedef;

CREATE TABLE raw.eclipboardimagecapturedef (
	"EClipboardImageCaptureDefNum" int8 NOT NULL,
	"DefNum" int8 NULL,
	"IsSelfPortrait" bool NULL,
	"FrequencyDays" int4 NULL,
	"ClinicNum" int8 NULL,
	"OcrCaptureType" bool NULL,
	"Frequency" bool NULL,
	"ResubmitInterval" int8 NULL,
	CONSTRAINT eclipboardimagecapturedef_pkey PRIMARY KEY ("EClipboardImageCaptureDefNum")
);


-- raw.eclipboardsheetdef definition

-- Drop table

-- DROP TABLE raw.eclipboardsheetdef;

CREATE TABLE raw.eclipboardsheetdef (
	"EClipboardSheetDefNum" int8 NOT NULL,
	"SheetDefNum" int8 NULL,
	"ClinicNum" int8 NULL,
	"ResubmitInterval" int8 NULL,
	"ItemOrder" int4 NULL,
	"PrefillStatus" bool NULL,
	"MinAge" int4 NULL,
	"MaxAge" int4 NULL,
	"IgnoreSheetDefNums" text NULL,
	"PrefillStatusOverride" int8 NULL,
	"EFormDefNum" int8 NULL,
	"Frequency" int2 NULL,
	CONSTRAINT eclipboardsheetdef_pkey PRIMARY KEY ("EClipboardSheetDefNum")
);


-- raw.eduresource definition

-- Drop table

-- DROP TABLE raw.eduresource;

CREATE TABLE raw.eduresource (
	"EduResourceNum" int8 NOT NULL,
	"DiseaseDefNum" int8 NULL,
	"MedicationNum" int8 NULL,
	"LabResultID" varchar(255) NULL,
	"LabResultName" varchar(255) NULL,
	"LabResultCompare" varchar(255) NULL,
	"ResourceUrl" varchar(255) NULL,
	"SmokingSnoMed" varchar(255) NULL,
	CONSTRAINT eduresource_pkey PRIMARY KEY ("EduResourceNum")
);


-- raw.eform definition

-- Drop table

-- DROP TABLE raw.eform;

CREATE TABLE raw.eform (
	"EFormNum" int8 NOT NULL,
	"FormType" bool NULL,
	"PatNum" int8 NULL,
	"DateTimeShown" timestamp NULL,
	"Description" varchar(255) NULL,
	"DateTEdited" timestamp NULL,
	"MaxWidth" int4 NULL,
	"EFormDefNum" int8 NULL,
	"Status" bool NULL,
	"RevID" int4 NULL,
	"ShowLabelsBold" bool NULL,
	"SpaceBelowEachField" int4 NULL,
	"SpaceToRightEachField" int4 NULL,
	"SaveImageCategory" int8 NULL,
	CONSTRAINT eform_pkey PRIMARY KEY ("EFormNum")
);


-- raw.eformdef definition

-- Drop table

-- DROP TABLE raw.eformdef;

CREATE TABLE raw.eformdef (
	"EFormDefNum" int8 NOT NULL,
	"FormType" bool NULL,
	"Description" varchar(255) NULL,
	"DateTCreated" timestamp NULL,
	"IsInternalHidden" bool NULL,
	"MaxWidth" int4 NULL,
	"RevID" int4 NULL,
	"ShowLabelsBold" bool NULL,
	"SpaceBelowEachField" int4 NULL,
	"SpaceToRightEachField" int4 NULL,
	"SaveImageCategory" int8 NULL,
	CONSTRAINT eformdef_pkey PRIMARY KEY ("EFormDefNum")
);


-- raw.eformfield definition

-- Drop table

-- DROP TABLE raw.eformfield;

CREATE TABLE raw.eformfield (
	"EFormFieldNum" int8 NOT NULL,
	"EFormNum" int8 NULL,
	"PatNum" int8 NULL,
	"FieldType" bool NULL,
	"DbLink" varchar(255) NULL,
	"ValueLabel" text NULL,
	"ValueString" text NULL,
	"ItemOrder" int4 NULL,
	"PickListVis" varchar(255) NULL,
	"PickListDb" varchar(255) NULL,
	"IsHorizStacking" bool NULL,
	"IsTextWrap" bool NULL,
	"Width" int4 NULL,
	"FontScale" int4 NULL,
	"IsRequired" bool NULL,
	"ConditionalParent" varchar(255) NULL,
	"ConditionalValue" varchar(255) NULL,
	"LabelAlign" bool NULL,
	"SpaceBelow" int4 NULL,
	"ReportableName" varchar(255) NULL,
	"IsLocked" bool NULL,
	"Border" bool NULL,
	"IsWidthPercentage" bool NULL,
	"MinWidth" int4 NULL,
	"WidthLabel" int4 NULL,
	"SpaceToRight" int4 NULL,
	CONSTRAINT eformfield_pkey PRIMARY KEY ("EFormFieldNum")
);


-- raw.eformfielddef definition

-- Drop table

-- DROP TABLE raw.eformfielddef;

CREATE TABLE raw.eformfielddef (
	"EFormFieldDefNum" int8 NOT NULL,
	"EFormDefNum" int8 NULL,
	"FieldType" bool NULL,
	"DbLink" varchar(255) NULL,
	"ValueLabel" text NULL,
	"ItemOrder" int4 NULL,
	"PickListVis" varchar(255) NULL,
	"PickListDb" varchar(255) NULL,
	"IsHorizStacking" bool NULL,
	"IsTextWrap" bool NULL,
	"Width" int4 NULL,
	"FontScale" int4 NULL,
	"IsRequired" bool NULL,
	"ConditionalParent" varchar(255) NULL,
	"ConditionalValue" varchar(255) NULL,
	"LabelAlign" bool NULL,
	"SpaceBelow" int4 NULL,
	"ReportableName" varchar(255) NULL,
	"IsLocked" bool NULL,
	"Border" bool NULL,
	"IsWidthPercentage" bool NULL,
	"MinWidth" int4 NULL,
	"WidthLabel" int4 NULL,
	"SpaceToRight" int4 NULL,
	CONSTRAINT eformfielddef_pkey PRIMARY KEY ("EFormFieldDefNum")
);


-- raw.eformimportrule definition

-- Drop table

-- DROP TABLE raw.eformimportrule;

CREATE TABLE raw.eformimportrule (
	"EFormImportRuleNum" int8 NOT NULL,
	"FieldName" varchar(255) NULL,
	"Situation" bool NULL,
	"Action" bool NULL,
	CONSTRAINT eformimportrule_pkey PRIMARY KEY ("EFormImportRuleNum")
);


-- raw.ehramendment definition

-- Drop table

-- DROP TABLE raw.ehramendment;

CREATE TABLE raw.ehramendment (
	"EhrAmendmentNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	"IsAccepted" bool NULL,
	"Description" text NULL,
	"Source" bool NULL,
	"SourceName" text NULL,
	"FileName" varchar(255) NULL,
	"RawBase64" text NULL,
	"DateTRequest" timestamp NULL,
	"DateTAcceptDeny" timestamp NULL,
	"DateTAppend" timestamp NULL,
	CONSTRAINT ehramendment_pkey PRIMARY KEY ("EhrAmendmentNum")
);


-- raw.ehraptobs definition

-- Drop table

-- DROP TABLE raw.ehraptobs;

CREATE TABLE raw.ehraptobs (
	"EhrAptObsNum" int8 NOT NULL,
	"AptNum" int8 NULL,
	"IdentifyingCode" bool NULL,
	"ValType" bool NULL,
	"ValReported" varchar(255) NULL,
	"UcumCode" varchar(255) NULL,
	"ValCodeSystem" varchar(255) NULL,
	CONSTRAINT ehraptobs_pkey PRIMARY KEY ("EhrAptObsNum")
);


-- raw.ehrcareplan definition

-- Drop table

-- DROP TABLE raw.ehrcareplan;

CREATE TABLE raw.ehrcareplan (
	"EhrCarePlanNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	"SnomedEducation" varchar(255) NULL,
	"Instructions" varchar(255) NULL,
	"DatePlanned" date NULL,
	CONSTRAINT ehrcareplan_pkey PRIMARY KEY ("EhrCarePlanNum")
);


-- raw.ehrlab definition

-- Drop table

-- DROP TABLE raw.ehrlab;

CREATE TABLE raw.ehrlab (
	"EhrLabNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	"OrderControlCode" varchar(255) NULL,
	"PlacerOrderNum" varchar(255) NULL,
	"PlacerOrderNamespace" varchar(255) NULL,
	"PlacerOrderUniversalID" varchar(255) NULL,
	"PlacerOrderUniversalIDType" varchar(255) NULL,
	"FillerOrderNum" varchar(255) NULL,
	"FillerOrderNamespace" varchar(255) NULL,
	"FillerOrderUniversalID" varchar(255) NULL,
	"FillerOrderUniversalIDType" varchar(255) NULL,
	"PlacerGroupNum" varchar(255) NULL,
	"PlacerGroupNamespace" varchar(255) NULL,
	"PlacerGroupUniversalID" varchar(255) NULL,
	"PlacerGroupUniversalIDType" varchar(255) NULL,
	"OrderingProviderID" varchar(255) NULL,
	"OrderingProviderLName" varchar(255) NULL,
	"OrderingProviderFName" varchar(255) NULL,
	"OrderingProviderMiddleNames" varchar(255) NULL,
	"OrderingProviderSuffix" varchar(255) NULL,
	"OrderingProviderPrefix" varchar(255) NULL,
	"OrderingProviderAssigningAuthorityNamespaceID" varchar(255) NULL,
	"OrderingProviderAssigningAuthorityUniversalID" varchar(255) NULL,
	"OrderingProviderAssigningAuthorityIDType" varchar(255) NULL,
	"OrderingProviderNameTypeCode" varchar(255) NULL,
	"OrderingProviderIdentifierTypeCode" varchar(255) NULL,
	"SetIdOBR" int8 NULL,
	"UsiID" varchar(255) NULL,
	"UsiText" varchar(255) NULL,
	"UsiCodeSystemName" varchar(255) NULL,
	"UsiIDAlt" varchar(255) NULL,
	"UsiTextAlt" varchar(255) NULL,
	"UsiCodeSystemNameAlt" varchar(255) NULL,
	"UsiTextOriginal" varchar(255) NULL,
	"ObservationDateTimeStart" varchar(255) NULL,
	"ObservationDateTimeEnd" varchar(255) NULL,
	"SpecimenActionCode" varchar(255) NULL,
	"ResultDateTime" varchar(255) NULL,
	"ResultStatus" varchar(255) NULL,
	"ParentObservationID" varchar(255) NULL,
	"ParentObservationText" varchar(255) NULL,
	"ParentObservationCodeSystemName" varchar(255) NULL,
	"ParentObservationIDAlt" varchar(255) NULL,
	"ParentObservationTextAlt" varchar(255) NULL,
	"ParentObservationCodeSystemNameAlt" varchar(255) NULL,
	"ParentObservationTextOriginal" varchar(255) NULL,
	"ParentObservationSubID" varchar(255) NULL,
	"ParentPlacerOrderNum" varchar(255) NULL,
	"ParentPlacerOrderNamespace" varchar(255) NULL,
	"ParentPlacerOrderUniversalID" varchar(255) NULL,
	"ParentPlacerOrderUniversalIDType" varchar(255) NULL,
	"ParentFillerOrderNum" varchar(255) NULL,
	"ParentFillerOrderNamespace" varchar(255) NULL,
	"ParentFillerOrderUniversalID" varchar(255) NULL,
	"ParentFillerOrderUniversalIDType" varchar(255) NULL,
	"ListEhrLabResultsHandlingF" bool NULL,
	"ListEhrLabResultsHandlingN" bool NULL,
	"TQ1SetId" int8 NULL,
	"TQ1DateTimeStart" varchar(255) NULL,
	"TQ1DateTimeEnd" varchar(255) NULL,
	"IsCpoe" bool NULL,
	"OriginalPIDSegment" text NULL,
	CONSTRAINT ehrlab_pkey PRIMARY KEY ("EhrLabNum")
);


-- raw.ehrlabclinicalinfo definition

-- Drop table

-- DROP TABLE raw.ehrlabclinicalinfo;

CREATE TABLE raw.ehrlabclinicalinfo (
	"EhrLabClinicalInfoNum" int8 NOT NULL,
	"EhrLabNum" int8 NULL,
	"ClinicalInfoID" varchar(255) NULL,
	"ClinicalInfoText" varchar(255) NULL,
	"ClinicalInfoCodeSystemName" varchar(255) NULL,
	"ClinicalInfoIDAlt" varchar(255) NULL,
	"ClinicalInfoTextAlt" varchar(255) NULL,
	"ClinicalInfoCodeSystemNameAlt" varchar(255) NULL,
	"ClinicalInfoTextOriginal" varchar(255) NULL,
	CONSTRAINT ehrlabclinicalinfo_pkey PRIMARY KEY ("EhrLabClinicalInfoNum")
);


-- raw.ehrlabimage definition

-- Drop table

-- DROP TABLE raw.ehrlabimage;

CREATE TABLE raw.ehrlabimage (
	"EhrLabImageNum" int8 NOT NULL,
	"EhrLabNum" int8 NULL,
	"DocNum" int8 NULL,
	CONSTRAINT ehrlabimage_pkey PRIMARY KEY ("EhrLabImageNum")
);


-- raw.ehrlabnote definition

-- Drop table

-- DROP TABLE raw.ehrlabnote;

CREATE TABLE raw.ehrlabnote (
	"EhrLabNoteNum" int8 NOT NULL,
	"EhrLabNum" int8 NULL,
	"EhrLabResultNum" int8 NULL,
	"Comments" text NULL,
	CONSTRAINT ehrlabnote_pkey PRIMARY KEY ("EhrLabNoteNum")
);


-- raw.ehrlabresult definition

-- Drop table

-- DROP TABLE raw.ehrlabresult;

CREATE TABLE raw.ehrlabresult (
	"EhrLabResultNum" int8 NOT NULL,
	"EhrLabNum" int8 NULL,
	"SetIdOBX" int8 NULL,
	"ValueType" varchar(255) NULL,
	"ObservationIdentifierID" varchar(255) NULL,
	"ObservationIdentifierText" varchar(255) NULL,
	"ObservationIdentifierCodeSystemName" varchar(255) NULL,
	"ObservationIdentifierIDAlt" varchar(255) NULL,
	"ObservationIdentifierTextAlt" varchar(255) NULL,
	"ObservationIdentifierCodeSystemNameAlt" varchar(255) NULL,
	"ObservationIdentifierTextOriginal" varchar(255) NULL,
	"ObservationIdentifierSub" varchar(255) NULL,
	"ObservationValueCodedElementID" varchar(255) NULL,
	"ObservationValueCodedElementText" varchar(255) NULL,
	"ObservationValueCodedElementCodeSystemName" varchar(255) NULL,
	"ObservationValueCodedElementIDAlt" varchar(255) NULL,
	"ObservationValueCodedElementTextAlt" varchar(255) NULL,
	"ObservationValueCodedElementCodeSystemNameAlt" varchar(255) NULL,
	"ObservationValueCodedElementTextOriginal" varchar(255) NULL,
	"ObservationValueDateTime" varchar(255) NULL,
	"ObservationValueTime" time NULL,
	"ObservationValueComparator" varchar(255) NULL,
	"ObservationValueNumber1" float8 NULL,
	"ObservationValueSeparatorOrSuffix" varchar(255) NULL,
	"ObservationValueNumber2" float8 NULL,
	"ObservationValueNumeric" float8 NULL,
	"ObservationValueText" varchar(255) NULL,
	"UnitsID" varchar(255) NULL,
	"UnitsText" varchar(255) NULL,
	"UnitsCodeSystemName" varchar(255) NULL,
	"UnitsIDAlt" varchar(255) NULL,
	"UnitsTextAlt" varchar(255) NULL,
	"UnitsCodeSystemNameAlt" varchar(255) NULL,
	"UnitsTextOriginal" varchar(255) NULL,
	"referenceRange" varchar(255) NULL,
	"AbnormalFlags" varchar(255) NULL,
	"ObservationResultStatus" varchar(255) NULL,
	"ObservationDateTime" varchar(255) NULL,
	"AnalysisDateTime" varchar(255) NULL,
	"PerformingOrganizationName" varchar(255) NULL,
	"PerformingOrganizationNameAssigningAuthorityNamespaceId" varchar(255) NULL,
	"PerformingOrganizationNameAssigningAuthorityUniversalId" varchar(255) NULL,
	"PerformingOrganizationNameAssigningAuthorityUniversalIdType" varchar(255) NULL,
	"PerformingOrganizationIdentifierTypeCode" varchar(255) NULL,
	"PerformingOrganizationIdentifier" varchar(255) NULL,
	"PerformingOrganizationAddressStreet" varchar(255) NULL,
	"PerformingOrganizationAddressOtherDesignation" varchar(255) NULL,
	"PerformingOrganizationAddressCity" varchar(255) NULL,
	"PerformingOrganizationAddressStateOrProvince" varchar(255) NULL,
	"PerformingOrganizationAddressZipOrPostalCode" varchar(255) NULL,
	"PerformingOrganizationAddressCountryCode" varchar(255) NULL,
	"PerformingOrganizationAddressAddressType" varchar(255) NULL,
	"PerformingOrganizationAddressCountyOrParishCode" varchar(255) NULL,
	"MedicalDirectorID" varchar(255) NULL,
	"MedicalDirectorLName" varchar(255) NULL,
	"MedicalDirectorFName" varchar(255) NULL,
	"MedicalDirectorMiddleNames" varchar(255) NULL,
	"MedicalDirectorSuffix" varchar(255) NULL,
	"MedicalDirectorPrefix" varchar(255) NULL,
	"MedicalDirectorAssigningAuthorityNamespaceID" varchar(255) NULL,
	"MedicalDirectorAssigningAuthorityUniversalID" varchar(255) NULL,
	"MedicalDirectorAssigningAuthorityIDType" varchar(255) NULL,
	"MedicalDirectorNameTypeCode" varchar(255) NULL,
	"MedicalDirectorIdentifierTypeCode" varchar(255) NULL,
	CONSTRAINT ehrlabresult_pkey PRIMARY KEY ("EhrLabResultNum")
);


-- raw.ehrlabresultscopyto definition

-- Drop table

-- DROP TABLE raw.ehrlabresultscopyto;

CREATE TABLE raw.ehrlabresultscopyto (
	"EhrLabResultsCopyToNum" int8 NOT NULL,
	"EhrLabNum" int8 NULL,
	"CopyToID" varchar(255) NULL,
	"CopyToLName" varchar(255) NULL,
	"CopyToFName" varchar(255) NULL,
	"CopyToMiddleNames" varchar(255) NULL,
	"CopyToSuffix" varchar(255) NULL,
	"CopyToPrefix" varchar(255) NULL,
	"CopyToAssigningAuthorityNamespaceID" varchar(255) NULL,
	"CopyToAssigningAuthorityUniversalID" varchar(255) NULL,
	"CopyToAssigningAuthorityIDType" varchar(255) NULL,
	"CopyToNameTypeCode" varchar(255) NULL,
	"CopyToIdentifierTypeCode" varchar(255) NULL,
	CONSTRAINT ehrlabresultscopyto_pkey PRIMARY KEY ("EhrLabResultsCopyToNum")
);


-- raw.ehrlabspecimen definition

-- Drop table

-- DROP TABLE raw.ehrlabspecimen;

CREATE TABLE raw.ehrlabspecimen (
	"EhrLabSpecimenNum" int8 NOT NULL,
	"EhrLabNum" int8 NULL,
	"SetIdSPM" int8 NULL,
	"SpecimenTypeID" varchar(255) NULL,
	"SpecimenTypeText" varchar(255) NULL,
	"SpecimenTypeCodeSystemName" varchar(255) NULL,
	"SpecimenTypeIDAlt" varchar(255) NULL,
	"SpecimenTypeTextAlt" varchar(255) NULL,
	"SpecimenTypeCodeSystemNameAlt" varchar(255) NULL,
	"SpecimenTypeTextOriginal" varchar(255) NULL,
	"CollectionDateTimeStart" varchar(255) NULL,
	"CollectionDateTimeEnd" varchar(255) NULL,
	CONSTRAINT ehrlabspecimen_pkey PRIMARY KEY ("EhrLabSpecimenNum")
);


-- raw.ehrlabspecimencondition definition

-- Drop table

-- DROP TABLE raw.ehrlabspecimencondition;

CREATE TABLE raw.ehrlabspecimencondition (
	"EhrLabSpecimenConditionNum" int8 NOT NULL,
	"EhrLabSpecimenNum" int8 NULL,
	"SpecimenConditionID" varchar(255) NULL,
	"SpecimenConditionText" varchar(255) NULL,
	"SpecimenConditionCodeSystemName" varchar(255) NULL,
	"SpecimenConditionIDAlt" varchar(255) NULL,
	"SpecimenConditionTextAlt" varchar(255) NULL,
	"SpecimenConditionCodeSystemNameAlt" varchar(255) NULL,
	"SpecimenConditionTextOriginal" varchar(255) NULL,
	CONSTRAINT ehrlabspecimencondition_pkey PRIMARY KEY ("EhrLabSpecimenConditionNum")
);


-- raw.ehrlabspecimenrejectreason definition

-- Drop table

-- DROP TABLE raw.ehrlabspecimenrejectreason;

CREATE TABLE raw.ehrlabspecimenrejectreason (
	"EhrLabSpecimenRejectReasonNum" int8 NOT NULL,
	"EhrLabSpecimenNum" int8 NULL,
	"SpecimenRejectReasonID" varchar(255) NULL,
	"SpecimenRejectReasonText" varchar(255) NULL,
	"SpecimenRejectReasonCodeSystemName" varchar(255) NULL,
	"SpecimenRejectReasonIDAlt" varchar(255) NULL,
	"SpecimenRejectReasonTextAlt" varchar(255) NULL,
	"SpecimenRejectReasonCodeSystemNameAlt" varchar(255) NULL,
	"SpecimenRejectReasonTextOriginal" varchar(255) NULL,
	CONSTRAINT ehrlabspecimenrejectreason_pkey PRIMARY KEY ("EhrLabSpecimenRejectReasonNum")
);


-- raw.ehrmeasure definition

-- Drop table

-- DROP TABLE raw.ehrmeasure;

CREATE TABLE raw.ehrmeasure (
	"EhrMeasureNum" int8 NOT NULL,
	"MeasureType" int2 NULL,
	"Numerator" int2 NULL,
	"Denominator" int2 NULL,
	CONSTRAINT ehrmeasure_pkey PRIMARY KEY ("EhrMeasureNum")
);


-- raw.ehrmeasureevent definition

-- Drop table

-- DROP TABLE raw.ehrmeasureevent;

CREATE TABLE raw.ehrmeasureevent (
	"EhrMeasureEventNum" int8 NOT NULL,
	"DateTEvent" timestamp NULL,
	"EventType" int2 NULL,
	"PatNum" int8 NULL,
	"MoreInfo" varchar(255) NULL,
	"CodeValueEvent" varchar(30) NULL,
	"CodeSystemEvent" varchar(30) NULL,
	"CodeValueResult" varchar(30) NULL,
	"CodeSystemResult" varchar(30) NULL,
	"FKey" int8 NULL,
	"TobaccoCessationDesire" bool NULL,
	"DateStartTobacco" date NULL,
	CONSTRAINT ehrmeasureevent_pkey PRIMARY KEY ("EhrMeasureEventNum")
);


-- raw.ehrnotperformed definition

-- Drop table

-- DROP TABLE raw.ehrnotperformed;

CREATE TABLE raw.ehrnotperformed (
	"EhrNotPerformedNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	"ProvNum" int8 NULL,
	"CodeValue" varchar(30) NULL,
	"CodeSystem" varchar(30) NULL,
	"CodeValueReason" varchar(30) NULL,
	"CodeSystemReason" varchar(30) NULL,
	"Note" text NULL,
	"DateEntry" date NULL,
	CONSTRAINT ehrnotperformed_pkey PRIMARY KEY ("EhrNotPerformedNum")
);


-- raw.ehrpatient definition

-- Drop table

-- DROP TABLE raw.ehrpatient;

CREATE TABLE raw.ehrpatient (
	"PatNum" int8 NOT NULL,
	"MotherMaidenFname" varchar(255) NULL,
	"MotherMaidenLname" varchar(255) NULL,
	"VacShareOk" bool NULL,
	"MedicaidState" varchar(50) NULL,
	"SexualOrientation" varchar(255) NULL,
	"GenderIdentity" varchar(255) NULL,
	"SexualOrientationNote" varchar(255) NULL,
	"GenderIdentityNote" varchar(255) NULL,
	"DischargeDate" timestamp NULL,
	CONSTRAINT ehrpatient_pkey PRIMARY KEY ("PatNum")
);


-- raw.ehrprovkey definition

-- Drop table

-- DROP TABLE raw.ehrprovkey;

CREATE TABLE raw.ehrprovkey (
	"EhrProvKeyNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	"LName" varchar(255) NULL,
	"FName" varchar(255) NULL,
	"ProvKey" varchar(255) NULL,
	"FullTimeEquiv" float4 NULL,
	"Notes" text NULL,
	"YearValue" int4 NULL,
	CONSTRAINT ehrprovkey_pkey PRIMARY KEY ("EhrProvKeyNum")
);


-- raw.ehrquarterlykey definition

-- Drop table

-- DROP TABLE raw.ehrquarterlykey;

CREATE TABLE raw.ehrquarterlykey (
	"EhrQuarterlyKeyNum" int8 NOT NULL,
	"YearValue" int4 NULL,
	"QuarterValue" int4 NULL,
	"PracticeName" varchar(255) NULL,
	"KeyValue" varchar(255) NULL,
	"PatNum" int8 NULL,
	"Notes" text NULL,
	CONSTRAINT ehrquarterlykey_pkey PRIMARY KEY ("EhrQuarterlyKeyNum")
);


-- raw.ehrsummaryccd definition

-- Drop table

-- DROP TABLE raw.ehrsummaryccd;

CREATE TABLE raw.ehrsummaryccd (
	"EhrSummaryCcdNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	"DateSummary" date NULL,
	"ContentSummary" text NULL,
	"EmailAttachNum" int8 NULL,
	CONSTRAINT ehrsummaryccd_pkey PRIMARY KEY ("EhrSummaryCcdNum")
);


-- raw.ehrtrigger definition

-- Drop table

-- DROP TABLE raw.ehrtrigger;

CREATE TABLE raw.ehrtrigger (
	"EhrTriggerNum" int8 NOT NULL,
	"Description" varchar(255) NULL,
	"ProblemSnomedList" text NULL,
	"ProblemIcd9List" text NULL,
	"ProblemIcd10List" text NULL,
	"ProblemDefNumList" text NULL,
	"MedicationNumList" text NULL,
	"RxCuiList" text NULL,
	"CvxList" text NULL,
	"AllergyDefNumList" text NULL,
	"DemographicsList" text NULL,
	"LabLoincList" text NULL,
	"VitalLoincList" text NULL,
	"Instructions" text NULL,
	"Bibliography" text NULL,
	"Cardinality" bool NULL,
	CONSTRAINT ehrtrigger_pkey PRIMARY KEY ("EhrTriggerNum")
);


-- raw.electid definition

-- Drop table

-- DROP TABLE raw.electid;

CREATE TABLE raw.electid (
	"ElectIDNum" int8 NOT NULL,
	"PayorID" varchar(255) NULL,
	"CarrierName" varchar(255) NULL,
	"IsMedicaid" bool NULL,
	"ProviderTypes" varchar(255) NULL,
	"Comments" text NULL,
	"CommBridge" bool NULL,
	"Attributes" varchar(255) NULL,
	CONSTRAINT electid_pkey PRIMARY KEY ("ElectIDNum")
);


-- raw.emailaddress definition

-- Drop table

-- DROP TABLE raw.emailaddress;

CREATE TABLE raw.emailaddress (
	"EmailAddressNum" int8 NOT NULL,
	"SMTPserver" varchar(255) NULL,
	"EmailUsername" varchar(255) NULL,
	"EmailPassword" varchar(255) NULL,
	"ServerPort" int4 NULL,
	"UseSSL" bool NULL,
	"SenderAddress" varchar(255) NULL,
	"Pop3ServerIncoming" varchar(255) NULL,
	"ServerPortIncoming" int4 NULL,
	"UserNum" int8 NULL,
	"AccessToken" varchar(2000) NULL,
	"RefreshToken" text NULL,
	"DownloadInbox" bool NULL,
	"QueryString" varchar(1000) NULL,
	"AuthenticationType" bool NULL,
	CONSTRAINT emailaddress_pkey PRIMARY KEY ("EmailAddressNum")
);


-- raw.emailattach definition

-- Drop table

-- DROP TABLE raw.emailattach;

CREATE TABLE raw.emailattach (
	"EmailAttachNum" int8 NOT NULL,
	"EmailMessageNum" int8 NULL,
	"DisplayedFileName" varchar(255) NULL,
	"ActualFileName" varchar(255) NULL,
	"EmailTemplateNum" int8 NULL,
	CONSTRAINT emailattach_pkey PRIMARY KEY ("EmailAttachNum")
);


-- raw.emailautograph definition

-- Drop table

-- DROP TABLE raw.emailautograph;

CREATE TABLE raw.emailautograph (
	"EmailAutographNum" int8 NOT NULL,
	"Description" text NULL,
	"EmailAddress" varchar(255) NULL,
	"AutographText" text NULL,
	CONSTRAINT emailautograph_pkey PRIMARY KEY ("EmailAutographNum")
);


-- raw.emailhostingtemplate definition

-- Drop table

-- DROP TABLE raw.emailhostingtemplate;

CREATE TABLE raw.emailhostingtemplate (
	"EmailHostingTemplateNum" int8 NOT NULL,
	"TemplateName" varchar(255) NULL,
	"Subject" text NULL,
	"BodyPlainText" text NULL,
	"BodyHTML" text NULL,
	"TemplateId" int8 NULL,
	"ClinicNum" int8 NULL,
	"EmailTemplateType" varchar(255) NULL,
	"TemplateType" varchar(255) NULL,
	CONSTRAINT emailhostingtemplate_pkey PRIMARY KEY ("EmailHostingTemplateNum")
);


-- raw.emailmessage definition

-- Drop table

-- DROP TABLE raw.emailmessage;

CREATE TABLE raw.emailmessage (
	"EmailMessageNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	"ToAddress" text NULL,
	"FromAddress" text NULL,
	"Subject" text NULL,
	"BodyText" text NULL,
	"MsgDateTime" timestamp NULL,
	"SentOrReceived" int2 NULL,
	"RecipientAddress" varchar(255) NULL,
	"RawEmailIn" text NULL,
	"ProvNumWebMail" int8 NULL,
	"PatNumSubj" int8 NULL,
	"CcAddress" text NULL,
	"BccAddress" text NULL,
	"HideIn" bool NULL,
	"AptNum" int8 NULL,
	"UserNum" int8 NULL,
	"HtmlType" bool NULL,
	"SecDateTEntry" timestamp NULL,
	"SecDateTEdit" timestamp NULL,
	"MsgType" varchar(255) NULL,
	"FailReason" varchar(255) NULL,
	CONSTRAINT emailmessage_pkey PRIMARY KEY ("EmailMessageNum")
);


-- raw.emailmessageuid definition

-- Drop table

-- DROP TABLE raw.emailmessageuid;

CREATE TABLE raw.emailmessageuid (
	"EmailMessageUidNum" int8 NOT NULL,
	"MsgId" text NULL,
	"RecipientAddress" varchar(255) NULL,
	CONSTRAINT emailmessageuid_pkey PRIMARY KEY ("EmailMessageUidNum")
);


-- raw.emailsecure definition

-- Drop table

-- DROP TABLE raw.emailsecure;

CREATE TABLE raw.emailsecure (
	"EmailSecureNum" int8 NOT NULL,
	"ClinicNum" int8 NULL,
	"PatNum" int8 NULL,
	"EmailMessageNum" int8 NULL,
	"EmailChainFK" int8 NULL,
	"EmailFK" int8 NULL,
	"DateTEntry" timestamp NULL,
	"SecDateTEdit" timestamp NULL,
	CONSTRAINT emailsecure_pkey PRIMARY KEY ("EmailSecureNum")
);


-- raw.emailsecureattach definition

-- Drop table

-- DROP TABLE raw.emailsecureattach;

CREATE TABLE raw.emailsecureattach (
	"EmailSecureAttachNum" int8 NOT NULL,
	"ClinicNum" int8 NULL,
	"EmailAttachNum" int8 NULL,
	"EmailSecureNum" int8 NULL,
	"AttachmentGuid" varchar(50) NULL,
	"DisplayedFileName" varchar(255) NULL,
	"Extension" varchar(255) NULL,
	"DateTEntry" timestamp NULL,
	"SecDateTEdit" timestamp NULL,
	CONSTRAINT emailsecureattach_pkey PRIMARY KEY ("EmailSecureAttachNum")
);


-- raw.emailtemplate definition

-- Drop table

-- DROP TABLE raw.emailtemplate;

CREATE TABLE raw.emailtemplate (
	"EmailTemplateNum" int8 NOT NULL,
	"Subject" text NULL,
	"BodyText" text NULL,
	"Description" text NULL,
	"TemplateType" bool NULL,
	CONSTRAINT emailtemplate_pkey PRIMARY KEY ("EmailTemplateNum")
);


-- raw.employee definition

-- Drop table

-- DROP TABLE raw.employee;

CREATE TABLE raw.employee (
	"EmployeeNum" int8 NOT NULL,
	"LName" varchar(255) NULL,
	"FName" varchar(255) NULL,
	"MiddleI" varchar(255) NULL,
	"IsHidden" bool NULL,
	"ClockStatus" varchar(255) NULL,
	"PhoneExt" int4 NULL,
	"PayrollID" varchar(255) NULL,
	"WirelessPhone" varchar(255) NULL,
	"EmailWork" varchar(255) NULL,
	"EmailPersonal" varchar(255) NULL,
	"IsFurloughed" bool NULL,
	"IsWorkingHome" bool NULL,
	"ReportsTo" int8 NULL,
	CONSTRAINT employee_pkey PRIMARY KEY ("EmployeeNum")
);


-- raw.employer definition

-- Drop table

-- DROP TABLE raw.employer;

CREATE TABLE raw.employer (
	"EmployerNum" int8 NOT NULL,
	"EmpName" varchar(255) NULL,
	"Address" varchar(255) NULL,
	"Address2" varchar(255) NULL,
	"City" varchar(255) NULL,
	"State" varchar(255) NULL,
	"Zip" varchar(255) NULL,
	"Phone" varchar(255) NULL,
	CONSTRAINT employer_pkey PRIMARY KEY ("EmployerNum")
);


-- raw.encounter definition

-- Drop table

-- DROP TABLE raw.encounter;

CREATE TABLE raw.encounter (
	"EncounterNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	"ProvNum" int8 NULL,
	"CodeValue" varchar(30) NULL,
	"CodeSystem" varchar(30) NULL,
	"Note" text NULL,
	"DateEncounter" date NULL,
	CONSTRAINT encounter_pkey PRIMARY KEY ("EncounterNum")
);


-- raw.entrylog definition

-- Drop table

-- DROP TABLE raw.entrylog;

CREATE TABLE raw.entrylog (
	"EntryLogNum" int8 NOT NULL,
	"UserNum" int8 NULL,
	"FKeyType" bool NULL,
	"FKey" int8 NULL,
	"LogSource" int2 NULL,
	"EntryDateTime" timestamp NULL,
	CONSTRAINT entrylog_pkey PRIMARY KEY ("EntryLogNum")
);


-- raw.eobattach definition

-- Drop table

-- DROP TABLE raw.eobattach;

CREATE TABLE raw.eobattach (
	"EobAttachNum" int8 NOT NULL,
	"ClaimPaymentNum" int8 NULL,
	"DateTCreated" timestamp NULL,
	"FileName" varchar(255) NULL,
	"RawBase64" text NULL,
	"ClaimNumPreAuth" int8 NULL,
	CONSTRAINT eobattach_pkey PRIMARY KEY ("EobAttachNum")
);


-- raw.equipment definition

-- Drop table

-- DROP TABLE raw.equipment;

CREATE TABLE raw.equipment (
	"EquipmentNum" int8 NOT NULL,
	"Description" text NULL,
	"SerialNumber" varchar(255) NULL,
	"ModelYear" varchar(2) NULL,
	"DatePurchased" date NULL,
	"DateSold" date NULL,
	"PurchaseCost" float8 NULL,
	"MarketValue" float8 NULL,
	"Location" text NULL,
	"DateEntry" date NULL,
	"ProvNumCheckedOut" int8 NULL,
	"DateCheckedOut" date NULL,
	"DateExpectedBack" date NULL,
	"DispenseNote" text NULL,
	"Status" text NULL,
	CONSTRAINT equipment_pkey PRIMARY KEY ("EquipmentNum")
);


-- raw.erouting definition

-- Drop table

-- DROP TABLE raw.erouting;

CREATE TABLE raw.erouting (
	"ERoutingNum" int8 NOT NULL,
	"Description" varchar(255) NULL,
	"PatNum" int8 NULL,
	"ClinicNum" int8 NULL,
	"SecDateTEntry" timestamp NULL,
	"IsComplete" bool NULL,
	CONSTRAINT erouting_pkey PRIMARY KEY ("ERoutingNum")
);


-- raw.eroutingaction definition

-- Drop table

-- DROP TABLE raw.eroutingaction;

CREATE TABLE raw.eroutingaction (
	"ERoutingActionNum" int8 NOT NULL,
	"ERoutingNum" int8 NULL,
	"ItemOrder" int4 NULL,
	"ERoutingActionType" bool NULL,
	"UserNum" int8 NULL,
	"IsComplete" bool NULL,
	"DateTimeComplete" timestamp NULL,
	"ForeignKeyType" bool NULL,
	"ForeignKey" int8 NULL,
	"LabelOverride" varchar(255) NULL,
	CONSTRAINT eroutingaction_pkey PRIMARY KEY ("ERoutingActionNum")
);


-- raw.erxlog definition

-- Drop table

-- DROP TABLE raw.erxlog;

CREATE TABLE raw.erxlog (
	"ErxLogNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	"MsgText" text NULL,
	"DateTStamp" timestamp NULL,
	"ProvNum" int8 NULL,
	"UserNum" int8 NULL,
	CONSTRAINT erxlog_pkey PRIMARY KEY ("ErxLogNum")
);


-- raw.eservicelog definition

-- Drop table

-- DROP TABLE raw.eservicelog;

CREATE TABLE raw.eservicelog (
	"EServiceLogNum" int8 NOT NULL,
	"LogDateTime" timestamp NULL,
	"PatNum" int8 NULL,
	"EServiceType" int2 NULL,
	"EServiceAction" int2 NULL,
	"KeyType" int2 NULL,
	"LogGuid" varchar(36) NULL,
	"ClinicNum" int8 NULL,
	"FKey" int8 NULL,
	"DateTimeUploaded" timestamp NULL,
	"Note" varchar(255) NULL,
	CONSTRAINT eservicelog_pkey PRIMARY KEY ("EServiceLogNum")
);


-- raw.eserviceshortguid definition

-- Drop table

-- DROP TABLE raw.eserviceshortguid;

CREATE TABLE raw.eserviceshortguid (
	"EServiceShortGuidNum" int8 NOT NULL,
	"EServiceCode" varchar(255) NULL,
	"ShortGuid" varchar(255) NULL,
	"ShortURL" varchar(255) NULL,
	"FKey" int8 NULL,
	"FKeyType" varchar(255) NULL,
	"DateTimeExpiration" timestamp NULL,
	"DateTEntry" timestamp NULL,
	CONSTRAINT eserviceshortguid_pkey PRIMARY KEY ("EServiceShortGuidNum")
);


-- raw.eservicesignal definition

-- Drop table

-- DROP TABLE raw.eservicesignal;

CREATE TABLE raw.eservicesignal (
	"EServiceSignalNum" int8 NOT NULL,
	"ServiceCode" int4 NULL,
	"ReasonCategory" int4 NULL,
	"ReasonCode" int4 NULL,
	"Severity" int2 NULL,
	"Description" text NULL,
	"SigDateTime" timestamp NULL,
	"Tag" text NULL,
	"IsProcessed" bool NULL,
	CONSTRAINT eservicesignal_pkey PRIMARY KEY ("EServiceSignalNum")
);


-- raw.etl_load_status definition

-- Drop table

-- DROP TABLE raw.etl_load_status;

CREATE TABLE raw.etl_load_status (
	id serial4 NOT NULL,
	table_name varchar(255) NOT NULL,
	last_loaded timestamp DEFAULT '1970-01-01 00:00:01'::timestamp without time zone NOT NULL,
	last_primary_value varchar(255) NULL,
	primary_column_name varchar(255) NULL,
	rows_loaded int4 DEFAULT 0 NULL,
	load_status varchar(50) DEFAULT 'pending'::character varying NULL,
	_loaded_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	CONSTRAINT etl_load_status_pkey PRIMARY KEY (id),
	CONSTRAINT etl_load_status_table_name_key UNIQUE (table_name)
);
CREATE INDEX idx_etl_load_status_load_status ON raw.etl_load_status USING btree (load_status);
CREATE INDEX idx_etl_load_status_primary_value ON raw.etl_load_status USING btree (last_primary_value);
CREATE INDEX idx_etl_load_status_table_name ON raw.etl_load_status USING btree (table_name);


-- raw.etl_pipeline_metrics definition

-- Drop table

-- DROP TABLE raw.etl_pipeline_metrics;

CREATE TABLE raw.etl_pipeline_metrics (
	id serial4 NOT NULL,
	pipeline_id varchar(50) NOT NULL,
	start_time timestamp NULL,
	end_time timestamp NULL,
	total_time float8 NULL,
	tables_processed int4 DEFAULT 0 NULL,
	total_rows_processed int4 DEFAULT 0 NULL,
	success bool DEFAULT true NULL,
	error_count int4 DEFAULT 0 NULL,
	status varchar(20) DEFAULT 'idle'::character varying NULL,
	metrics_json jsonb NULL,
	created_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	CONSTRAINT etl_pipeline_metrics_pkey PRIMARY KEY (id)
);
CREATE INDEX idx_pipeline_metrics_pipeline_id ON raw.etl_pipeline_metrics USING btree (pipeline_id);
CREATE INDEX idx_pipeline_metrics_start_time ON raw.etl_pipeline_metrics USING btree (start_time);


-- raw.etl_table_metrics definition

-- Drop table

-- DROP TABLE raw.etl_table_metrics;

CREATE TABLE raw.etl_table_metrics (
	id serial4 NOT NULL,
	pipeline_id varchar(50) NOT NULL,
	table_name varchar(100) NOT NULL,
	rows_processed int4 DEFAULT 0 NULL,
	processing_time float8 NULL,
	success bool DEFAULT true NULL,
	"error" text NULL,
	"timestamp" timestamp NULL,
	created_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	CONSTRAINT etl_table_metrics_pkey PRIMARY KEY (id)
);
CREATE INDEX idx_table_metrics_pipeline_id ON raw.etl_table_metrics USING btree (pipeline_id);
CREATE INDEX idx_table_metrics_table_name ON raw.etl_table_metrics USING btree (table_name);


-- raw.etl_transform_status definition

-- Drop table

-- DROP TABLE raw.etl_transform_status;

CREATE TABLE raw.etl_transform_status (
	id serial4 NOT NULL,
	table_name varchar(255) NOT NULL,
	last_transformed timestamp DEFAULT '1970-01-01 00:00:01'::timestamp without time zone NOT NULL,
	last_primary_value varchar(255) NULL,
	primary_column_name varchar(255) NULL,
	rows_transformed int4 DEFAULT 0 NULL,
	transform_status varchar(50) DEFAULT 'pending'::character varying NULL,
	_transformed_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	_created_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	_updated_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	CONSTRAINT etl_transform_status_pkey PRIMARY KEY (id),
	CONSTRAINT etl_transform_status_table_name_key UNIQUE (table_name)
);
CREATE INDEX idx_etl_transform_status_primary_value ON raw.etl_transform_status USING btree (last_primary_value);
CREATE INDEX idx_etl_transform_status_table_name ON raw.etl_transform_status USING btree (table_name);
CREATE INDEX idx_etl_transform_status_transform_status ON raw.etl_transform_status USING btree (transform_status);


-- raw.etrans definition

-- Drop table

-- DROP TABLE raw.etrans;

CREATE TABLE raw.etrans (
	"EtransNum" int8 NOT NULL,
	"DateTimeTrans" timestamp NULL,
	"ClearingHouseNum" int8 NULL,
	"Etype" int2 NULL,
	"ClaimNum" int8 NULL,
	"OfficeSequenceNumber" int4 NULL,
	"CarrierTransCounter" int4 NULL,
	"CarrierTransCounter2" int4 NULL,
	"CarrierNum" int8 NULL,
	"CarrierNum2" int8 NULL,
	"PatNum" int8 NULL,
	"BatchNumber" int4 NULL,
	"AckCode" varchar(255) NULL,
	"TransSetNum" int4 NULL,
	"Note" text NULL,
	"EtransMessageTextNum" int8 NULL,
	"AckEtransNum" int8 NULL,
	"PlanNum" int8 NULL,
	"InsSubNum" int8 NULL,
	"TranSetId835" varchar(255) NULL,
	"CarrierNameRaw" varchar(60) NULL,
	"PatientNameRaw" varchar(133) NULL,
	"UserNum" int8 NULL,
	CONSTRAINT etrans_pkey PRIMARY KEY ("EtransNum")
);


-- raw.etrans835 definition

-- Drop table

-- DROP TABLE raw.etrans835;

CREATE TABLE raw.etrans835 (
	"Etrans835Num" int8 NOT NULL,
	"EtransNum" int8 NULL,
	"PayerName" varchar(60) NULL,
	"TransRefNum" varchar(50) NULL,
	"InsPaid" float8 NULL,
	"ControlId" varchar(9) NULL,
	"PaymentMethodCode" varchar(3) NULL,
	"PatientName" varchar(100) NULL,
	"Status" bool NULL,
	"AutoProcessed" bool NULL,
	"IsApproved" bool NULL,
	CONSTRAINT etrans835_pkey PRIMARY KEY ("Etrans835Num")
);


-- raw.etrans835attach definition

-- Drop table

-- DROP TABLE raw.etrans835attach;

CREATE TABLE raw.etrans835attach (
	"Etrans835AttachNum" int8 NOT NULL,
	"EtransNum" int8 NULL,
	"ClaimNum" int8 NULL,
	"ClpSegmentIndex" int4 NULL,
	"DateTimeEntry" timestamp NULL,
	CONSTRAINT etrans835attach_pkey PRIMARY KEY ("Etrans835AttachNum")
);


-- raw.etransmessagetext definition

-- Drop table

-- DROP TABLE raw.etransmessagetext;

CREATE TABLE raw.etransmessagetext (
	"EtransMessageTextNum" int8 NOT NULL,
	"MessageText" text NULL,
	CONSTRAINT etransmessagetext_pkey PRIMARY KEY ("EtransMessageTextNum")
);


-- raw.evaluation definition

-- Drop table

-- DROP TABLE raw.evaluation;

CREATE TABLE raw.evaluation (
	"EvaluationNum" int8 NOT NULL,
	"InstructNum" int8 NULL,
	"StudentNum" int8 NULL,
	"SchoolCourseNum" int8 NULL,
	"EvalTitle" varchar(255) NULL,
	"DateEval" date NULL,
	"GradingScaleNum" int8 NULL,
	"OverallGradeShowing" varchar(255) NULL,
	"OverallGradeNumber" float4 NULL,
	"Notes" text NULL,
	CONSTRAINT evaluation_pkey PRIMARY KEY ("EvaluationNum")
);


-- raw.evaluationcriterion definition

-- Drop table

-- DROP TABLE raw.evaluationcriterion;

CREATE TABLE raw.evaluationcriterion (
	"EvaluationCriterionNum" int8 NOT NULL,
	"EvaluationNum" int8 NULL,
	"CriterionDescript" varchar(255) NULL,
	"IsCategoryName" bool NULL,
	"GradingScaleNum" int8 NULL,
	"GradeShowing" varchar(255) NULL,
	"GradeNumber" float4 NULL,
	"Notes" text NULL,
	"ItemOrder" int4 NULL,
	"MaxPointsPoss" float4 NULL,
	CONSTRAINT evaluationcriterion_pkey PRIMARY KEY ("EvaluationCriterionNum")
);


-- raw.famaging definition

-- Drop table

-- DROP TABLE raw.famaging;

CREATE TABLE raw.famaging (
	"PatNum" int8 NOT NULL,
	"Bal_0_30" float8 NULL,
	"Bal_31_60" float8 NULL,
	"Bal_61_90" float8 NULL,
	"BalOver90" float8 NULL,
	"InsEst" float8 NULL,
	"BalTotal" float8 NULL,
	"PayPlanDue" float8 NULL,
	CONSTRAINT famaging_pkey PRIMARY KEY ("PatNum")
);


-- raw.familyhealth definition

-- Drop table

-- DROP TABLE raw.familyhealth;

CREATE TABLE raw.familyhealth (
	"FamilyHealthNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	"Relationship" bool NULL,
	"DiseaseDefNum" int8 NULL,
	"PersonName" varchar(255) NULL,
	CONSTRAINT familyhealth_pkey PRIMARY KEY ("FamilyHealthNum")
);


-- raw.fee definition

-- Drop table

-- DROP TABLE raw.fee;

CREATE TABLE raw.fee (
	"FeeNum" int8 NOT NULL,
	"Amount" float8 NULL,
	"OldCode" varchar(15) NULL,
	"FeeSched" int8 NULL,
	"UseDefaultFee" bool NULL,
	"UseDefaultCov" bool NULL,
	"CodeNum" int8 NULL,
	"ClinicNum" int8 NULL,
	"ProvNum" int8 NULL,
	"SecUserNumEntry" int8 NULL,
	"SecDateEntry" date NULL,
	"SecDateTEdit" timestamp NULL,
	CONSTRAINT fee_pkey PRIMARY KEY ("FeeNum")
);


-- raw.feesched definition

-- Drop table

-- DROP TABLE raw.feesched;

CREATE TABLE raw.feesched (
	"FeeSchedNum" int8 NOT NULL,
	"Description" varchar(255) NULL,
	"FeeSchedType" int4 NULL,
	"ItemOrder" int4 NULL,
	"IsHidden" bool NULL,
	"IsGlobal" bool NULL,
	"SecUserNumEntry" int8 NULL,
	"SecDateEntry" date NULL,
	"SecDateTEdit" timestamp NULL,
	CONSTRAINT feesched_pkey PRIMARY KEY ("FeeSchedNum")
);


-- raw.feeschedgroup definition

-- Drop table

-- DROP TABLE raw.feeschedgroup;

CREATE TABLE raw.feeschedgroup (
	"FeeSchedGroupNum" int8 NOT NULL,
	"Description" varchar(255) NULL,
	"FeeSchedNum" int8 NULL,
	"ClinicNums" varchar(255) NULL,
	CONSTRAINT feeschedgroup_pkey PRIMARY KEY ("FeeSchedGroupNum")
);


-- raw.fhircontactpoint definition

-- Drop table

-- DROP TABLE raw.fhircontactpoint;

CREATE TABLE raw.fhircontactpoint (
	"FHIRContactPointNum" int8 NOT NULL,
	"FHIRSubscriptionNum" int8 NULL,
	"ContactSystem" bool NULL,
	"ContactValue" varchar(255) NULL,
	"ContactUse" bool NULL,
	"ItemOrder" int4 NULL,
	"DateStart" date NULL,
	"DateEnd" date NULL,
	CONSTRAINT fhircontactpoint_pkey PRIMARY KEY ("FHIRContactPointNum")
);


-- raw.fhirsubscription definition

-- Drop table

-- DROP TABLE raw.fhirsubscription;

CREATE TABLE raw.fhirsubscription (
	"FHIRSubscriptionNum" int8 NOT NULL,
	"Criteria" varchar(255) NULL,
	"Reason" varchar(255) NULL,
	"SubStatus" bool NULL,
	"ErrorNote" text NULL,
	"ChannelType" bool NULL,
	"ChannelEndpoint" varchar(255) NULL,
	"ChannelPayLoad" varchar(255) NULL,
	"ChannelHeader" varchar(255) NULL,
	"DateEnd" timestamp NULL,
	"APIKeyHash" varchar(255) NULL,
	CONSTRAINT fhirsubscription_pkey PRIMARY KEY ("FHIRSubscriptionNum")
);


-- raw.files definition

-- Drop table

-- DROP TABLE raw.files;

CREATE TABLE raw.files (
	"DocNum" int8 NOT NULL,
	"Data" bytea NULL,
	"Thumbnail" bytea NULL,
	CONSTRAINT files_pkey PRIMARY KEY ("DocNum")
);


-- raw.formpat definition

-- Drop table

-- DROP TABLE raw.formpat;

CREATE TABLE raw.formpat (
	"FormPatNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	"FormDateTime" timestamp NULL,
	CONSTRAINT formpat_pkey PRIMARY KEY ("FormPatNum")
);


-- raw.gradingscale definition

-- Drop table

-- DROP TABLE raw.gradingscale;

CREATE TABLE raw.gradingscale (
	"GradingScaleNum" int8 NOT NULL,
	"Description" varchar(255) NULL,
	"ScaleType" bool NULL,
	CONSTRAINT gradingscale_pkey PRIMARY KEY ("GradingScaleNum")
);


-- raw.gradingscaleitem definition

-- Drop table

-- DROP TABLE raw.gradingscaleitem;

CREATE TABLE raw.gradingscaleitem (
	"GradingScaleItemNum" int8 NOT NULL,
	"GradingScaleNum" int8 NULL,
	"GradeShowing" varchar(255) NULL,
	"GradeNumber" float4 NULL,
	"Description" varchar(255) NULL,
	CONSTRAINT gradingscaleitem_pkey PRIMARY KEY ("GradingScaleItemNum")
);


-- raw.grouppermission definition

-- Drop table

-- DROP TABLE raw.grouppermission;

CREATE TABLE raw.grouppermission (
	"GroupPermNum" int8 NOT NULL,
	"NewerDate" date NULL,
	"NewerDays" int4 NULL,
	"UserGroupNum" int8 NULL,
	"PermType" int2 NULL,
	"FKey" int8 NULL,
	CONSTRAINT grouppermission_pkey PRIMARY KEY ("GroupPermNum")
);


-- raw.guardian definition

-- Drop table

-- DROP TABLE raw.guardian;

CREATE TABLE raw.guardian (
	"GuardianNum" int8 NOT NULL,
	"PatNumChild" int8 NULL,
	"PatNumGuardian" int8 NULL,
	"Relationship" int2 NULL,
	"IsGuardian" bool NULL,
	CONSTRAINT guardian_pkey PRIMARY KEY ("GuardianNum")
);


-- raw.hcpcs definition

-- Drop table

-- DROP TABLE raw.hcpcs;

CREATE TABLE raw.hcpcs (
	"HcpcsNum" int8 NOT NULL,
	"HcpcsCode" varchar(255) NULL,
	"DescriptionShort" varchar(255) NULL,
	CONSTRAINT hcpcs_pkey PRIMARY KEY ("HcpcsNum")
);


-- raw.hieclinic definition

-- Drop table

-- DROP TABLE raw.hieclinic;

CREATE TABLE raw.hieclinic (
	"HieClinicNum" int8 NOT NULL,
	"ClinicNum" int8 NULL,
	"SupportedCarrierFlags" bool NULL,
	"PathExportCCD" varchar(255) NULL,
	"TimeOfDayExportCCD" int8 NULL,
	"IsEnabled" bool NULL,
	CONSTRAINT hieclinic_pkey PRIMARY KEY ("HieClinicNum")
);


-- raw.hiequeue definition

-- Drop table

-- DROP TABLE raw.hiequeue;

CREATE TABLE raw.hiequeue (
	"HieQueueNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	CONSTRAINT hiequeue_pkey PRIMARY KEY ("HieQueueNum")
);


-- raw.histappointment definition

-- Drop table

-- DROP TABLE raw.histappointment;

CREATE TABLE raw.histappointment (
	"HistApptNum" int8 NOT NULL,
	"HistUserNum" int8 NULL,
	"HistDateTStamp" timestamp NULL,
	"HistApptAction" int2 NULL,
	"ApptSource" bool NULL,
	"AptNum" int8 NULL,
	"PatNum" int8 NULL,
	"AptStatus" int2 NULL,
	"Pattern" varchar(255) NULL,
	"Confirmed" int8 NULL,
	"TimeLocked" bool NULL,
	"Op" int8 NULL,
	"Note" text NULL,
	"ProvNum" int8 NULL,
	"ProvHyg" int8 NULL,
	"AptDateTime" timestamp NULL,
	"NextAptNum" int8 NULL,
	"UnschedStatus" int8 NULL,
	"IsNewPatient" bool NULL,
	"ProcDescript" varchar(255) NULL,
	"Assistant" int8 NULL,
	"ClinicNum" int8 NULL,
	"IsHygiene" bool NULL,
	"DateTStamp" timestamp NULL,
	"DateTimeArrived" timestamp NULL,
	"DateTimeSeated" timestamp NULL,
	"DateTimeDismissed" timestamp NULL,
	"InsPlan1" int8 NULL,
	"InsPlan2" int8 NULL,
	"DateTimeAskedToArrive" timestamp NULL,
	"ProcsColored" text NULL,
	"ColorOverride" int4 NULL,
	"AppointmentTypeNum" int8 NULL,
	"SecUserNumEntry" int8 NULL,
	"SecDateTEntry" timestamp NULL,
	"Priority" bool NULL,
	"ProvBarText" varchar(60) NULL,
	"PatternSecondary" varchar(255) NULL,
	"SecurityHash" varchar(255) NULL,
	"ItemOrderPlanned" int4 NULL,
	"IsMirrored" bool NULL,
	CONSTRAINT histappointment_pkey PRIMARY KEY ("HistApptNum")
);


-- raw.hl7msg definition

-- Drop table

-- DROP TABLE raw.hl7msg;

CREATE TABLE raw.hl7msg (
	"HL7MsgNum" int8 NOT NULL,
	"HL7Status" int4 NULL,
	"MsgText" text NULL,
	"AptNum" int8 NULL,
	"DateTStamp" timestamp NULL,
	"PatNum" int8 NULL,
	"Note" text NULL,
	CONSTRAINT hl7msg_pkey PRIMARY KEY ("HL7MsgNum")
);


-- raw.hl7procattach definition

-- Drop table

-- DROP TABLE raw.hl7procattach;

CREATE TABLE raw.hl7procattach (
	"HL7ProcAttachNum" int8 NOT NULL,
	"HL7MsgNum" int8 NULL,
	"ProcNum" int8 NULL,
	CONSTRAINT hl7procattach_pkey PRIMARY KEY ("HL7ProcAttachNum")
);


-- raw.icd10 definition

-- Drop table

-- DROP TABLE raw.icd10;

CREATE TABLE raw.icd10 (
	"Icd10Num" int8 NOT NULL,
	"Icd10Code" varchar(255) NULL,
	"Description" varchar(255) NULL,
	"IsCode" varchar(255) NULL,
	CONSTRAINT icd10_pkey PRIMARY KEY ("Icd10Num")
);


-- raw.icd9 definition

-- Drop table

-- DROP TABLE raw.icd9;

CREATE TABLE raw.icd9 (
	"ICD9Num" int8 NOT NULL,
	"ICD9Code" varchar(255) NULL,
	"Description" varchar(255) NULL,
	"DateTStamp" timestamp NULL,
	CONSTRAINT icd9_pkey PRIMARY KEY ("ICD9Num")
);


-- raw.imagedraw definition

-- Drop table

-- DROP TABLE raw.imagedraw;

CREATE TABLE raw.imagedraw (
	"ImageDrawNum" int8 NOT NULL,
	"DocNum" int8 NULL,
	"MountNum" int8 NULL,
	"ColorDraw" int4 NULL,
	"ColorBack" int4 NULL,
	"DrawingSegment" text NULL,
	"DrawText" varchar(255) NULL,
	"FontSize" float4 NULL,
	"DrawType" int2 NULL,
	"ImageAnnotVendor" bool NULL,
	"Details" text NULL,
	"PearlLayer" bool NULL,
	CONSTRAINT imagedraw_pkey PRIMARY KEY ("ImageDrawNum")
);


-- raw.imagingdevice definition

-- Drop table

-- DROP TABLE raw.imagingdevice;

CREATE TABLE raw.imagingdevice (
	"ImagingDeviceNum" int8 NOT NULL,
	"Description" varchar(255) NULL,
	"ComputerName" varchar(255) NULL,
	"DeviceType" int2 NULL,
	"TwainName" varchar(255) NULL,
	"ItemOrder" int4 NULL,
	"ShowTwainUI" bool NULL,
	CONSTRAINT imagingdevice_pkey PRIMARY KEY ("ImagingDeviceNum")
);


-- raw.insbluebook definition

-- Drop table

-- DROP TABLE raw.insbluebook;

CREATE TABLE raw.insbluebook (
	"InsBlueBookNum" int8 NOT NULL,
	"ProcCodeNum" int8 NULL,
	"CarrierNum" int8 NULL,
	"PlanNum" int8 NULL,
	"GroupNum" varchar(25) NULL,
	"InsPayAmt" float8 NULL,
	"AllowedOverride" float8 NULL,
	"DateTEntry" timestamp NULL,
	"ProcNum" int8 NULL,
	"ProcDate" date NULL,
	"ClaimType" varchar(10) NULL,
	"ClaimNum" int8 NULL,
	CONSTRAINT insbluebook_pkey PRIMARY KEY ("InsBlueBookNum")
);


-- raw.insbluebooklog definition

-- Drop table

-- DROP TABLE raw.insbluebooklog;

CREATE TABLE raw.insbluebooklog (
	"InsBlueBookLogNum" int8 NOT NULL,
	"ClaimProcNum" int8 NULL,
	"AllowedFee" float8 NULL,
	"DateTEntry" timestamp NULL,
	"Description" text NULL,
	CONSTRAINT insbluebooklog_pkey PRIMARY KEY ("InsBlueBookLogNum")
);


-- raw.insbluebookrule definition

-- Drop table

-- DROP TABLE raw.insbluebookrule;

CREATE TABLE raw.insbluebookrule (
	"InsBlueBookRuleNum" int8 NOT NULL,
	"ItemOrder" int2 NULL,
	"RuleType" int2 NULL,
	"LimitValue" int4 NULL,
	"LimitType" bool NULL,
	CONSTRAINT insbluebookrule_pkey PRIMARY KEY ("InsBlueBookRuleNum")
);


-- raw.inseditlog definition

-- Drop table

-- DROP TABLE raw.inseditlog;

CREATE TABLE raw.inseditlog (
	"InsEditLogNum" int8 NOT NULL,
	"FKey" int8 NULL,
	"LogType" int2 NULL,
	"FieldName" varchar(255) NULL,
	"OldValue" varchar(255) NULL,
	"NewValue" varchar(255) NULL,
	"UserNum" int8 NULL,
	"DateTStamp" timestamp NULL,
	"ParentKey" int8 NULL,
	"Description" varchar(255) NULL,
	CONSTRAINT inseditlog_pkey PRIMARY KEY ("InsEditLogNum")
);


-- raw.inseditpatlog definition

-- Drop table

-- DROP TABLE raw.inseditpatlog;

CREATE TABLE raw.inseditpatlog (
	"InsEditPatLogNum" int8 NOT NULL,
	"FKey" int8 NULL,
	"LogType" int2 NULL,
	"FieldName" varchar(255) NULL,
	"OldValue" varchar(255) NULL,
	"NewValue" varchar(255) NULL,
	"UserNum" int8 NULL,
	"DateTStamp" timestamp NULL,
	"ParentKey" int8 NULL,
	"Description" varchar(255) NULL,
	CONSTRAINT inseditpatlog_pkey PRIMARY KEY ("InsEditPatLogNum")
);


-- raw.insfilingcode definition

-- Drop table

-- DROP TABLE raw.insfilingcode;

CREATE TABLE raw.insfilingcode (
	"InsFilingCodeNum" int8 NOT NULL,
	"Descript" varchar(255) NULL,
	"EclaimCode" varchar(100) NULL,
	"ItemOrder" int4 NULL,
	"GroupType" int8 NULL,
	"ExcludeOtherCoverageOnPriClaims" bool NULL,
	CONSTRAINT insfilingcode_pkey PRIMARY KEY ("InsFilingCodeNum")
);


-- raw.insplan definition

-- Drop table

-- DROP TABLE raw.insplan;

CREATE TABLE raw.insplan (
	"PlanNum" int8 NOT NULL,
	"GroupName" varchar(50) NULL,
	"GroupNum" varchar(25) NULL,
	"PlanNote" text NULL,
	"FeeSched" int8 NULL,
	"PlanType" bpchar(1) NULL,
	"ClaimFormNum" int8 NULL,
	"UseAltCode" bool NULL,
	"ClaimsUseUCR" bool NULL,
	"CopayFeeSched" int8 NULL,
	"EmployerNum" int8 NULL,
	"CarrierNum" int8 NULL,
	"AllowedFeeSched" int8 NULL,
	"TrojanID" varchar(100) NULL,
	"DivisionNo" varchar(255) NULL,
	"IsMedical" bool NULL,
	"FilingCode" int8 NULL,
	"DentaideCardSequence" bool NULL,
	"ShowBaseUnits" bool NULL,
	"CodeSubstNone" bool NULL,
	"IsHidden" bool NULL,
	"MonthRenew" int2 NULL,
	"FilingCodeSubtype" int8 NULL,
	"CanadianPlanFlag" varchar(5) NULL,
	"CanadianDiagnosticCode" varchar(255) NULL,
	"CanadianInstitutionCode" varchar(255) NULL,
	"RxBIN" varchar(255) NULL,
	"CobRule" int2 NULL,
	"SopCode" varchar(255) NULL,
	"SecUserNumEntry" int8 NULL,
	"SecDateEntry" date NULL,
	"SecDateTEdit" timestamp NULL,
	"HideFromVerifyList" bool NULL,
	"OrthoType" bool NULL,
	"OrthoAutoProcFreq" bool NULL,
	"OrthoAutoProcCodeNumOverride" int8 NULL,
	"OrthoAutoFeeBilled" float8 NULL,
	"OrthoAutoClaimDaysWait" int4 NULL,
	"BillingType" int8 NULL,
	"HasPpoSubstWriteoffs" bool NULL,
	"ExclusionFeeRule" bool NULL,
	"ManualFeeSchedNum" int8 NULL,
	"IsBlueBookEnabled" bool NULL,
	"InsPlansZeroWriteOffsOnAnnualMaxOverride" bool NULL,
	"InsPlansZeroWriteOffsOnFreqOrAgingOverride" bool NULL,
	"PerVisitPatAmount" float8 NULL,
	"PerVisitInsAmount" float8 NULL,
	CONSTRAINT insplan_pkey PRIMARY KEY ("PlanNum")
);


-- raw.insplanpreference definition

-- Drop table

-- DROP TABLE raw.insplanpreference;

CREATE TABLE raw.insplanpreference (
	"InsPlanPrefNum" int8 NOT NULL,
	"PlanNum" int8 NULL,
	"FKey" int8 NULL,
	"FKeyType" bool NULL,
	"ValueString" text NULL,
	CONSTRAINT insplanpreference_pkey PRIMARY KEY ("InsPlanPrefNum")
);


-- raw.inssub definition

-- Drop table

-- DROP TABLE raw.inssub;

CREATE TABLE raw.inssub (
	"InsSubNum" int8 NOT NULL,
	"PlanNum" int8 NULL,
	"Subscriber" int8 NULL,
	"DateEffective" date NULL,
	"DateTerm" date NULL,
	"ReleaseInfo" bool NULL,
	"AssignBen" bool NULL,
	"SubscriberID" varchar(255) NULL,
	"BenefitNotes" text NULL,
	"SubscNote" text NULL,
	"SecUserNumEntry" int8 NULL,
	"SecDateEntry" date NULL,
	"SecDateTEdit" timestamp NULL,
	"SecurityHash" varchar(255) NULL,
	CONSTRAINT inssub_pkey PRIMARY KEY ("InsSubNum")
);


-- raw.installmentplan definition

-- Drop table

-- DROP TABLE raw.installmentplan;

CREATE TABLE raw.installmentplan (
	"InstallmentPlanNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	"DateAgreement" date NULL,
	"DateFirstPayment" date NULL,
	"MonthlyPayment" float8 NULL,
	"APR" float4 NULL,
	"Note" varchar(255) NULL,
	CONSTRAINT installmentplan_pkey PRIMARY KEY ("InstallmentPlanNum")
);


-- raw.instructor definition

-- Drop table

-- DROP TABLE raw.instructor;

CREATE TABLE raw.instructor (
	"InstructorNum" int4 NOT NULL,
	"LName" varchar(255) NULL,
	"FName" varchar(255) NULL,
	"Suffix" varchar(100) NULL,
	CONSTRAINT instructor_pkey PRIMARY KEY ("InstructorNum")
);


-- raw.insverify definition

-- Drop table

-- DROP TABLE raw.insverify;

CREATE TABLE raw.insverify (
	"InsVerifyNum" int8 NOT NULL,
	"DateLastVerified" date NULL,
	"UserNum" int8 NULL,
	"VerifyType" int2 NULL,
	"FKey" int8 NULL,
	"DefNum" int8 NULL,
	"Note" text NULL,
	"DateLastAssigned" date NULL,
	"DateTimeEntry" timestamp NULL,
	"HoursAvailableForVerification" float8 NULL,
	"SecDateTEdit" timestamp NULL,
	"SecurityHash" varchar(255) NULL,
	CONSTRAINT insverify_pkey PRIMARY KEY ("InsVerifyNum")
);


-- raw.insverifyhist definition

-- Drop table

-- DROP TABLE raw.insverifyhist;

CREATE TABLE raw.insverifyhist (
	"InsVerifyHistNum" int8 NOT NULL,
	"InsVerifyNum" int8 NULL,
	"DateLastVerified" date NULL,
	"UserNum" int8 NULL,
	"VerifyType" int2 NULL,
	"FKey" int8 NULL,
	"DefNum" int8 NULL,
	"Note" text NULL,
	"DateLastAssigned" date NULL,
	"DateTimeEntry" timestamp NULL,
	"HoursAvailableForVerification" float8 NULL,
	"VerifyUserNum" int8 NULL,
	"SecDateTEdit" timestamp NULL,
	"SecurityHash" varchar(255) NULL,
	CONSTRAINT insverifyhist_pkey PRIMARY KEY ("InsVerifyHistNum")
);


-- raw.intervention definition

-- Drop table

-- DROP TABLE raw.intervention;

CREATE TABLE raw.intervention (
	"InterventionNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	"ProvNum" int8 NULL,
	"CodeValue" varchar(30) NULL,
	"CodeSystem" varchar(30) NULL,
	"Note" text NULL,
	"DateEntry" date NULL,
	"CodeSet" bool NULL,
	"IsPatDeclined" bool NULL,
	CONSTRAINT intervention_pkey PRIMARY KEY ("InterventionNum")
);


-- raw.journalentry definition

-- Drop table

-- DROP TABLE raw.journalentry;

CREATE TABLE raw.journalentry (
	"JournalEntryNum" int8 NOT NULL,
	"TransactionNum" int8 NULL,
	"AccountNum" int8 NULL,
	"DateDisplayed" date NULL,
	"DebitAmt" float8 NULL,
	"CreditAmt" float8 NULL,
	"Memo" text NULL,
	"Splits" text NULL,
	"CheckNumber" varchar(255) NULL,
	"ReconcileNum" int8 NULL,
	"SecUserNumEntry" int8 NULL,
	"SecDateTEntry" timestamp NULL,
	"SecUserNumEdit" int8 NULL,
	"SecDateTEdit" timestamp NULL,
	CONSTRAINT journalentry_pkey PRIMARY KEY ("JournalEntryNum")
);


-- raw.labcase definition

-- Drop table

-- DROP TABLE raw.labcase;

CREATE TABLE raw.labcase (
	"LabCaseNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	"LaboratoryNum" int8 NULL,
	"AptNum" int8 NULL,
	"PlannedAptNum" int8 NULL,
	"DateTimeDue" timestamp NULL,
	"DateTimeCreated" timestamp NULL,
	"DateTimeSent" timestamp NULL,
	"DateTimeRecd" timestamp NULL,
	"DateTimeChecked" timestamp NULL,
	"ProvNum" int8 NULL,
	"Instructions" text NULL,
	"LabFee" float8 NULL,
	"DateTStamp" timestamp NULL,
	"InvoiceNum" varchar(255) NULL,
	CONSTRAINT labcase_pkey PRIMARY KEY ("LabCaseNum")
);


-- raw.laboratory definition

-- Drop table

-- DROP TABLE raw.laboratory;

CREATE TABLE raw.laboratory (
	"LaboratoryNum" int8 NOT NULL,
	"Description" varchar(255) NULL,
	"Phone" varchar(255) NULL,
	"Notes" text NULL,
	"Slip" int8 NULL,
	"Address" varchar(255) NULL,
	"City" varchar(255) NULL,
	"State" varchar(255) NULL,
	"Zip" varchar(255) NULL,
	"Email" varchar(255) NULL,
	"WirelessPhone" varchar(255) NULL,
	"IsHidden" bool NULL,
	CONSTRAINT laboratory_pkey PRIMARY KEY ("LaboratoryNum")
);


-- raw.labpanel definition

-- Drop table

-- DROP TABLE raw.labpanel;

CREATE TABLE raw.labpanel (
	"LabPanelNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	"RawMessage" text NULL,
	"LabNameAddress" varchar(255) NULL,
	"DateTStamp" timestamp NULL,
	"SpecimenCondition" varchar(255) NULL,
	"SpecimenSource" varchar(255) NULL,
	"ServiceId" varchar(255) NULL,
	"ServiceName" varchar(255) NULL,
	"MedicalOrderNum" int8 NULL,
	CONSTRAINT labpanel_pkey PRIMARY KEY ("LabPanelNum")
);


-- raw.labresult definition

-- Drop table

-- DROP TABLE raw.labresult;

CREATE TABLE raw.labresult (
	"LabResultNum" int8 NOT NULL,
	"LabPanelNum" int8 NULL,
	"DateTimeTest" timestamp NULL,
	"TestName" varchar(255) NULL,
	"DateTStamp" timestamp NULL,
	"TestID" varchar(255) NULL,
	"ObsValue" varchar(255) NULL,
	"ObsUnits" varchar(255) NULL,
	"ObsRange" varchar(255) NULL,
	"AbnormalFlag" bool NULL,
	CONSTRAINT labresult_pkey PRIMARY KEY ("LabResultNum")
);


-- raw.labturnaround definition

-- Drop table

-- DROP TABLE raw.labturnaround;

CREATE TABLE raw.labturnaround (
	"LabTurnaroundNum" int8 NOT NULL,
	"LaboratoryNum" int8 NULL,
	"Description" varchar(255) NULL,
	"DaysPublished" int2 NULL,
	"DaysActual" int2 NULL,
	CONSTRAINT labturnaround_pkey PRIMARY KEY ("LabTurnaroundNum")
);


-- raw."language" definition

-- Drop table

-- DROP TABLE raw."language";

CREATE TABLE raw."language" (
	"LanguageNum" int8 NOT NULL,
	"EnglishComments" text NULL,
	"ClassType" text NULL,
	"English" text NULL,
	"IsObsolete" bool NULL,
	CONSTRAINT language_pkey PRIMARY KEY ("LanguageNum")
);


-- raw.languageforeign definition

-- Drop table

-- DROP TABLE raw.languageforeign;

CREATE TABLE raw.languageforeign (
	"LanguageForeignNum" int8 NOT NULL,
	"ClassType" text NULL,
	"English" text NULL,
	"Culture" varchar(255) NULL,
	"Translation" text NULL,
	"Comments" text NULL,
	CONSTRAINT languageforeign_pkey PRIMARY KEY ("LanguageForeignNum")
);


-- raw.languagepat definition

-- Drop table

-- DROP TABLE raw.languagepat;

CREATE TABLE raw.languagepat (
	"LanguagePatNum" int8 NOT NULL,
	"PrefName" varchar(255) NULL,
	"Language" varchar(255) NULL,
	"Translation" text NULL,
	"EFormFieldDefNum" int8 NULL,
	CONSTRAINT languagepat_pkey PRIMARY KEY ("LanguagePatNum")
);


-- raw.letter definition

-- Drop table

-- DROP TABLE raw.letter;

CREATE TABLE raw.letter (
	"LetterNum" int8 NOT NULL,
	"Description" varchar(255) NULL,
	"BodyText" text NULL,
	CONSTRAINT letter_pkey PRIMARY KEY ("LetterNum")
);


-- raw.lettermerge definition

-- Drop table

-- DROP TABLE raw.lettermerge;

CREATE TABLE raw.lettermerge (
	"LetterMergeNum" int8 NOT NULL,
	"Description" varchar(255) NULL,
	"TemplateName" varchar(255) NULL,
	"DataFileName" varchar(255) NULL,
	"Category" int8 NULL,
	"ImageFolder" int8 NULL,
	CONSTRAINT lettermerge_pkey PRIMARY KEY ("LetterMergeNum")
);


-- raw.lettermergefield definition

-- Drop table

-- DROP TABLE raw.lettermergefield;

CREATE TABLE raw.lettermergefield (
	"FieldNum" int8 NOT NULL,
	"LetterMergeNum" int8 NULL,
	"FieldName" varchar(255) NULL,
	CONSTRAINT lettermergefield_pkey PRIMARY KEY ("FieldNum")
);


-- raw.limitedbetafeature definition

-- Drop table

-- DROP TABLE raw.limitedbetafeature;

CREATE TABLE raw.limitedbetafeature (
	"LimitedBetaFeatureNum" int8 NOT NULL,
	"LimitedBetaFeatureTypeNum" int8 NULL,
	"ClinicNum" int8 NULL,
	"IsSignedUp" bool NULL,
	CONSTRAINT limitedbetafeature_pkey PRIMARY KEY ("LimitedBetaFeatureNum")
);


-- raw.loginattempt definition

-- Drop table

-- DROP TABLE raw.loginattempt;

CREATE TABLE raw.loginattempt (
	"LoginAttemptNum" int8 NOT NULL,
	"UserName" varchar(255) NULL,
	"LoginType" bool NULL,
	"DateTFail" timestamp NULL,
	CONSTRAINT loginattempt_pkey PRIMARY KEY ("LoginAttemptNum")
);


-- raw.loinc definition

-- Drop table

-- DROP TABLE raw.loinc;

CREATE TABLE raw.loinc (
	"LoincNum" int8 NOT NULL,
	"LoincCode" varchar(255) NULL,
	"Component" varchar(255) NULL,
	"PropertyObserved" varchar(255) NULL,
	"TimeAspct" varchar(255) NULL,
	"SystemMeasured" varchar(255) NULL,
	"ScaleType" varchar(255) NULL,
	"MethodType" varchar(255) NULL,
	"StatusOfCode" varchar(255) NULL,
	"NameShort" varchar(255) NULL,
	"ClassType" varchar(255) NULL,
	"UnitsRequired" bool NULL,
	"OrderObs" varchar(255) NULL,
	"HL7FieldSubfieldID" varchar(255) NULL,
	"ExternalCopyrightNotice" text NULL,
	"NameLongCommon" varchar(255) NULL,
	"UnitsUCUM" varchar(255) NULL,
	"RankCommonTests" int4 NULL,
	"RankCommonOrders" int4 NULL,
	CONSTRAINT loinc_pkey PRIMARY KEY ("LoincNum")
);


-- raw.maparea definition

-- Drop table

-- DROP TABLE raw.maparea;

CREATE TABLE raw.maparea (
	"MapAreaNum" int8 NOT NULL,
	"Extension" int4 NULL,
	"XPos" float8 NULL,
	"YPos" float8 NULL,
	"Width" float8 NULL,
	"Height" float8 NULL,
	"Description" varchar(255) NULL,
	"ItemType" bool NULL,
	"MapAreaContainerNum" int8 NULL,
	CONSTRAINT maparea_pkey PRIMARY KEY ("MapAreaNum")
);


-- raw.medicalorder definition

-- Drop table

-- DROP TABLE raw.medicalorder;

CREATE TABLE raw.medicalorder (
	"MedicalOrderNum" int8 NOT NULL,
	"MedOrderType" bool NULL,
	"PatNum" int8 NULL,
	"DateTimeOrder" timestamp NULL,
	"Description" varchar(255) NULL,
	"IsDiscontinued" bool NULL,
	"ProvNum" int8 NULL,
	CONSTRAINT medicalorder_pkey PRIMARY KEY ("MedicalOrderNum")
);


-- raw.medication definition

-- Drop table

-- DROP TABLE raw.medication;

CREATE TABLE raw.medication (
	"MedicationNum" int8 NOT NULL,
	"MedName" varchar(255) NULL,
	"GenericNum" int8 NULL,
	"Notes" text NULL,
	"DateTStamp" timestamp NULL,
	"RxCui" int8 NULL,
	CONSTRAINT medication_pkey PRIMARY KEY ("MedicationNum")
);


-- raw.medicationpat definition

-- Drop table

-- DROP TABLE raw.medicationpat;

CREATE TABLE raw.medicationpat (
	"MedicationPatNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	"MedicationNum" int8 NULL,
	"PatNote" text NULL,
	"DateTStamp" timestamp NULL,
	"DateStart" date NULL,
	"DateStop" date NULL,
	"ProvNum" int8 NULL,
	"MedDescript" varchar(255) NULL,
	"RxCui" int8 NULL,
	"ErxGuid" varchar(255) NULL,
	"IsCpoe" bool NULL,
	CONSTRAINT medicationpat_pkey PRIMARY KEY ("MedicationPatNum")
);


-- raw.medlab definition

-- Drop table

-- DROP TABLE raw.medlab;

CREATE TABLE raw.medlab (
	"MedLabNum" int8 NOT NULL,
	"SendingApp" varchar(255) NULL,
	"SendingFacility" varchar(255) NULL,
	"PatNum" int8 NULL,
	"ProvNum" int8 NULL,
	"PatIDLab" varchar(255) NULL,
	"PatIDAlt" varchar(255) NULL,
	"PatAge" varchar(255) NULL,
	"PatAccountNum" varchar(255) NULL,
	"PatFasting" bool NULL,
	"SpecimenID" varchar(255) NULL,
	"SpecimenIDFiller" varchar(255) NULL,
	"ObsTestID" varchar(255) NULL,
	"ObsTestDescript" varchar(255) NULL,
	"ObsTestLoinc" varchar(255) NULL,
	"ObsTestLoincText" varchar(255) NULL,
	"DateTimeCollected" timestamp NULL,
	"TotalVolume" varchar(255) NULL,
	"ActionCode" varchar(255) NULL,
	"ClinicalInfo" varchar(255) NULL,
	"DateTimeEntered" timestamp NULL,
	"OrderingProvNPI" varchar(255) NULL,
	"OrderingProvLocalID" varchar(255) NULL,
	"OrderingProvLName" varchar(255) NULL,
	"OrderingProvFName" varchar(255) NULL,
	"SpecimenIDAlt" varchar(255) NULL,
	"DateTimeReported" timestamp NULL,
	"ResultStatus" varchar(255) NULL,
	"ParentObsID" varchar(255) NULL,
	"ParentObsTestID" varchar(255) NULL,
	"NotePat" text NULL,
	"NoteLab" text NULL,
	"FileName" varchar(255) NULL,
	"OriginalPIDSegment" text NULL,
	CONSTRAINT medlab_pkey PRIMARY KEY ("MedLabNum")
);


-- raw.medlabfacattach definition

-- Drop table

-- DROP TABLE raw.medlabfacattach;

CREATE TABLE raw.medlabfacattach (
	"MedLabFacAttachNum" int8 NOT NULL,
	"MedLabNum" int8 NULL,
	"MedLabResultNum" int8 NULL,
	"MedLabFacilityNum" int8 NULL,
	CONSTRAINT medlabfacattach_pkey PRIMARY KEY ("MedLabFacAttachNum")
);


-- raw.medlabfacility definition

-- Drop table

-- DROP TABLE raw.medlabfacility;

CREATE TABLE raw.medlabfacility (
	"MedLabFacilityNum" int8 NOT NULL,
	"FacilityName" varchar(255) NULL,
	"Address" varchar(255) NULL,
	"City" varchar(255) NULL,
	"State" varchar(255) NULL,
	"Zip" varchar(255) NULL,
	"Phone" varchar(255) NULL,
	"DirectorTitle" varchar(255) NULL,
	"DirectorLName" varchar(255) NULL,
	"DirectorFName" varchar(255) NULL,
	CONSTRAINT medlabfacility_pkey PRIMARY KEY ("MedLabFacilityNum")
);


-- raw.medlabresult definition

-- Drop table

-- DROP TABLE raw.medlabresult;

CREATE TABLE raw.medlabresult (
	"MedLabResultNum" int8 NOT NULL,
	"MedLabNum" int8 NULL,
	"ObsID" varchar(255) NULL,
	"ObsText" varchar(255) NULL,
	"ObsLoinc" varchar(255) NULL,
	"ObsLoincText" varchar(255) NULL,
	"ObsIDSub" varchar(255) NULL,
	"ObsValue" text NULL,
	"ObsSubType" varchar(255) NULL,
	"ObsUnits" varchar(255) NULL,
	"ReferenceRange" varchar(255) NULL,
	"AbnormalFlag" varchar(255) NULL,
	"ResultStatus" varchar(255) NULL,
	"DateTimeObs" timestamp NULL,
	"FacilityID" varchar(255) NULL,
	"DocNum" int8 NULL,
	"Note" text NULL,
	CONSTRAINT medlabresult_pkey PRIMARY KEY ("MedLabResultNum")
);


-- raw.medlabspecimen definition

-- Drop table

-- DROP TABLE raw.medlabspecimen;

CREATE TABLE raw.medlabspecimen (
	"MedLabSpecimenNum" int8 NOT NULL,
	"MedLabNum" int8 NULL,
	"SpecimenID" varchar(255) NULL,
	"SpecimenDescript" varchar(255) NULL,
	"DateTimeCollected" timestamp NULL,
	CONSTRAINT medlabspecimen_pkey PRIMARY KEY ("MedLabSpecimenNum")
);


-- raw.mobileappdevice definition

-- Drop table

-- DROP TABLE raw.mobileappdevice;

CREATE TABLE raw.mobileappdevice (
	"MobileAppDeviceNum" int8 NOT NULL,
	"ClinicNum" int8 NULL,
	"DeviceName" varchar(255) NULL,
	"UniqueID" varchar(255) NULL,
	"IsEclipboardEnabled" bool NULL,
	"EclipboardLastAttempt" timestamp NULL,
	"EclipboardLastLogin" timestamp NULL,
	"PatNum" int8 NULL,
	"LastCheckInActivity" timestamp NULL,
	"IsBYODDevice" bool NULL,
	"DevicePage" int2 NULL,
	"UserNum" int8 NULL,
	"IsODTouchEnabled" bool NULL,
	"ODTouchLastLogin" timestamp NULL,
	"ODTouchLastAttempt" timestamp NULL,
	CONSTRAINT mobileappdevice_pkey PRIMARY KEY ("MobileAppDeviceNum")
);


-- raw.mobilebrandingprofile definition

-- Drop table

-- DROP TABLE raw.mobilebrandingprofile;

CREATE TABLE raw.mobilebrandingprofile (
	"MobileBrandingProfileNum" int8 NOT NULL,
	"ClinicNum" int8 NULL,
	"OfficeDescription" varchar(255) NULL,
	"LogoFilePath" varchar(255) NULL,
	"DateTStamp" timestamp NULL,
	CONSTRAINT mobilebrandingprofile_pkey PRIMARY KEY ("MobileBrandingProfileNum")
);


-- raw.mobiledatabyte definition

-- Drop table

-- DROP TABLE raw.mobiledatabyte;

CREATE TABLE raw.mobiledatabyte (
	"MobileDataByteNum" int8 NOT NULL,
	"RawBase64Data" text NULL,
	"RawBase64Code" text NULL,
	"RawBase64Tag" text NULL,
	"PatNum" int8 NULL,
	"ActionType" bool NULL,
	"DateTimeEntry" timestamp NULL,
	"DateTimeExpires" timestamp NULL,
	CONSTRAINT mobiledatabyte_pkey PRIMARY KEY ("MobileDataByteNum")
);


-- raw.mobilenotification definition

-- Drop table

-- DROP TABLE raw.mobilenotification;

CREATE TABLE raw.mobilenotification (
	"MobileNotificationNum" int8 NOT NULL,
	"NotificationType" bool NULL,
	"DeviceId" varchar(255) NULL,
	"PrimaryKeys" text NULL,
	"Tags" text NULL,
	"DateTimeEntry" timestamp NULL,
	"DateTimeExpires" timestamp NULL,
	"AppTarget" bool NULL,
	CONSTRAINT mobilenotification_pkey PRIMARY KEY ("MobileNotificationNum")
);


-- raw.mount definition

-- Drop table

-- DROP TABLE raw.mount;

CREATE TABLE raw.mount (
	"MountNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	"DocCategory" int8 NULL,
	"DateCreated" timestamp NULL,
	"Description" varchar(255) NULL,
	"Note" text NULL,
	"Width" int4 NULL,
	"Height" int4 NULL,
	"ColorBack" int4 NULL,
	"ProvNum" int8 NULL,
	"ColorFore" int4 NULL,
	"ColorTextBack" int4 NULL,
	"FlipOnAcquire" bool NULL,
	"AdjModeAfterSeries" bool NULL,
	CONSTRAINT mount_pkey PRIMARY KEY ("MountNum")
);


-- raw.mountdef definition

-- Drop table

-- DROP TABLE raw.mountdef;

CREATE TABLE raw.mountdef (
	"MountDefNum" int8 NOT NULL,
	"Description" varchar(255) NULL,
	"ItemOrder" int4 NULL,
	"Width" int4 NULL,
	"Height" int4 NULL,
	"ColorBack" int4 NULL,
	"ColorFore" int4 NULL,
	"ColorTextBack" int4 NULL,
	"ScaleValue" varchar(255) NULL,
	"DefaultCat" int8 NULL,
	"FlipOnAcquire" bool NULL,
	"AdjModeAfterSeries" bool NULL,
	CONSTRAINT mountdef_pkey PRIMARY KEY ("MountDefNum")
);


-- raw.mountitem definition

-- Drop table

-- DROP TABLE raw.mountitem;

CREATE TABLE raw.mountitem (
	"MountItemNum" int8 NOT NULL,
	"MountNum" int8 NULL,
	"Xpos" int4 NULL,
	"Ypos" int4 NULL,
	"ItemOrder" int4 NULL,
	"Width" int4 NULL,
	"Height" int4 NULL,
	"RotateOnAcquire" int4 NULL,
	"ToothNumbers" varchar(255) NULL,
	"TextShowing" text NULL,
	"FontSize" float4 NULL,
	CONSTRAINT mountitem_pkey PRIMARY KEY ("MountItemNum")
);


-- raw.mountitemdef definition

-- Drop table

-- DROP TABLE raw.mountitemdef;

CREATE TABLE raw.mountitemdef (
	"MountItemDefNum" int8 NOT NULL,
	"MountDefNum" int8 NULL,
	"Xpos" int4 NULL,
	"Ypos" int4 NULL,
	"Width" int4 NULL,
	"Height" int4 NULL,
	"ItemOrder" int4 NULL,
	"RotateOnAcquire" int4 NULL,
	"ToothNumbers" varchar(255) NULL,
	"TextShowing" text NULL,
	"FontSize" float4 NULL,
	CONSTRAINT mountitemdef_pkey PRIMARY KEY ("MountItemDefNum")
);


-- raw.msgtopaysent definition

-- Drop table

-- DROP TABLE raw.msgtopaysent;

CREATE TABLE raw.msgtopaysent (
	"MsgToPaySentNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	"ClinicNum" int8 NULL,
	"SendStatus" bool NULL,
	"Source" bool NULL,
	"MessageType" bool NULL,
	"MessageFk" int8 NULL,
	"Subject" text NULL,
	"Message" text NULL,
	"EmailType" bool NULL,
	"DateTimeEntry" timestamp NULL,
	"DateTimeSent" timestamp NULL,
	"ResponseDescript" text NULL,
	"ApptReminderRuleNum" int8 NULL,
	"ShortGUID" varchar(255) NULL,
	"DateTimeSendFailed" timestamp NULL,
	"ApptNum" int8 NULL,
	"ApptDateTime" timestamp NULL,
	"TSPrior" int8 NULL,
	"StatementNum" int8 NULL,
	CONSTRAINT msgtopaysent_pkey PRIMARY KEY ("MsgToPaySentNum")
);


-- raw.oidexternal definition

-- Drop table

-- DROP TABLE raw.oidexternal;

CREATE TABLE raw.oidexternal (
	"OIDExternalNum" int8 NOT NULL,
	"IDType" varchar(255) NULL,
	"IDInternal" int8 NULL,
	"IDExternal" varchar(255) NULL,
	"rootExternal" varchar(255) NULL,
	CONSTRAINT oidexternal_pkey PRIMARY KEY ("OIDExternalNum")
);


-- raw.oidinternal definition

-- Drop table

-- DROP TABLE raw.oidinternal;

CREATE TABLE raw.oidinternal (
	"OIDInternalNum" int8 NOT NULL,
	"IDType" varchar(255) NULL,
	"IDRoot" varchar(255) NULL,
	CONSTRAINT oidinternal_pkey PRIMARY KEY ("OIDInternalNum")
);


-- raw.operatory definition

-- Drop table

-- DROP TABLE raw.operatory;

CREATE TABLE raw.operatory (
	"OperatoryNum" int8 NOT NULL,
	"OpName" varchar(255) NULL,
	"Abbrev" varchar(255) NULL,
	"ItemOrder" int2 NULL,
	"IsHidden" bool NULL,
	"ProvDentist" int8 NULL,
	"ProvHygienist" int8 NULL,
	"IsHygiene" bool NULL,
	"ClinicNum" int8 NULL,
	"DateTStamp" timestamp NULL,
	"SetProspective" bool NULL,
	"IsWebSched" bool NULL,
	"IsNewPatAppt" bool NULL,
	"OperatoryType" int8 NULL,
	CONSTRAINT operatory_pkey PRIMARY KEY ("OperatoryNum")
);


-- raw.orionproc definition

-- Drop table

-- DROP TABLE raw.orionproc;

CREATE TABLE raw.orionproc (
	"OrionProcNum" int8 NOT NULL,
	"ProcNum" int8 NULL,
	"DPC" bool NULL,
	"DateScheduleBy" date NULL,
	"DateStopClock" date NULL,
	"Status2" int4 NULL,
	"IsOnCall" bool NULL,
	"IsEffectiveComm" bool NULL,
	"IsRepair" bool NULL,
	"DPCpost" bool NULL,
	CONSTRAINT orionproc_pkey PRIMARY KEY ("OrionProcNum")
);


-- raw.orthocase definition

-- Drop table

-- DROP TABLE raw.orthocase;

CREATE TABLE raw.orthocase (
	"OrthoCaseNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	"ProvNum" int8 NULL,
	"ClinicNum" int8 NULL,
	"Fee" float8 NULL,
	"FeeInsPrimary" float8 NULL,
	"FeePat" float8 NULL,
	"BandingDate" date NULL,
	"DebondDate" date NULL,
	"DebondDateExpected" date NULL,
	"IsTransfer" bool NULL,
	"OrthoType" int8 NULL,
	"SecDateTEntry" timestamp NULL,
	"SecUserNumEntry" int8 NULL,
	"SecDateTEdit" timestamp NULL,
	"IsActive" bool NULL,
	"FeeInsSecondary" float8 NULL,
	CONSTRAINT orthocase_pkey PRIMARY KEY ("OrthoCaseNum")
);


-- raw.orthochart definition

-- Drop table

-- DROP TABLE raw.orthochart;

CREATE TABLE raw.orthochart (
	"OrthoChartNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	"DateService" date NULL,
	"FieldName" varchar(255) NULL,
	"FieldValue" text NULL,
	"UserNum" int8 NULL,
	"ProvNum" int8 NULL,
	"OrthoChartRowNum" int8 NULL,
	CONSTRAINT orthochart_pkey PRIMARY KEY ("OrthoChartNum")
);


-- raw.orthochartlog definition

-- Drop table

-- DROP TABLE raw.orthochartlog;

CREATE TABLE raw.orthochartlog (
	"OrthoChartLogNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	"ComputerName" varchar(255) NULL,
	"DateTimeLog" timestamp NULL,
	"DateTimeService" timestamp NULL,
	"UserNum" int8 NULL,
	"ProvNum" int8 NULL,
	"OrthoChartRowNum" int8 NULL,
	"LogData" text NULL,
	CONSTRAINT orthochartlog_pkey PRIMARY KEY ("OrthoChartLogNum")
);


-- raw.orthochartrow definition

-- Drop table

-- DROP TABLE raw.orthochartrow;

CREATE TABLE raw.orthochartrow (
	"OrthoChartRowNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	"DateTimeService" timestamp NULL,
	"UserNum" int8 NULL,
	"ProvNum" int8 NULL,
	"Signature" text NULL,
	CONSTRAINT orthochartrow_pkey PRIMARY KEY ("OrthoChartRowNum")
);


-- raw.orthocharttab definition

-- Drop table

-- DROP TABLE raw.orthocharttab;

CREATE TABLE raw.orthocharttab (
	"OrthoChartTabNum" int8 NOT NULL,
	"TabName" varchar(255) NULL,
	"ItemOrder" int4 NULL,
	"IsHidden" bool NULL,
	CONSTRAINT orthocharttab_pkey PRIMARY KEY ("OrthoChartTabNum")
);


-- raw.orthocharttablink definition

-- Drop table

-- DROP TABLE raw.orthocharttablink;

CREATE TABLE raw.orthocharttablink (
	"OrthoChartTabLinkNum" int8 NOT NULL,
	"ItemOrder" int4 NULL,
	"OrthoChartTabNum" int8 NULL,
	"DisplayFieldNum" int8 NULL,
	"ColumnWidthOverride" int4 NULL,
	CONSTRAINT orthocharttablink_pkey PRIMARY KEY ("OrthoChartTabLinkNum")
);


-- raw.orthohardware definition

-- Drop table

-- DROP TABLE raw.orthohardware;

CREATE TABLE raw.orthohardware (
	"OrthoHardwareNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	"DateExam" date NULL,
	"OrthoHardwareType" bool NULL,
	"OrthoHardwareSpecNum" int8 NULL,
	"ToothRange" varchar(255) NULL,
	"Note" varchar(255) NULL,
	"IsHidden" bool NULL,
	CONSTRAINT orthohardware_pkey PRIMARY KEY ("OrthoHardwareNum")
);


-- raw.orthohardwarespec definition

-- Drop table

-- DROP TABLE raw.orthohardwarespec;

CREATE TABLE raw.orthohardwarespec (
	"OrthoHardwareSpecNum" int8 NOT NULL,
	"OrthoHardwareType" int2 NULL,
	"Description" varchar(255) NULL,
	"ItemColor" int4 NULL,
	"IsHidden" bool NULL,
	"ItemOrder" int4 NULL,
	CONSTRAINT orthohardwarespec_pkey PRIMARY KEY ("OrthoHardwareSpecNum")
);


-- raw.orthoplanlink definition

-- Drop table

-- DROP TABLE raw.orthoplanlink;

CREATE TABLE raw.orthoplanlink (
	"OrthoPlanLinkNum" int8 NOT NULL,
	"OrthoCaseNum" int8 NULL,
	"LinkType" bool NULL,
	"FKey" int8 NULL,
	"IsActive" bool NULL,
	"SecDateTEntry" timestamp NULL,
	"SecUserNumEntry" int8 NULL,
	CONSTRAINT orthoplanlink_pkey PRIMARY KEY ("OrthoPlanLinkNum")
);


-- raw.orthoproclink definition

-- Drop table

-- DROP TABLE raw.orthoproclink;

CREATE TABLE raw.orthoproclink (
	"OrthoProcLinkNum" int8 NOT NULL,
	"OrthoCaseNum" int8 NULL,
	"ProcNum" int8 NULL,
	"SecDateTEntry" timestamp NULL,
	"SecUserNumEntry" int8 NULL,
	"ProcLinkType" bool NULL,
	CONSTRAINT orthoproclink_pkey PRIMARY KEY ("OrthoProcLinkNum")
);


-- raw.orthorx definition

-- Drop table

-- DROP TABLE raw.orthorx;

CREATE TABLE raw.orthorx (
	"OrthoRxNum" int8 NOT NULL,
	"OrthoHardwareSpecNum" int8 NULL,
	"Description" varchar(255) NULL,
	"ToothRange" varchar(255) NULL,
	"ItemOrder" int4 NULL,
	CONSTRAINT orthorx_pkey PRIMARY KEY ("OrthoRxNum")
);


-- raw.orthoschedule definition

-- Drop table

-- DROP TABLE raw.orthoschedule;

CREATE TABLE raw.orthoschedule (
	"OrthoScheduleNum" int8 NOT NULL,
	"BandingDateOverride" date NULL,
	"DebondDateOverride" date NULL,
	"BandingAmount" float8 NULL,
	"VisitAmount" float8 NULL,
	"DebondAmount" float8 NULL,
	"IsActive" bool NULL,
	"SecDateTEdit" timestamp NULL,
	CONSTRAINT orthoschedule_pkey PRIMARY KEY ("OrthoScheduleNum")
);


-- raw.patfield definition

-- Drop table

-- DROP TABLE raw.patfield;

CREATE TABLE raw.patfield (
	"PatFieldNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	"FieldName" varchar(255) NULL,
	"FieldValue" text NULL,
	"SecUserNumEntry" int8 NULL,
	"SecDateEntry" date NULL,
	"SecDateTEdit" timestamp NULL,
	CONSTRAINT patfield_pkey PRIMARY KEY ("PatFieldNum")
);


-- raw.patfielddef definition

-- Drop table

-- DROP TABLE raw.patfielddef;

CREATE TABLE raw.patfielddef (
	"PatFieldDefNum" int8 NOT NULL,
	"FieldName" varchar(255) NULL,
	"FieldType" int2 NULL,
	"PickList" text NULL,
	"ItemOrder" int4 NULL,
	"IsHidden" bool NULL,
	CONSTRAINT patfielddef_pkey PRIMARY KEY ("PatFieldDefNum")
);


-- raw.patfieldpickitem definition

-- Drop table

-- DROP TABLE raw.patfieldpickitem;

CREATE TABLE raw.patfieldpickitem (
	"PatFieldPickItemNum" int8 NOT NULL,
	"PatFieldDefNum" int8 NULL,
	"Name" varchar(255) NULL,
	"Abbreviation" varchar(255) NULL,
	"IsHidden" bool NULL,
	"ItemOrder" int4 NULL,
	CONSTRAINT patfieldpickitem_pkey PRIMARY KEY ("PatFieldPickItemNum")
);


-- raw.patient definition

-- Drop table

-- DROP TABLE raw.patient;

CREATE TABLE raw.patient (
	"PatNum" int8 NOT NULL,
	"LName" varchar(100) NULL,
	"FName" varchar(100) NULL,
	"MiddleI" varchar(100) NULL,
	"Preferred" varchar(100) NULL,
	"PatStatus" int2 NULL,
	"Gender" int2 NULL,
	"Position" int2 NULL,
	"Birthdate" date NULL,
	"SSN" varchar(100) NULL,
	"Address" varchar(100) NULL,
	"Address2" varchar(100) NULL,
	"City" varchar(100) NULL,
	"State" varchar(100) NULL,
	"Zip" varchar(100) NULL,
	"HmPhone" varchar(30) NULL,
	"WkPhone" varchar(30) NULL,
	"WirelessPhone" varchar(30) NULL,
	"Guarantor" int8 NULL,
	"CreditType" bpchar(1) NULL,
	"Email" varchar(100) NULL,
	"Salutation" varchar(100) NULL,
	"EstBalance" float8 NULL,
	"PriProv" int8 NULL,
	"SecProv" int8 NULL,
	"FeeSched" int8 NULL,
	"BillingType" int8 NULL,
	"ImageFolder" varchar(100) NULL,
	"AddrNote" text NULL,
	"FamFinUrgNote" text NULL,
	"MedUrgNote" varchar(255) NULL,
	"ApptModNote" varchar(255) NULL,
	"StudentStatus" bpchar(1) NULL,
	"SchoolName" varchar(255) NULL,
	"ChartNumber" varchar(100) NULL,
	"MedicaidID" varchar(20) NULL,
	"Bal_0_30" float8 NULL,
	"Bal_31_60" float8 NULL,
	"Bal_61_90" float8 NULL,
	"BalOver90" float8 NULL,
	"InsEst" float8 NULL,
	"BalTotal" float8 NULL,
	"EmployerNum" int8 NULL,
	"EmploymentNote" varchar(255) NULL,
	"County" varchar(255) NULL,
	"GradeLevel" bool NULL,
	"Urgency" bool NULL,
	"DateFirstVisit" date NULL,
	"ClinicNum" int8 NULL,
	"HasIns" varchar(255) NULL,
	"TrophyFolder" varchar(255) NULL,
	"PlannedIsDone" bool NULL,
	"Premed" bool NULL,
	"Ward" varchar(255) NULL,
	"PreferConfirmMethod" int2 NULL,
	"PreferContactMethod" int2 NULL,
	"PreferRecallMethod" int2 NULL,
	"SchedBeforeTime" time NULL,
	"SchedAfterTime" time NULL,
	"SchedDayOfWeek" bool NULL,
	"Language" varchar(100) NULL,
	"AdmitDate" date NULL,
	"Title" varchar(15) NULL,
	"PayPlanDue" float8 NULL,
	"SiteNum" int8 NULL,
	"DateTStamp" timestamp NULL,
	"ResponsParty" int8 NULL,
	"CanadianEligibilityCode" bool NULL,
	"AskToArriveEarly" int4 NULL,
	"PreferContactConfidential" bool NULL,
	"SuperFamily" int8 NULL,
	"TxtMsgOk" int2 NULL,
	"SmokingSnoMed" varchar(32) NULL,
	"Country" varchar(255) NULL,
	"DateTimeDeceased" timestamp NULL,
	"BillingCycleDay" int4 NULL,
	"SecUserNumEntry" int8 NULL,
	"SecDateEntry" date NULL,
	"HasSuperBilling" bool NULL,
	"PatNumCloneFrom" int8 NULL,
	"DiscountPlanNum" int8 NULL,
	"HasSignedTil" bool NULL,
	"ShortCodeOptIn" int2 NULL,
	"SecurityHash" varchar(255) NULL,
	CONSTRAINT patient_pkey PRIMARY KEY ("PatNum")
);


-- raw.patientlink definition

-- Drop table

-- DROP TABLE raw.patientlink;

CREATE TABLE raw.patientlink (
	"PatientLinkNum" int8 NOT NULL,
	"PatNumFrom" int8 NULL,
	"PatNumTo" int8 NULL,
	"LinkType" bool NULL,
	"DateTimeLink" timestamp NULL,
	CONSTRAINT patientlink_pkey PRIMARY KEY ("PatientLinkNum")
);


-- raw.patientnote definition

-- Drop table

-- DROP TABLE raw.patientnote;

CREATE TABLE raw.patientnote (
	"PatNum" int8 NOT NULL,
	"FamFinancial" text NULL,
	"ApptPhone" text NULL,
	"Medical" text NULL,
	"Service" text NULL,
	"MedicalComp" text NULL,
	"Treatment" text NULL,
	"ICEName" varchar(255) NULL,
	"ICEPhone" varchar(30) NULL,
	"OrthoMonthsTreatOverride" int4 NULL,
	"DateOrthoPlacementOverride" date NULL,
	"SecDateTEntry" timestamp NULL,
	"SecDateTEdit" timestamp NULL,
	"Consent" bool NULL,
	"UserNumOrthoLocked" int8 NULL,
	"Pronoun" int2 NULL,
	CONSTRAINT patientnote_pkey PRIMARY KEY ("PatNum")
);


-- raw.patientportalinvite definition

-- Drop table

-- DROP TABLE raw.patientportalinvite;

CREATE TABLE raw.patientportalinvite (
	"PatientPortalInviteNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	"ApptNum" int8 NULL,
	"ClinicNum" int8 NULL,
	"DateTimeEntry" timestamp NULL,
	"TSPrior" int8 NULL,
	"SendStatus" bool NULL,
	"MessageFk" int8 NULL,
	"ResponseDescript" text NULL,
	"MessageType" bool NULL,
	"DateTimeSent" timestamp NULL,
	"ApptReminderRuleNum" int8 NULL,
	"ApptDateTime" timestamp NULL,
	CONSTRAINT patientportalinvite_pkey PRIMARY KEY ("PatientPortalInviteNum")
);


-- raw.patientrace definition

-- Drop table

-- DROP TABLE raw.patientrace;

CREATE TABLE raw.patientrace (
	"PatientRaceNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	"Race" bool NULL,
	"CdcrecCode" varchar(255) NULL,
	CONSTRAINT patientrace_pkey PRIMARY KEY ("PatientRaceNum")
);


-- raw.patplan definition

-- Drop table

-- DROP TABLE raw.patplan;

CREATE TABLE raw.patplan (
	"PatPlanNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	"Ordinal" int2 NULL,
	"IsPending" bool NULL,
	"Relationship" int2 NULL,
	"PatID" varchar(100) NULL,
	"InsSubNum" int8 NULL,
	"OrthoAutoFeeBilledOverride" float8 NULL,
	"OrthoAutoNextClaimDate" date NULL,
	"SecDateTEntry" timestamp NULL,
	"SecDateTEdit" timestamp NULL,
	CONSTRAINT patplan_pkey PRIMARY KEY ("PatPlanNum")
);


-- raw.patrestriction definition

-- Drop table

-- DROP TABLE raw.patrestriction;

CREATE TABLE raw.patrestriction (
	"PatRestrictionNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	"PatRestrictType" bool NULL,
	CONSTRAINT patrestriction_pkey PRIMARY KEY ("PatRestrictionNum")
);


-- raw.payconnectresponseweb definition

-- Drop table

-- DROP TABLE raw.payconnectresponseweb;

CREATE TABLE raw.payconnectresponseweb (
	"PayConnectResponseWebNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	"PayNum" int8 NULL,
	"AccountToken" varchar(255) NULL,
	"PaymentToken" varchar(255) NULL,
	"ProcessingStatus" varchar(255) NULL,
	"DateTimeEntry" timestamp NULL,
	"DateTimePending" timestamp NULL,
	"DateTimeCompleted" timestamp NULL,
	"DateTimeExpired" timestamp NULL,
	"DateTimeLastError" timestamp NULL,
	"LastResponseStr" text NULL,
	"CCSource" bool NULL,
	"Amount" float8 NULL,
	"PayNote" varchar(255) NULL,
	"IsTokenSaved" bool NULL,
	"PayToken" varchar(255) NULL,
	"ExpDateToken" varchar(255) NULL,
	"RefNumber" varchar(255) NULL,
	"TransType" varchar(255) NULL,
	"EmailResponse" varchar(255) NULL,
	"LogGuid" varchar(36) NULL,
	CONSTRAINT payconnectresponseweb_pkey PRIMARY KEY ("PayConnectResponseWebNum")
);


-- raw.payment definition

-- Drop table

-- DROP TABLE raw.payment;

CREATE TABLE raw.payment (
	"PayNum" int8 NOT NULL,
	"PayType" int8 NULL,
	"PayDate" date NULL,
	"PayAmt" float8 NULL,
	"CheckNum" varchar(25) NULL,
	"BankBranch" varchar(25) NULL,
	"PayNote" text NULL,
	"IsSplit" bool NULL,
	"PatNum" int8 NULL,
	"ClinicNum" int8 NULL,
	"DateEntry" date NULL,
	"DepositNum" int8 NULL,
	"Receipt" text NULL,
	"IsRecurringCC" bool NULL,
	"SecUserNumEntry" int8 NULL,
	"SecDateTEdit" timestamp NULL,
	"PaymentSource" bool NULL,
	"ProcessStatus" bool NULL,
	"RecurringChargeDate" date NULL,
	"ExternalId" varchar(255) NULL,
	"PaymentStatus" bool NULL,
	"IsCcCompleted" bool NULL,
	"MerchantFee" float8 NULL,
	CONSTRAINT payment_pkey PRIMARY KEY ("PayNum")
);


-- raw.payperiod definition

-- Drop table

-- DROP TABLE raw.payperiod;

CREATE TABLE raw.payperiod (
	"PayPeriodNum" int8 NOT NULL,
	"DateStart" date NULL,
	"DateStop" date NULL,
	"DatePaycheck" date NULL,
	CONSTRAINT payperiod_pkey PRIMARY KEY ("PayPeriodNum")
);


-- raw.payplan definition

-- Drop table

-- DROP TABLE raw.payplan;

CREATE TABLE raw.payplan (
	"PayPlanNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	"Guarantor" int8 NULL,
	"PayPlanDate" date NULL,
	"APR" float8 NULL,
	"Note" text NULL,
	"PlanNum" int8 NULL,
	"CompletedAmt" float8 NULL,
	"InsSubNum" int8 NULL,
	"PaySchedule" bool NULL,
	"NumberOfPayments" int4 NULL,
	"PayAmt" float8 NULL,
	"DownPayment" float8 NULL,
	"IsClosed" bool NULL,
	"Signature" text NULL,
	"SigIsTopaz" bool NULL,
	"PlanCategory" int8 NULL,
	"IsDynamic" bool NULL,
	"ChargeFrequency" bool NULL,
	"DatePayPlanStart" date NULL,
	"IsLocked" bool NULL,
	"DateInterestStart" date NULL,
	"DynamicPayPlanTPOption" bool NULL,
	"MobileAppDeviceNum" int8 NULL,
	"SecurityHash" varchar(255) NULL,
	CONSTRAINT payplan_pkey PRIMARY KEY ("PayPlanNum")
);


-- raw.payplancharge definition

-- Drop table

-- DROP TABLE raw.payplancharge;

CREATE TABLE raw.payplancharge (
	"PayPlanChargeNum" int8 NOT NULL,
	"PayPlanNum" int8 NULL,
	"Guarantor" int8 NULL,
	"PatNum" int8 NULL,
	"ChargeDate" date NULL,
	"Principal" float8 NULL,
	"Interest" float8 NULL,
	"Note" text NULL,
	"ProvNum" int8 NULL,
	"ClinicNum" int8 NULL,
	"ChargeType" bool NULL,
	"ProcNum" int8 NULL,
	"SecDateTEntry" timestamp NULL,
	"SecDateTEdit" timestamp NULL,
	"StatementNum" int8 NULL,
	"FKey" int8 NULL,
	"LinkType" bool NULL,
	"IsOffset" bool NULL,
	CONSTRAINT payplancharge_pkey PRIMARY KEY ("PayPlanChargeNum")
);


-- raw.payplanlink definition

-- Drop table

-- DROP TABLE raw.payplanlink;

CREATE TABLE raw.payplanlink (
	"PayPlanLinkNum" int8 NOT NULL,
	"PayPlanNum" int8 NULL,
	"LinkType" bool NULL,
	"FKey" int8 NULL,
	"AmountOverride" float8 NULL,
	"SecDateTEntry" timestamp NULL,
	CONSTRAINT payplanlink_pkey PRIMARY KEY ("PayPlanLinkNum")
);


-- raw.payplantemplate definition

-- Drop table

-- DROP TABLE raw.payplantemplate;

CREATE TABLE raw.payplantemplate (
	"PayPlanTemplateNum" int8 NOT NULL,
	"PayPlanTemplateName" varchar(255) NULL,
	"ClinicNum" int8 NULL,
	"APR" float8 NULL,
	"InterestDelay" int4 NULL,
	"PayAmt" float8 NULL,
	"NumberOfPayments" int4 NULL,
	"ChargeFrequency" bool NULL,
	"DownPayment" float8 NULL,
	"DynamicPayPlanTPOption" bool NULL,
	"Note" varchar(255) NULL,
	"IsHidden" bool NULL,
	CONSTRAINT payplantemplate_pkey PRIMARY KEY ("PayPlanTemplateNum")
);


-- raw.paysplit definition

-- Drop table

-- DROP TABLE raw.paysplit;

CREATE TABLE raw.paysplit (
	"SplitNum" int8 NOT NULL,
	"SplitAmt" float8 NULL,
	"PatNum" int8 NULL,
	"ProcDate" date NULL,
	"PayNum" int8 NULL,
	"IsDiscount" bool NULL,
	"DiscountType" bool NULL,
	"ProvNum" int8 NULL,
	"PayPlanNum" int8 NULL,
	"DatePay" date NULL,
	"ProcNum" int8 NULL,
	"DateEntry" date NULL,
	"UnearnedType" int8 NULL,
	"ClinicNum" int8 NULL,
	"SecUserNumEntry" int8 NULL,
	"SecDateTEdit" timestamp NULL,
	"FSplitNum" int8 NULL,
	"AdjNum" int8 NULL,
	"PayPlanChargeNum" int8 NULL,
	"PayPlanDebitType" bool NULL,
	"SecurityHash" varchar(255) NULL,
	CONSTRAINT paysplit_pkey PRIMARY KEY ("SplitNum")
);


-- raw.payterminal definition

-- Drop table

-- DROP TABLE raw.payterminal;

CREATE TABLE raw.payterminal (
	"PayTerminalNum" int8 NOT NULL,
	"Name" varchar(255) NULL,
	"ClinicNum" int8 NULL,
	"TerminalID" varchar(255) NULL,
	CONSTRAINT payterminal_pkey PRIMARY KEY ("PayTerminalNum")
);


-- raw.pearlrequest definition

-- Drop table

-- DROP TABLE raw.pearlrequest;

CREATE TABLE raw.pearlrequest (
	"PearlRequestNum" int8 NOT NULL,
	"RequestId" varchar(255) NULL,
	"DocNum" int8 NULL,
	"RequestStatus" bool NULL,
	"DateTSent" date NULL,
	"DateTChecked" date NULL,
	CONSTRAINT pearlrequest_pkey PRIMARY KEY ("PearlRequestNum")
);


-- raw.perioexam definition

-- Drop table

-- DROP TABLE raw.perioexam;

CREATE TABLE raw.perioexam (
	"PerioExamNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	"ExamDate" date NULL,
	"ProvNum" int8 NULL,
	"DateTMeasureEdit" timestamp NULL,
	"Note" text NULL,
	CONSTRAINT perioexam_pkey PRIMARY KEY ("PerioExamNum")
);


-- raw.periomeasure definition

-- Drop table

-- DROP TABLE raw.periomeasure;

CREATE TABLE raw.periomeasure (
	"PerioMeasureNum" int8 NOT NULL,
	"PerioExamNum" int8 NULL,
	"SequenceType" int2 NULL,
	"IntTooth" int2 NULL,
	"ToothValue" int2 NULL,
	"MBvalue" int2 NULL,
	"Bvalue" int2 NULL,
	"DBvalue" int2 NULL,
	"MLvalue" int2 NULL,
	"Lvalue" int2 NULL,
	"DLvalue" int2 NULL,
	"SecDateTEntry" timestamp NULL,
	"SecDateTEdit" timestamp NULL,
	CONSTRAINT periomeasure_pkey PRIMARY KEY ("PerioMeasureNum")
);


-- raw.pharmacy definition

-- Drop table

-- DROP TABLE raw.pharmacy;

CREATE TABLE raw.pharmacy (
	"PharmacyNum" int8 NOT NULL,
	"PharmID" varchar(255) NULL,
	"StoreName" varchar(255) NULL,
	"Phone" varchar(255) NULL,
	"Fax" varchar(255) NULL,
	"Address" varchar(255) NULL,
	"Address2" varchar(255) NULL,
	"City" varchar(255) NULL,
	"State" varchar(255) NULL,
	"Zip" varchar(255) NULL,
	"Note" text NULL,
	"DateTStamp" timestamp NULL,
	CONSTRAINT pharmacy_pkey PRIMARY KEY ("PharmacyNum")
);


-- raw.pharmclinic definition

-- Drop table

-- DROP TABLE raw.pharmclinic;

CREATE TABLE raw.pharmclinic (
	"PharmClinicNum" int8 NOT NULL,
	"PharmacyNum" int8 NULL,
	"ClinicNum" int8 NULL,
	CONSTRAINT pharmclinic_pkey PRIMARY KEY ("PharmClinicNum")
);


-- raw.phonenumber definition

-- Drop table

-- DROP TABLE raw.phonenumber;

CREATE TABLE raw.phonenumber (
	"PhoneNumberNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	"PhoneNumberVal" varchar(255) NULL,
	"PhoneNumberDigits" varchar(30) NULL,
	"PhoneType" bool NULL,
	CONSTRAINT phonenumber_pkey PRIMARY KEY ("PhoneNumberNum")
);


-- raw.popup definition

-- Drop table

-- DROP TABLE raw.popup;

CREATE TABLE raw.popup (
	"PopupNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	"Description" text NULL,
	"IsDisabled" bool NULL,
	"PopupLevel" bool NULL,
	"UserNum" int8 NULL,
	"DateTimeEntry" timestamp NULL,
	"IsArchived" bool NULL,
	"PopupNumArchive" int8 NULL,
	"DateTimeDisabled" timestamp NULL,
	CONSTRAINT popup_pkey PRIMARY KEY ("PopupNum")
);


-- raw.preference definition

-- Drop table

-- DROP TABLE raw.preference;

CREATE TABLE raw.preference (
	"PrefName" varchar(255) NULL,
	"ValueString" text NULL,
	"PrefNum" int8 NOT NULL,
	"Comments" text NULL,
	CONSTRAINT preference_pkey PRIMARY KEY ("PrefNum")
);


-- raw.printer definition

-- Drop table

-- DROP TABLE raw.printer;

CREATE TABLE raw.printer (
	"PrinterNum" int8 NOT NULL,
	"ComputerNum" int8 NULL,
	"PrintSit" int2 NULL,
	"PrinterName" varchar(255) NULL,
	"DisplayPrompt" bool NULL,
	"FileExtension" varchar(255) NULL,
	"IsVirtualPrinter" bool NULL,
	CONSTRAINT printer_pkey PRIMARY KEY ("PrinterNum")
);


-- raw.procapptcolor definition

-- Drop table

-- DROP TABLE raw.procapptcolor;

CREATE TABLE raw.procapptcolor (
	"ProcApptColorNum" int8 NOT NULL,
	"CodeRange" varchar(255) NULL,
	"ColorText" int4 NULL,
	"ShowPreviousDate" bool NULL,
	CONSTRAINT procapptcolor_pkey PRIMARY KEY ("ProcApptColorNum")
);


-- raw.procbutton definition

-- Drop table

-- DROP TABLE raw.procbutton;

CREATE TABLE raw.procbutton (
	"ProcButtonNum" int8 NOT NULL,
	"Description" varchar(255) NULL,
	"ItemOrder" int2 NULL,
	"Category" int8 NULL,
	"ButtonImage" text NULL,
	"IsMultiVisit" bool NULL,
	CONSTRAINT procbutton_pkey PRIMARY KEY ("ProcButtonNum")
);


-- raw.procbuttonitem definition

-- Drop table

-- DROP TABLE raw.procbuttonitem;

CREATE TABLE raw.procbuttonitem (
	"ProcButtonItemNum" int8 NOT NULL,
	"ProcButtonNum" int8 NULL,
	"OldCode" varchar(15) NULL,
	"AutoCodeNum" int8 NULL,
	"CodeNum" int8 NULL,
	"ItemOrder" int8 NULL,
	CONSTRAINT procbuttonitem_pkey PRIMARY KEY ("ProcButtonItemNum")
);


-- raw.procbuttonquick definition

-- Drop table

-- DROP TABLE raw.procbuttonquick;

CREATE TABLE raw.procbuttonquick (
	"ProcButtonQuickNum" int8 NOT NULL,
	"Description" varchar(255) NULL,
	"CodeValue" varchar(255) NULL,
	"Surf" varchar(255) NULL,
	"YPos" int4 NULL,
	"ItemOrder" int4 NULL,
	"IsLabel" bool NULL,
	CONSTRAINT procbuttonquick_pkey PRIMARY KEY ("ProcButtonQuickNum")
);


-- raw.proccodenote definition

-- Drop table

-- DROP TABLE raw.proccodenote;

CREATE TABLE raw.proccodenote (
	"ProcCodeNoteNum" int8 NOT NULL,
	"CodeNum" int8 NULL,
	"ProvNum" int8 NULL,
	"Note" text NULL,
	"ProcTime" varchar(255) NULL,
	"ProcStatus" bool NULL,
	CONSTRAINT proccodenote_pkey PRIMARY KEY ("ProcCodeNoteNum")
);


-- raw.procedurecode definition

-- Drop table

-- DROP TABLE raw.procedurecode;

CREATE TABLE raw.procedurecode (
	"CodeNum" int8 NOT NULL,
	"ProcCode" varchar(15) NULL,
	"Descript" varchar(255) NULL,
	"AbbrDesc" varchar(50) NULL,
	"ProcTime" varchar(24) NULL,
	"ProcCat" int8 NULL,
	"TreatArea" int2 NULL,
	"NoBillIns" bool NULL,
	"IsProsth" bool NULL,
	"DefaultNote" text NULL,
	"IsHygiene" bool NULL,
	"GTypeNum" int2 NULL,
	"AlternateCode1" varchar(15) NULL,
	"MedicalCode" varchar(15) NULL,
	"IsTaxed" bool NULL,
	"PaintType" int2 NULL,
	"GraphicColor" int4 NULL,
	"LaymanTerm" varchar(255) NULL,
	"IsCanadianLab" bool NULL,
	"PreExisting" bool NULL,
	"BaseUnits" int4 NULL,
	"SubstitutionCode" varchar(25) NULL,
	"SubstOnlyIf" int4 NULL,
	"DateTStamp" timestamp NULL,
	"IsMultiVisit" bool NULL,
	"DrugNDC" varchar(255) NULL,
	"RevenueCodeDefault" varchar(255) NULL,
	"ProvNumDefault" int8 NULL,
	"CanadaTimeUnits" float8 NULL,
	"IsRadiology" bool NULL,
	"DefaultClaimNote" text NULL,
	"DefaultTPNote" text NULL,
	"BypassGlobalLock" bool NULL,
	"TaxCode" varchar(16) NULL,
	"PaintText" varchar(255) NULL,
	"AreaAlsoToothRange" bool NULL,
	"DiagnosticCodes" varchar(255) NULL,
	CONSTRAINT procedurecode_pkey PRIMARY KEY ("CodeNum")
);


-- raw.procedurelog definition

-- Drop table

-- DROP TABLE raw.procedurelog;

CREATE TABLE raw.procedurelog (
	"ProcNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	"AptNum" int8 NULL,
	"OldCode" varchar(15) NULL,
	"ProcDate" date NULL,
	"ProcFee" float8 NULL,
	"Surf" varchar(10) NULL,
	"ToothNum" varchar(2) NULL,
	"ToothRange" varchar(100) NULL,
	"Priority" int8 NULL,
	"ProcStatus" int2 NULL,
	"ProvNum" int8 NULL,
	"Dx" int8 NULL,
	"PlannedAptNum" int8 NULL,
	"PlaceService" int2 NULL,
	"Prosthesis" bpchar(1) NULL,
	"DateOriginalProsth" date NULL,
	"ClaimNote" varchar(80) NULL,
	"DateEntryC" date NULL,
	"ClinicNum" int8 NULL,
	"MedicalCode" varchar(15) NULL,
	"DiagnosticCode" varchar(255) NULL,
	"IsPrincDiag" bool NULL,
	"ProcNumLab" int8 NULL,
	"BillingTypeOne" int8 NULL,
	"BillingTypeTwo" int8 NULL,
	"CodeNum" int8 NULL,
	"CodeMod1" bpchar(2) NULL,
	"CodeMod2" bpchar(2) NULL,
	"CodeMod3" bpchar(2) NULL,
	"CodeMod4" bpchar(2) NULL,
	"RevCode" varchar(45) NULL,
	"UnitQty" int4 NULL,
	"BaseUnits" int4 NULL,
	"StartTime" int4 NULL,
	"StopTime" int4 NULL,
	"DateTP" date NULL,
	"SiteNum" int8 NULL,
	"HideGraphics" bool NULL,
	"CanadianTypeCodes" varchar(20) NULL,
	"ProcTime" time NULL,
	"ProcTimeEnd" time NULL,
	"DateTStamp" timestamp NULL,
	"Prognosis" int8 NULL,
	"DrugUnit" bool NULL,
	"DrugQty" float4 NULL,
	"UnitQtyType" bool NULL,
	"StatementNum" int8 NULL,
	"IsLocked" bool NULL,
	"BillingNote" varchar(255) NULL,
	"RepeatChargeNum" int8 NULL,
	"SnomedBodySite" varchar(255) NULL,
	"DiagnosticCode2" varchar(255) NULL,
	"DiagnosticCode3" varchar(255) NULL,
	"DiagnosticCode4" varchar(255) NULL,
	"ProvOrderOverride" int8 NULL,
	"Discount" float8 NULL,
	"IsDateProsthEst" bool NULL,
	"IcdVersion" int2 NULL,
	"IsCpoe" bool NULL,
	"SecUserNumEntry" int8 NULL,
	"SecDateEntry" timestamp NULL,
	"DateComplete" date NULL,
	"OrderingReferralNum" int8 NULL,
	"TaxAmt" float8 NULL,
	"Urgency" bool NULL,
	"DiscountPlanAmt" float8 NULL,
	CONSTRAINT procedurelog_pkey PRIMARY KEY ("ProcNum")
);


-- raw.procgroupitem definition

-- Drop table

-- DROP TABLE raw.procgroupitem;

CREATE TABLE raw.procgroupitem (
	"ProcGroupItemNum" int8 NOT NULL,
	"ProcNum" int8 NULL,
	"GroupNum" int8 NULL,
	CONSTRAINT procgroupitem_pkey PRIMARY KEY ("ProcGroupItemNum")
);


-- raw.procmultivisit definition

-- Drop table

-- DROP TABLE raw.procmultivisit;

CREATE TABLE raw.procmultivisit (
	"ProcMultiVisitNum" int8 NOT NULL,
	"GroupProcMultiVisitNum" int8 NULL,
	"ProcNum" int8 NULL,
	"ProcStatus" int2 NULL,
	"IsInProcess" bool NULL,
	"SecDateTEntry" timestamp NULL,
	"SecDateTEdit" timestamp NULL,
	"PatNum" int8 NULL,
	CONSTRAINT procmultivisit_pkey PRIMARY KEY ("ProcMultiVisitNum")
);


-- raw.procnote definition

-- Drop table

-- DROP TABLE raw.procnote;

CREATE TABLE raw.procnote (
	"ProcNoteNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	"ProcNum" int8 NULL,
	"EntryDateTime" timestamp NULL,
	"UserNum" int8 NULL,
	"Note" text NULL,
	"SigIsTopaz" bool NULL,
	"Signature" text NULL,
	CONSTRAINT procnote_pkey PRIMARY KEY ("ProcNoteNum")
);


-- raw.proctp definition

-- Drop table

-- DROP TABLE raw.proctp;

CREATE TABLE raw.proctp (
	"ProcTPNum" int8 NOT NULL,
	"TreatPlanNum" int8 NULL,
	"PatNum" int8 NULL,
	"ProcNumOrig" int8 NULL,
	"ItemOrder" int2 NULL,
	"Priority" int8 NULL,
	"ToothNumTP" varchar(255) NULL,
	"Surf" varchar(255) NULL,
	"ProcCode" varchar(15) NULL,
	"Descript" varchar(255) NULL,
	"FeeAmt" float8 NULL,
	"PriInsAmt" float8 NULL,
	"SecInsAmt" float8 NULL,
	"PatAmt" float8 NULL,
	"Discount" float8 NULL,
	"Prognosis" varchar(255) NULL,
	"Dx" varchar(255) NULL,
	"ProcAbbr" varchar(50) NULL,
	"SecUserNumEntry" int8 NULL,
	"SecDateEntry" date NULL,
	"SecDateTEdit" timestamp NULL,
	"FeeAllowed" float8 NULL,
	"TaxAmt" float8 NULL,
	"ProvNum" int8 NULL,
	"DateTP" date NULL,
	"ClinicNum" int8 NULL,
	"CatPercUCR" float8 NULL,
	CONSTRAINT proctp_pkey PRIMARY KEY ("ProcTPNum")
);


-- raw."program" definition

-- Drop table

-- DROP TABLE raw."program";

CREATE TABLE raw."program" (
	"ProgramNum" int8 NOT NULL,
	"ProgName" varchar(100) NULL,
	"ProgDesc" varchar(100) NULL,
	"Enabled" bool NULL,
	"Path" text NULL,
	"CommandLine" text NULL,
	"Note" text NULL,
	"PluginDllName" varchar(255) NULL,
	"ButtonImage" text NULL,
	"FileTemplate" text NULL,
	"FilePath" varchar(255) NULL,
	"IsDisabledByHq" bool NULL,
	"CustErr" varchar(255) NULL,
	CONSTRAINT program_pkey PRIMARY KEY ("ProgramNum")
);


-- raw.programproperty definition

-- Drop table

-- DROP TABLE raw.programproperty;

CREATE TABLE raw.programproperty (
	"ProgramPropertyNum" int8 NOT NULL,
	"ProgramNum" int8 NULL,
	"PropertyDesc" varchar(255) NULL,
	"PropertyValue" text NULL,
	"ComputerName" varchar(255) NULL,
	"ClinicNum" int8 NULL,
	"IsMasked" bool NULL,
	"IsHighSecurity" bool NULL,
	CONSTRAINT programproperty_pkey PRIMARY KEY ("ProgramPropertyNum")
);


-- raw.promotion definition

-- Drop table

-- DROP TABLE raw.promotion;

CREATE TABLE raw.promotion (
	"PromotionNum" int8 NOT NULL,
	"PromotionName" varchar(255) NULL,
	"DateTimeCreated" date NULL,
	"ClinicNum" int8 NULL,
	"TypePromotion" bool NULL,
	CONSTRAINT promotion_pkey PRIMARY KEY ("PromotionNum")
);


-- raw.promotionlog definition

-- Drop table

-- DROP TABLE raw.promotionlog;

CREATE TABLE raw.promotionlog (
	"PromotionLogNum" int8 NOT NULL,
	"PromotionNum" int8 NULL,
	"PatNum" int8 NULL,
	"MessageFk" int8 NULL,
	"EmailHostingFK" int8 NULL,
	"DateTimeSent" timestamp NULL,
	"PromotionStatus" bool NULL,
	"ClinicNum" int8 NULL,
	"SendStatus" bool NULL,
	"MessageType" bool NULL,
	"DateTimeEntry" timestamp NULL,
	"ResponseDescript" text NULL,
	"ApptReminderRuleNum" int8 NULL,
	CONSTRAINT promotionlog_pkey PRIMARY KEY ("PromotionLogNum")
);


-- raw.provider definition

-- Drop table

-- DROP TABLE raw.provider;

CREATE TABLE raw.provider (
	"ProvNum" int8 NOT NULL,
	"Abbr" varchar(255) NULL,
	"ItemOrder" int2 NULL,
	"LName" varchar(100) NULL,
	"FName" varchar(100) NULL,
	"MI" varchar(100) NULL,
	"Suffix" varchar(100) NULL,
	"FeeSched" int8 NULL,
	"Specialty" int8 NULL,
	"SSN" varchar(12) NULL,
	"StateLicense" varchar(15) NULL,
	"DEANum" varchar(15) NULL,
	"IsSecondary" bool NULL,
	"ProvColor" int4 NULL,
	"IsHidden" bool NULL,
	"UsingTIN" bool NULL,
	"BlueCrossID" varchar(25) NULL,
	"SigOnFile" bool NULL,
	"MedicaidID" varchar(20) NULL,
	"OutlineColor" int4 NULL,
	"SchoolClassNum" int8 NULL,
	"NationalProvID" varchar(255) NULL,
	"CanadianOfficeNum" varchar(100) NULL,
	"DateTStamp" timestamp NULL,
	"AnesthProvType" int2 NULL,
	"TaxonomyCodeOverride" varchar(255) NULL,
	"IsCDAnet" bool NULL,
	"EcwID" varchar(255) NULL,
	"StateRxID" varchar(255) NULL,
	"IsNotPerson" bool NULL,
	"StateWhereLicensed" varchar(50) NULL,
	"EmailAddressNum" int8 NULL,
	"IsInstructor" bool NULL,
	"EhrMuStage" int2 NULL,
	"ProvNumBillingOverride" int8 NULL,
	"CustomID" varchar(255) NULL,
	"ProvStatus" int2 NULL,
	"IsHiddenReport" bool NULL,
	"IsErxEnabled" bool NULL,
	"Birthdate" date NULL,
	"SchedNote" varchar(255) NULL,
	"WebSchedDescript" varchar(500) NULL,
	"WebSchedImageLocation" varchar(255) NULL,
	"HourlyProdGoalAmt" float8 NULL,
	"DateTerm" date NULL,
	"PreferredName" varchar(100) NULL,
	CONSTRAINT provider_pkey PRIMARY KEY ("ProvNum")
);


-- raw.providerclinic definition

-- Drop table

-- DROP TABLE raw.providerclinic;

CREATE TABLE raw.providerclinic (
	"ProviderClinicNum" int8 NOT NULL,
	"ProvNum" int8 NULL,
	"ClinicNum" int8 NULL,
	"DEANum" varchar(15) NULL,
	"StateLicense" varchar(50) NULL,
	"StateRxID" varchar(255) NULL,
	"StateWhereLicensed" varchar(15) NULL,
	"CareCreditMerchantId" varchar(20) NULL,
	CONSTRAINT providerclinic_pkey PRIMARY KEY ("ProviderClinicNum")
);


-- raw.providercliniclink definition

-- Drop table

-- DROP TABLE raw.providercliniclink;

CREATE TABLE raw.providercliniclink (
	"ProviderClinicLinkNum" int8 NOT NULL,
	"ProvNum" int8 NULL,
	"ClinicNum" int8 NULL,
	CONSTRAINT providercliniclink_pkey PRIMARY KEY ("ProviderClinicLinkNum")
);


-- raw.providererx definition

-- Drop table

-- DROP TABLE raw.providererx;

CREATE TABLE raw.providererx (
	"ProviderErxNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	"NationalProviderID" varchar(255) NULL,
	"IsEnabled" bool NULL,
	"IsIdentifyProofed" bool NULL,
	"IsSentToHq" bool NULL,
	"IsEpcs" bool NULL,
	"ErxType" bool NULL,
	"UserId" varchar(255) NULL,
	"AccountId" varchar(25) NULL,
	"RegistrationKeyNum" int8 NULL,
	CONSTRAINT providererx_pkey PRIMARY KEY ("ProviderErxNum")
);


-- raw.providerident definition

-- Drop table

-- DROP TABLE raw.providerident;

CREATE TABLE raw.providerident (
	"ProviderIdentNum" int8 NOT NULL,
	"ProvNum" int8 NULL,
	"PayorID" varchar(255) NULL,
	"SuppIDType" bool NULL,
	"IDNumber" varchar(255) NULL,
	CONSTRAINT providerident_pkey PRIMARY KEY ("ProviderIdentNum")
);


-- raw.question definition

-- Drop table

-- DROP TABLE raw.question;

CREATE TABLE raw.question (
	"QuestionNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	"ItemOrder" int2 NULL,
	"Description" text NULL,
	"Answer" text NULL,
	"FormPatNum" int8 NULL,
	CONSTRAINT question_pkey PRIMARY KEY ("QuestionNum")
);


-- raw.quickpastecat definition

-- Drop table

-- DROP TABLE raw.quickpastecat;

CREATE TABLE raw.quickpastecat (
	"QuickPasteCatNum" int8 NOT NULL,
	"Description" varchar(255) NULL,
	"ItemOrder" int2 NULL,
	"DefaultForTypes" text NULL,
	CONSTRAINT quickpastecat_pkey PRIMARY KEY ("QuickPasteCatNum")
);


-- raw.quickpastenote definition

-- Drop table

-- DROP TABLE raw.quickpastenote;

CREATE TABLE raw.quickpastenote (
	"QuickPasteNoteNum" int8 NOT NULL,
	"QuickPasteCatNum" int8 NULL,
	"ItemOrder" int2 NULL,
	"Note" text NULL,
	"Abbreviation" varchar(255) NULL,
	CONSTRAINT quickpastenote_pkey PRIMARY KEY ("QuickPasteNoteNum")
);


-- raw.reactivation definition

-- Drop table

-- DROP TABLE raw.reactivation;

CREATE TABLE raw.reactivation (
	"ReactivationNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	"ReactivationStatus" int8 NULL,
	"ReactivationNote" text NULL,
	"DoNotContact" bool NULL,
	CONSTRAINT reactivation_pkey PRIMARY KEY ("ReactivationNum")
);


-- raw.recall definition

-- Drop table

-- DROP TABLE raw.recall;

CREATE TABLE raw.recall (
	"RecallNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	"DateDueCalc" date NULL,
	"DateDue" date NULL,
	"DatePrevious" date NULL,
	"RecallInterval" int4 NULL,
	"RecallStatus" int8 NULL,
	"Note" text NULL,
	"IsDisabled" bool NULL,
	"DateTStamp" timestamp NULL,
	"RecallTypeNum" int8 NULL,
	"DisableUntilBalance" float8 NULL,
	"DisableUntilDate" date NULL,
	"DateScheduled" date NULL,
	"Priority" bool NULL,
	"TimePatternOverride" varchar(255) NULL,
	CONSTRAINT recall_pkey PRIMARY KEY ("RecallNum")
);


-- raw.recalltrigger definition

-- Drop table

-- DROP TABLE raw.recalltrigger;

CREATE TABLE raw.recalltrigger (
	"RecallTriggerNum" int8 NOT NULL,
	"RecallTypeNum" int8 NULL,
	"CodeNum" int8 NULL,
	CONSTRAINT recalltrigger_pkey PRIMARY KEY ("RecallTriggerNum")
);


-- raw.recalltype definition

-- Drop table

-- DROP TABLE raw.recalltype;

CREATE TABLE raw.recalltype (
	"RecallTypeNum" int8 NOT NULL,
	"Description" varchar(255) NULL,
	"DefaultInterval" int4 NULL,
	"TimePattern" varchar(255) NULL,
	"Procedures" varchar(255) NULL,
	"AppendToSpecial" bool NULL,
	CONSTRAINT recalltype_pkey PRIMARY KEY ("RecallTypeNum")
);


-- raw.reconcile definition

-- Drop table

-- DROP TABLE raw.reconcile;

CREATE TABLE raw.reconcile (
	"ReconcileNum" int8 NOT NULL,
	"AccountNum" int8 NULL,
	"StartingBal" float8 NULL,
	"EndingBal" float8 NULL,
	"DateReconcile" date NULL,
	"IsLocked" bool NULL,
	CONSTRAINT reconcile_pkey PRIMARY KEY ("ReconcileNum")
);


-- raw.recurringcharge definition

-- Drop table

-- DROP TABLE raw.recurringcharge;

CREATE TABLE raw.recurringcharge (
	"RecurringChargeNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	"ClinicNum" int8 NULL,
	"DateTimeCharge" timestamp NULL,
	"ChargeStatus" bool NULL,
	"FamBal" float8 NULL,
	"PayPlanDue" float8 NULL,
	"TotalDue" float8 NULL,
	"RepeatAmt" float8 NULL,
	"ChargeAmt" float8 NULL,
	"UserNum" int8 NULL,
	"PayNum" int8 NULL,
	"CreditCardNum" int8 NULL,
	"ErrorMsg" text NULL,
	CONSTRAINT recurringcharge_pkey PRIMARY KEY ("RecurringChargeNum")
);


-- raw.refattach definition

-- Drop table

-- DROP TABLE raw.refattach;

CREATE TABLE raw.refattach (
	"RefAttachNum" int8 NOT NULL,
	"ReferralNum" int8 NULL,
	"PatNum" int8 NULL,
	"ItemOrder" int2 NULL,
	"RefDate" date NULL,
	"RefType" bool NULL,
	"RefToStatus" int2 NULL,
	"Note" text NULL,
	"IsTransitionOfCare" bool NULL,
	"ProcNum" int8 NULL,
	"DateProcComplete" date NULL,
	"ProvNum" int8 NULL,
	"DateTStamp" timestamp NULL,
	CONSTRAINT refattach_pkey PRIMARY KEY ("RefAttachNum")
);


-- raw.referral definition

-- Drop table

-- DROP TABLE raw.referral;

CREATE TABLE raw.referral (
	"ReferralNum" int8 NOT NULL,
	"LName" varchar(100) NULL,
	"FName" varchar(100) NULL,
	"MName" varchar(100) NULL,
	"SSN" varchar(9) NULL,
	"UsingTIN" bool NULL,
	"Specialty" int8 NULL,
	"ST" varchar(2) NULL,
	"Telephone" varchar(10) NULL,
	"Address" varchar(100) NULL,
	"Address2" varchar(100) NULL,
	"City" varchar(100) NULL,
	"Zip" varchar(10) NULL,
	"Note" text NULL,
	"Phone2" varchar(30) NULL,
	"IsHidden" bool NULL,
	"NotPerson" bool NULL,
	"Title" varchar(255) NULL,
	"EMail" varchar(255) NULL,
	"PatNum" int8 NULL,
	"NationalProvID" varchar(255) NULL,
	"Slip" int8 NULL,
	"IsDoctor" bool NULL,
	"IsTrustedDirect" bool NULL,
	"DateTStamp" timestamp NULL,
	"IsPreferred" bool NULL,
	"BusinessName" varchar(255) NULL,
	"DisplayNote" varchar(4000) NULL,
	CONSTRAINT referral_pkey PRIMARY KEY ("ReferralNum")
);


-- raw.referralcliniclink definition

-- Drop table

-- DROP TABLE raw.referralcliniclink;

CREATE TABLE raw.referralcliniclink (
	"ReferralClinicLinkNum" int8 NOT NULL,
	"ReferralNum" int8 NULL,
	"ClinicNum" int8 NULL,
	CONSTRAINT referralcliniclink_pkey PRIMARY KEY ("ReferralClinicLinkNum")
);


-- raw.registrationkey definition

-- Drop table

-- DROP TABLE raw.registrationkey;

CREATE TABLE raw.registrationkey (
	"RegistrationKeyNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	"RegKey" varchar(4000) NULL,
	"Note" varchar(4000) NULL,
	"DateStarted" date NULL,
	"DateDisabled" date NULL,
	"DateEnded" date NULL,
	"IsForeign" bool NULL,
	"UsesServerVersion" bool NULL,
	"IsFreeVersion" bool NULL,
	"IsOnlyForTesting" bool NULL,
	"VotesAllotted" int4 NULL,
	"IsResellerCustomer" bool NULL,
	"HasEarlyAccess" bool NULL,
	"DateTBackupScheduled" timestamp NULL,
	"BackupPassCode" varchar(32) NULL,
	CONSTRAINT registrationkey_pkey PRIMARY KEY ("RegistrationKeyNum")
);


-- raw.reminderrule definition

-- Drop table

-- DROP TABLE raw.reminderrule;

CREATE TABLE raw.reminderrule (
	"ReminderRuleNum" int8 NOT NULL,
	"ReminderCriterion" bool NULL,
	"CriterionFK" int8 NULL,
	"CriterionValue" varchar(255) NULL,
	"Message" varchar(255) NULL,
	CONSTRAINT reminderrule_pkey PRIMARY KEY ("ReminderRuleNum")
);


-- raw.repeatcharge definition

-- Drop table

-- DROP TABLE raw.repeatcharge;

CREATE TABLE raw.repeatcharge (
	"RepeatChargeNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	"ProcCode" varchar(15) NULL,
	"ChargeAmt" float8 NULL,
	"DateStart" date NULL,
	"DateStop" date NULL,
	"Note" text NULL,
	"CopyNoteToProc" bool NULL,
	"CreatesClaim" bool NULL,
	"IsEnabled" bool NULL,
	"UsePrepay" bool NULL,
	"Npi" text NULL,
	"ErxAccountId" text NULL,
	"ProviderName" text NULL,
	"ChargeAmtAlt" float8 NULL,
	"UnearnedTypes" varchar(4000) NULL,
	"Frequency" bool NULL,
	CONSTRAINT repeatcharge_pkey PRIMARY KEY ("RepeatChargeNum")
);


-- raw.replicationserver definition

-- Drop table

-- DROP TABLE raw.replicationserver;

CREATE TABLE raw.replicationserver (
	"ReplicationServerNum" int8 NOT NULL,
	"Descript" text NULL,
	"ServerId" int4 NULL,
	"RangeStart" int8 NULL,
	"RangeEnd" int8 NULL,
	"AtoZpath" varchar(255) NULL,
	"UpdateBlocked" bool NULL,
	"SlaveMonitor" varchar(255) NULL,
	CONSTRAINT replicationserver_pkey PRIMARY KEY ("ReplicationServerNum")
);


-- raw.reqneeded definition

-- Drop table

-- DROP TABLE raw.reqneeded;

CREATE TABLE raw.reqneeded (
	"ReqNeededNum" int8 NOT NULL,
	"Descript" varchar(255) NULL,
	"SchoolCourseNum" int8 NULL,
	"SchoolClassNum" int8 NULL,
	CONSTRAINT reqneeded_pkey PRIMARY KEY ("ReqNeededNum")
);


-- raw.reqstudent definition

-- Drop table

-- DROP TABLE raw.reqstudent;

CREATE TABLE raw.reqstudent (
	"ReqStudentNum" int8 NOT NULL,
	"ReqNeededNum" int8 NULL,
	"Descript" varchar(255) NULL,
	"SchoolCourseNum" int8 NULL,
	"ProvNum" int8 NULL,
	"AptNum" int8 NULL,
	"PatNum" int8 NULL,
	"InstructorNum" int8 NULL,
	"DateCompleted" date NULL,
	CONSTRAINT reqstudent_pkey PRIMARY KEY ("ReqStudentNum")
);


-- raw.requiredfield definition

-- Drop table

-- DROP TABLE raw.requiredfield;

CREATE TABLE raw.requiredfield (
	"RequiredFieldNum" int8 NOT NULL,
	"FieldType" bool NULL,
	"FieldName" varchar(50) NULL,
	CONSTRAINT requiredfield_pkey PRIMARY KEY ("RequiredFieldNum")
);


-- raw.requiredfieldcondition definition

-- Drop table

-- DROP TABLE raw.requiredfieldcondition;

CREATE TABLE raw.requiredfieldcondition (
	"RequiredFieldConditionNum" int8 NOT NULL,
	"RequiredFieldNum" int8 NULL,
	"ConditionType" varchar(50) NULL,
	"Operator" bool NULL,
	"ConditionValue" varchar(255) NULL,
	"ConditionRelationship" bool NULL,
	CONSTRAINT requiredfieldcondition_pkey PRIMARY KEY ("RequiredFieldConditionNum")
);


-- raw.reseller definition

-- Drop table

-- DROP TABLE raw.reseller;

CREATE TABLE raw.reseller (
	"ResellerNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	"UserName" varchar(255) NULL,
	"ResellerPassword" varchar(255) NULL,
	"BillingType" int8 NULL,
	"VotesAllotted" int4 NULL,
	"Note" varchar(4000) NULL,
	CONSTRAINT reseller_pkey PRIMARY KEY ("ResellerNum")
);


-- raw.resellerservice definition

-- Drop table

-- DROP TABLE raw.resellerservice;

CREATE TABLE raw.resellerservice (
	"ResellerServiceNum" int8 NOT NULL,
	"ResellerNum" int8 NULL,
	"CodeNum" int8 NULL,
	"Fee" float8 NULL,
	"HostedUrl" varchar(255) NULL,
	CONSTRAINT resellerservice_pkey PRIMARY KEY ("ResellerServiceNum")
);


-- raw.rxalert definition

-- Drop table

-- DROP TABLE raw.rxalert;

CREATE TABLE raw.rxalert (
	"RxAlertNum" int8 NOT NULL,
	"RxDefNum" int8 NULL,
	"DiseaseDefNum" int8 NULL,
	"AllergyDefNum" int8 NULL,
	"MedicationNum" int8 NULL,
	"NotificationMsg" varchar(255) NULL,
	"IsHighSignificance" bool NULL,
	CONSTRAINT rxalert_pkey PRIMARY KEY ("RxAlertNum")
);


-- raw.rxdef definition

-- Drop table

-- DROP TABLE raw.rxdef;

CREATE TABLE raw.rxdef (
	"RxDefNum" int8 NOT NULL,
	"Drug" varchar(255) NULL,
	"Sig" varchar(255) NULL,
	"Disp" varchar(255) NULL,
	"Refills" varchar(30) NULL,
	"Notes" varchar(255) NULL,
	"IsControlled" bool NULL,
	"RxCui" int8 NULL,
	"IsProcRequired" bool NULL,
	"PatientInstruction" text NULL,
	CONSTRAINT rxdef_pkey PRIMARY KEY ("RxDefNum")
);


-- raw.rxnorm definition

-- Drop table

-- DROP TABLE raw.rxnorm;

CREATE TABLE raw.rxnorm (
	"RxNormNum" int8 NOT NULL,
	"RxCui" varchar(255) NULL,
	"MmslCode" varchar(255) NULL,
	"Description" text NULL,
	CONSTRAINT rxnorm_pkey PRIMARY KEY ("RxNormNum")
);


-- raw.rxpat definition

-- Drop table

-- DROP TABLE raw.rxpat;

CREATE TABLE raw.rxpat (
	"RxNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	"RxDate" date NULL,
	"Drug" varchar(255) NULL,
	"Sig" varchar(255) NULL,
	"Disp" varchar(255) NULL,
	"Refills" varchar(30) NULL,
	"ProvNum" int8 NULL,
	"Notes" varchar(255) NULL,
	"PharmacyNum" int8 NULL,
	"IsControlled" bool NULL,
	"DateTStamp" timestamp NULL,
	"SendStatus" int2 NULL,
	"RxCui" int8 NULL,
	"DosageCode" varchar(255) NULL,
	"ErxGuid" varchar(40) NULL,
	"IsErxOld" bool NULL,
	"ErxPharmacyInfo" varchar(255) NULL,
	"IsProcRequired" bool NULL,
	"ProcNum" int8 NULL,
	"DaysOfSupply" float8 NULL,
	"PatientInstruction" text NULL,
	"ClinicNum" int8 NULL,
	"UserNum" int8 NULL,
	"RxType" bool NULL,
	CONSTRAINT rxpat_pkey PRIMARY KEY ("RxNum")
);


-- raw.schedule definition

-- Drop table

-- DROP TABLE raw.schedule;

CREATE TABLE raw.schedule (
	"ScheduleNum" int8 NOT NULL,
	"SchedDate" date NULL,
	"StartTime" time NULL,
	"StopTime" time NULL,
	"SchedType" int2 NULL,
	"ProvNum" int8 NULL,
	"BlockoutType" int8 NULL,
	"Note" text NULL,
	"Status" int2 NULL,
	"EmployeeNum" int8 NULL,
	"DateTStamp" timestamp NULL,
	"ClinicNum" int8 NULL,
	CONSTRAINT schedule_pkey PRIMARY KEY ("ScheduleNum")
);


-- raw.scheduledprocess definition

-- Drop table

-- DROP TABLE raw.scheduledprocess;

CREATE TABLE raw.scheduledprocess (
	"ScheduledProcessNum" int8 NOT NULL,
	"ScheduledAction" varchar(50) NULL,
	"TimeToRun" timestamp NULL,
	"FrequencyToRun" varchar(50) NULL,
	"LastRanDateTime" timestamp NULL,
	CONSTRAINT scheduledprocess_pkey PRIMARY KEY ("ScheduledProcessNum")
);


-- raw.scheduleop definition

-- Drop table

-- DROP TABLE raw.scheduleop;

CREATE TABLE raw.scheduleop (
	"ScheduleOpNum" int8 NOT NULL,
	"ScheduleNum" int8 NULL,
	"OperatoryNum" int8 NULL,
	CONSTRAINT scheduleop_pkey PRIMARY KEY ("ScheduleOpNum")
);


-- raw.schoolclass definition

-- Drop table

-- DROP TABLE raw.schoolclass;

CREATE TABLE raw.schoolclass (
	"SchoolClassNum" int8 NOT NULL,
	"GradYear" int4 NULL,
	"Descript" varchar(255) NULL,
	CONSTRAINT schoolclass_pkey PRIMARY KEY ("SchoolClassNum")
);


-- raw.schoolcourse definition

-- Drop table

-- DROP TABLE raw.schoolcourse;

CREATE TABLE raw.schoolcourse (
	"SchoolCourseNum" int8 NOT NULL,
	"CourseID" varchar(255) NULL,
	"Descript" varchar(255) NULL,
	CONSTRAINT schoolcourse_pkey PRIMARY KEY ("SchoolCourseNum")
);


-- raw.screen definition

-- Drop table

-- DROP TABLE raw.screen;

CREATE TABLE raw.screen (
	"ScreenNum" int8 NOT NULL,
	"Gender" bool NULL,
	"RaceOld" bool NULL,
	"GradeLevel" bool NULL,
	"Age" bool NULL,
	"Urgency" bool NULL,
	"HasCaries" bool NULL,
	"NeedsSealants" bool NULL,
	"CariesExperience" bool NULL,
	"EarlyChildCaries" bool NULL,
	"ExistingSealants" bool NULL,
	"MissingAllTeeth" bool NULL,
	"Birthdate" date NULL,
	"ScreenGroupNum" int8 NULL,
	"ScreenGroupOrder" int2 NULL,
	"Comments" varchar(255) NULL,
	"ScreenPatNum" int8 NULL,
	"SheetNum" int8 NULL,
	CONSTRAINT screen_pkey PRIMARY KEY ("ScreenNum")
);


-- raw.screengroup definition

-- Drop table

-- DROP TABLE raw.screengroup;

CREATE TABLE raw.screengroup (
	"ScreenGroupNum" int8 NOT NULL,
	"Description" varchar(255) NULL,
	"SGDate" date NULL,
	"ProvName" varchar(255) NULL,
	"ProvNum" int8 NULL,
	"PlaceService" bool NULL,
	"County" varchar(255) NULL,
	"GradeSchool" varchar(255) NULL,
	"SheetDefNum" int8 NULL,
	CONSTRAINT screengroup_pkey PRIMARY KEY ("ScreenGroupNum")
);


-- raw.screenpat definition

-- Drop table

-- DROP TABLE raw.screenpat;

CREATE TABLE raw.screenpat (
	"ScreenPatNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	"ScreenGroupNum" int8 NULL,
	"SheetNum" int8 NULL,
	"PatScreenPerm" bool NULL,
	CONSTRAINT screenpat_pkey PRIMARY KEY ("ScreenPatNum")
);


-- raw.securitylog definition

-- Drop table

-- DROP TABLE raw.securitylog;

CREATE TABLE raw.securitylog (
	"SecurityLogNum" int8 NOT NULL,
	"PermType" int2 NULL,
	"UserNum" int8 NULL,
	"LogDateTime" timestamp NULL,
	"LogText" text NULL,
	"PatNum" int8 NULL,
	"CompName" varchar(255) NULL,
	"FKey" int8 NULL,
	"LogSource" int2 NULL,
	"DefNum" int8 NULL,
	"DefNumError" int8 NULL,
	"DateTPrevious" timestamp NULL,
	CONSTRAINT securitylog_pkey PRIMARY KEY ("SecurityLogNum")
);


-- raw.securityloghash definition

-- Drop table

-- DROP TABLE raw.securityloghash;

CREATE TABLE raw.securityloghash (
	"SecurityLogHashNum" int8 NOT NULL,
	"SecurityLogNum" int8 NULL,
	"LogHash" varchar(255) NULL,
	CONSTRAINT securityloghash_pkey PRIMARY KEY ("SecurityLogHashNum")
);


-- raw.sessiontoken definition

-- Drop table

-- DROP TABLE raw.sessiontoken;

CREATE TABLE raw.sessiontoken (
	"SessionTokenNum" int8 NOT NULL,
	"SessionTokenHash" varchar(255) NULL,
	"Expiration" timestamp NULL,
	"TokenType" bool NULL,
	"FKey" int8 NULL,
	CONSTRAINT sessiontoken_pkey PRIMARY KEY ("SessionTokenNum")
);


-- raw.sheet definition

-- Drop table

-- DROP TABLE raw.sheet;

CREATE TABLE raw.sheet (
	"SheetNum" int8 NOT NULL,
	"SheetType" int4 NULL,
	"PatNum" int8 NULL,
	"DateTimeSheet" timestamp NULL,
	"FontSize" float4 NULL,
	"FontName" varchar(255) NULL,
	"Width" int4 NULL,
	"Height" int4 NULL,
	"IsLandscape" bool NULL,
	"InternalNote" text NULL,
	"Description" varchar(255) NULL,
	"ShowInTerminal" int2 NULL,
	"IsWebForm" bool NULL,
	"IsMultiPage" bool NULL,
	"IsDeleted" bool NULL,
	"SheetDefNum" int8 NULL,
	"DocNum" int8 NULL,
	"ClinicNum" int8 NULL,
	"DateTSheetEdited" timestamp NULL,
	"HasMobileLayout" bool NULL,
	"RevID" int4 NULL,
	"WebFormSheetID" int8 NULL,
	CONSTRAINT sheet_pkey PRIMARY KEY ("SheetNum")
);


-- raw.sheetdef definition

-- Drop table

-- DROP TABLE raw.sheetdef;

CREATE TABLE raw.sheetdef (
	"SheetDefNum" int8 NOT NULL,
	"Description" varchar(255) NULL,
	"SheetType" int4 NULL,
	"FontSize" float4 NULL,
	"FontName" varchar(255) NULL,
	"Width" int4 NULL,
	"Height" int4 NULL,
	"IsLandscape" bool NULL,
	"PageCount" int4 NULL,
	"IsMultiPage" bool NULL,
	"BypassGlobalLock" bool NULL,
	"HasMobileLayout" bool NULL,
	"DateTCreated" timestamp NULL,
	"RevID" int4 NULL,
	"AutoCheckSaveImage" bool NULL,
	"AutoCheckSaveImageDocCategory" int8 NULL,
	CONSTRAINT sheetdef_pkey PRIMARY KEY ("SheetDefNum")
);


-- raw.sheetfield definition

-- Drop table

-- DROP TABLE raw.sheetfield;

CREATE TABLE raw.sheetfield (
	"SheetFieldNum" int8 NOT NULL,
	"SheetNum" int8 NULL,
	"FieldType" int4 NULL,
	"FieldName" varchar(255) NULL,
	"FieldValue" text NULL,
	"FontSize" float4 NULL,
	"FontName" varchar(255) NULL,
	"FontIsBold" bool NULL,
	"XPos" int4 NULL,
	"YPos" int4 NULL,
	"Width" int4 NULL,
	"Height" int4 NULL,
	"GrowthBehavior" int4 NULL,
	"RadioButtonValue" varchar(255) NULL,
	"RadioButtonGroup" varchar(255) NULL,
	"IsRequired" bool NULL,
	"TabOrder" int4 NULL,
	"ReportableName" varchar(255) NULL,
	"TextAlign" int2 NULL,
	"ItemColor" int4 NULL,
	"DateTimeSig" timestamp NULL,
	"IsLocked" bool NULL,
	"TabOrderMobile" int4 NULL,
	"UiLabelMobile" text NULL,
	"UiLabelMobileRadioButton" text NULL,
	"SheetFieldDefNum" int8 NULL,
	"CanElectronicallySign" bool NULL,
	"IsSigProvRestricted" bool NULL,
	CONSTRAINT sheetfield_pkey PRIMARY KEY ("SheetFieldNum")
);


-- raw.sheetfielddef definition

-- Drop table

-- DROP TABLE raw.sheetfielddef;

CREATE TABLE raw.sheetfielddef (
	"SheetFieldDefNum" int8 NOT NULL,
	"SheetDefNum" int8 NULL,
	"FieldType" int4 NULL,
	"FieldName" varchar(255) NULL,
	"FieldValue" text NULL,
	"FontSize" float4 NULL,
	"FontName" varchar(255) NULL,
	"FontIsBold" bool NULL,
	"XPos" int4 NULL,
	"YPos" int4 NULL,
	"Width" int4 NULL,
	"Height" int4 NULL,
	"GrowthBehavior" int4 NULL,
	"RadioButtonValue" varchar(255) NULL,
	"RadioButtonGroup" varchar(255) NULL,
	"IsRequired" bool NULL,
	"TabOrder" int4 NULL,
	"ReportableName" varchar(255) NULL,
	"TextAlign" int2 NULL,
	"IsPaymentOption" bool NULL,
	"ItemColor" int4 NULL,
	"IsLocked" bool NULL,
	"TabOrderMobile" int4 NULL,
	"UiLabelMobile" text NULL,
	"UiLabelMobileRadioButton" text NULL,
	"LayoutMode" bool NULL,
	"Language" varchar(255) NULL,
	"CanElectronicallySign" bool NULL,
	"IsSigProvRestricted" bool NULL,
	CONSTRAINT sheetfielddef_pkey PRIMARY KEY ("SheetFieldDefNum")
);


-- raw.sigbutdef definition

-- Drop table

-- DROP TABLE raw.sigbutdef;

CREATE TABLE raw.sigbutdef (
	"SigButDefNum" int8 NOT NULL,
	"ButtonText" varchar(255) NULL,
	"ButtonIndex" int2 NULL,
	"SynchIcon" int2 NULL,
	"ComputerName" varchar(255) NULL,
	"SigElementDefNumUser" int8 NULL,
	"SigElementDefNumExtra" int8 NULL,
	"SigElementDefNumMsg" int8 NULL,
	CONSTRAINT sigbutdef_pkey PRIMARY KEY ("SigButDefNum")
);


-- raw.sigelementdef definition

-- Drop table

-- DROP TABLE raw.sigelementdef;

CREATE TABLE raw.sigelementdef (
	"SigElementDefNum" int8 NOT NULL,
	"LightRow" int2 NULL,
	"LightColor" int4 NULL,
	"SigElementType" int2 NULL,
	"SigText" varchar(255) NULL,
	"Sound" text NULL,
	"ItemOrder" int2 NULL,
	CONSTRAINT sigelementdef_pkey PRIMARY KEY ("SigElementDefNum")
);


-- raw.sigmessage definition

-- Drop table

-- DROP TABLE raw.sigmessage;

CREATE TABLE raw.sigmessage (
	"SigMessageNum" int8 NOT NULL,
	"ButtonText" varchar(255) NULL,
	"ButtonIndex" int4 NULL,
	"SynchIcon" bool NULL,
	"FromUser" varchar(255) NULL,
	"ToUser" varchar(255) NULL,
	"MessageDateTime" timestamp NULL,
	"AckDateTime" timestamp NULL,
	"SigText" varchar(255) NULL,
	"SigElementDefNumUser" int8 NULL,
	"SigElementDefNumExtra" int8 NULL,
	"SigElementDefNumMsg" int8 NULL,
	CONSTRAINT sigmessage_pkey PRIMARY KEY ("SigMessageNum")
);


-- raw.signalod definition

-- Drop table

-- DROP TABLE raw.signalod;

CREATE TABLE raw.signalod (
	"SignalNum" int8 NOT NULL,
	"DateViewing" date NULL,
	"SigDateTime" timestamp NULL,
	"FKey" int8 NULL,
	"FKeyType" varchar(255) NULL,
	"IType" int2 NULL,
	"RemoteRole" bool NULL,
	"MsgValue" text NULL,
	CONSTRAINT signalod_pkey PRIMARY KEY ("SignalNum")
);


-- raw.site definition

-- Drop table

-- DROP TABLE raw.site;

CREATE TABLE raw.site (
	"SiteNum" int8 NOT NULL,
	"Description" varchar(255) NULL,
	"Note" text NULL,
	"Address" varchar(100) NULL,
	"Address2" varchar(100) NULL,
	"City" varchar(100) NULL,
	"State" varchar(100) NULL,
	"Zip" varchar(100) NULL,
	"ProvNum" int8 NULL,
	"PlaceService" bool NULL,
	CONSTRAINT site_pkey PRIMARY KEY ("SiteNum")
);


-- raw.smsblockphone definition

-- Drop table

-- DROP TABLE raw.smsblockphone;

CREATE TABLE raw.smsblockphone (
	"SmsBlockPhoneNum" int8 NOT NULL,
	"BlockWirelessNumber" varchar(255) NULL,
	CONSTRAINT smsblockphone_pkey PRIMARY KEY ("SmsBlockPhoneNum")
);


-- raw.smsfrommobile definition

-- Drop table

-- DROP TABLE raw.smsfrommobile;

CREATE TABLE raw.smsfrommobile (
	"SmsFromMobileNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	"ClinicNum" int8 NULL,
	"CommlogNum" int8 NULL,
	"MsgText" text NULL,
	"DateTimeReceived" timestamp NULL,
	"SmsPhoneNumber" varchar(255) NULL,
	"MobilePhoneNumber" varchar(255) NULL,
	"MsgPart" int4 NULL,
	"MsgTotal" int4 NULL,
	"MsgRefID" varchar(255) NULL,
	"SmsStatus" bool NULL,
	"Flags" varchar(255) NULL,
	"IsHidden" bool NULL,
	"MatchCount" int4 NULL,
	"GuidMessage" varchar(255) NULL,
	"SecDateTEdit" timestamp NULL,
	CONSTRAINT smsfrommobile_pkey PRIMARY KEY ("SmsFromMobileNum")
);


-- raw.smsphone definition

-- Drop table

-- DROP TABLE raw.smsphone;

CREATE TABLE raw.smsphone (
	"SmsPhoneNum" int8 NOT NULL,
	"ClinicNum" int8 NULL,
	"PhoneNumber" varchar(255) NULL,
	"DateTimeActive" timestamp NULL,
	"DateTimeInactive" timestamp NULL,
	"InactiveCode" varchar(255) NULL,
	"CountryCode" varchar(255) NULL,
	CONSTRAINT smsphone_pkey PRIMARY KEY ("SmsPhoneNum")
);


-- raw.smstomobile definition

-- Drop table

-- DROP TABLE raw.smstomobile;

CREATE TABLE raw.smstomobile (
	"SmsToMobileNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	"GuidMessage" varchar(255) NULL,
	"GuidBatch" varchar(255) NULL,
	"SmsPhoneNumber" varchar(255) NULL,
	"MobilePhoneNumber" varchar(255) NULL,
	"IsTimeSensitive" bool NULL,
	"MsgType" int2 NULL,
	"MsgText" text NULL,
	"SmsStatus" int2 NULL,
	"MsgParts" int4 NULL,
	"MsgChargeUSD" float4 NULL,
	"ClinicNum" int8 NULL,
	"CustErrorText" varchar(255) NULL,
	"DateTimeSent" timestamp NULL,
	"DateTimeTerminated" timestamp NULL,
	"IsHidden" bool NULL,
	"MsgDiscountUSD" float4 NULL,
	"SecDateTEdit" timestamp NULL,
	CONSTRAINT smstomobile_pkey PRIMARY KEY ("SmsToMobileNum")
);


-- raw.snomed definition

-- Drop table

-- DROP TABLE raw.snomed;

CREATE TABLE raw.snomed (
	"SnomedNum" int8 NOT NULL,
	"SnomedCode" varchar(255) NULL,
	"Description" varchar(255) NULL,
	CONSTRAINT snomed_pkey PRIMARY KEY ("SnomedNum")
);


-- raw.sop definition

-- Drop table

-- DROP TABLE raw.sop;

CREATE TABLE raw.sop (
	"SopNum" int8 NOT NULL,
	"SopCode" varchar(255) NULL,
	"Description" varchar(255) NULL,
	CONSTRAINT sop_pkey PRIMARY KEY ("SopNum")
);


-- raw.stateabbr definition

-- Drop table

-- DROP TABLE raw.stateabbr;

CREATE TABLE raw.stateabbr (
	"StateAbbrNum" int8 NOT NULL,
	"Description" varchar(50) NULL,
	"Abbr" varchar(50) NULL,
	"MedicaidIDLength" int4 NULL,
	CONSTRAINT stateabbr_pkey PRIMARY KEY ("StateAbbrNum")
);


-- raw."statement" definition

-- Drop table

-- DROP TABLE raw."statement";

CREATE TABLE raw."statement" (
	"StatementNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	"DateSent" date NULL,
	"DateRangeFrom" date NULL,
	"DateRangeTo" date NULL,
	"Note" text NULL,
	"NoteBold" text NULL,
	"Mode_" int2 NULL,
	"HidePayment" bool NULL,
	"SinglePatient" bool NULL,
	"Intermingled" bool NULL,
	"IsSent" bool NULL,
	"DocNum" int8 NULL,
	"DateTStamp" timestamp NULL,
	"IsReceipt" bool NULL,
	"IsInvoice" bool NULL,
	"IsInvoiceCopy" bool NULL,
	"EmailSubject" varchar(255) NULL,
	"EmailBody" text NULL,
	"SuperFamily" int8 NULL,
	"IsBalValid" bool NULL,
	"InsEst" float8 NULL,
	"BalTotal" float8 NULL,
	"StatementType" varchar(50) NULL,
	"ShortGUID" varchar(30) NULL,
	"StatementShortURL" varchar(50) NULL,
	"StatementURL" varchar(255) NULL,
	"SmsSendStatus" int2 NULL,
	"LimitedCustomFamily" int2 NULL,
	"ShowTransSinceBalZero" bool NULL,
	CONSTRAINT statement_pkey PRIMARY KEY ("StatementNum")
);


-- raw.statementprod definition

-- Drop table

-- DROP TABLE raw.statementprod;

CREATE TABLE raw.statementprod (
	"StatementProdNum" int8 NOT NULL,
	"StatementNum" int8 NULL,
	"FKey" int8 NULL,
	"ProdType" int2 NULL,
	"LateChargeAdjNum" int8 NULL,
	"DocNum" int8 NULL,
	CONSTRAINT statementprod_pkey PRIMARY KEY ("StatementProdNum")
);


-- raw.stmtlink definition

-- Drop table

-- DROP TABLE raw.stmtlink;

CREATE TABLE raw.stmtlink (
	"StmtLinkNum" int8 NOT NULL,
	"StatementNum" int8 NULL,
	"StmtLinkType" int2 NULL,
	"FKey" int8 NULL,
	CONSTRAINT stmtlink_pkey PRIMARY KEY ("StmtLinkNum")
);


-- raw.substitutionlink definition

-- Drop table

-- DROP TABLE raw.substitutionlink;

CREATE TABLE raw.substitutionlink (
	"SubstitutionLinkNum" int8 NOT NULL,
	"PlanNum" int8 NULL,
	"CodeNum" int8 NULL,
	"SubstitutionCode" varchar(25) NULL,
	"SubstOnlyIf" int4 NULL,
	CONSTRAINT substitutionlink_pkey PRIMARY KEY ("SubstitutionLinkNum")
);


-- raw.supplier definition

-- Drop table

-- DROP TABLE raw.supplier;

CREATE TABLE raw.supplier (
	"SupplierNum" int8 NOT NULL,
	"Name" varchar(255) NULL,
	"Phone" varchar(255) NULL,
	"CustomerId" varchar(255) NULL,
	"Website" text NULL,
	"UserName" varchar(255) NULL,
	"Password" varchar(255) NULL,
	"Note" text NULL,
	CONSTRAINT supplier_pkey PRIMARY KEY ("SupplierNum")
);


-- raw.supply definition

-- Drop table

-- DROP TABLE raw.supply;

CREATE TABLE raw.supply (
	"SupplyNum" int8 NOT NULL,
	"SupplierNum" int8 NULL,
	"CatalogNumber" varchar(255) NULL,
	"Descript" varchar(255) NULL,
	"Category" int8 NULL,
	"ItemOrder" int4 NULL,
	"LevelDesired" float4 NULL,
	"IsHidden" bool NULL,
	"Price" float8 NULL,
	"BarCodeOrID" varchar(255) NULL,
	"DispDefaultQuant" float4 NULL,
	"DispUnitsCount" int4 NULL,
	"DispUnitDesc" varchar(255) NULL,
	"LevelOnHand" float4 NULL,
	"OrderQty" int4 NULL,
	CONSTRAINT supply_pkey PRIMARY KEY ("SupplyNum")
);


-- raw.supplyneeded definition

-- Drop table

-- DROP TABLE raw.supplyneeded;

CREATE TABLE raw.supplyneeded (
	"SupplyNeededNum" int8 NOT NULL,
	"Description" text NULL,
	"DateAdded" date NULL,
	CONSTRAINT supplyneeded_pkey PRIMARY KEY ("SupplyNeededNum")
);


-- raw.supplyorder definition

-- Drop table

-- DROP TABLE raw.supplyorder;

CREATE TABLE raw.supplyorder (
	"SupplyOrderNum" int8 NOT NULL,
	"SupplierNum" int8 NULL,
	"DatePlaced" date NULL,
	"Note" text NULL,
	"AmountTotal" float8 NULL,
	"UserNum" int8 NULL,
	"ShippingCharge" float8 NULL,
	"DateReceived" date NULL,
	CONSTRAINT supplyorder_pkey PRIMARY KEY ("SupplyOrderNum")
);


-- raw.supplyorderitem definition

-- Drop table

-- DROP TABLE raw.supplyorderitem;

CREATE TABLE raw.supplyorderitem (
	"SupplyOrderItemNum" int8 NOT NULL,
	"SupplyOrderNum" int8 NULL,
	"SupplyNum" int8 NULL,
	"Qty" int4 NULL,
	"Price" float8 NULL,
	"DateReceived" date NULL,
	CONSTRAINT supplyorderitem_pkey PRIMARY KEY ("SupplyOrderItemNum")
);


-- raw.task definition

-- Drop table

-- DROP TABLE raw.task;

CREATE TABLE raw.task (
	"TaskNum" int8 NOT NULL,
	"TaskListNum" int8 NULL,
	"DateTask" date NULL,
	"KeyNum" int8 NULL,
	"Descript" text NULL,
	"TaskStatus" int2 NULL,
	"IsRepeating" bool NULL,
	"DateType" bool NULL,
	"FromNum" int8 NULL,
	"ObjectType" int2 NULL,
	"DateTimeEntry" timestamp NULL,
	"UserNum" int8 NULL,
	"DateTimeFinished" timestamp NULL,
	"PriorityDefNum" int8 NULL,
	"ReminderGroupId" varchar(20) NULL,
	"ReminderType" int2 NULL,
	"ReminderFrequency" int4 NULL,
	"DateTimeOriginal" timestamp NULL,
	"SecDateTEdit" timestamp NULL,
	"DescriptOverride" varchar(255) NULL,
	"IsReadOnly" bool NULL,
	"TriageCategory" int8 NULL,
	CONSTRAINT task_pkey PRIMARY KEY ("TaskNum")
);


-- raw.taskancestor definition

-- Drop table

-- DROP TABLE raw.taskancestor;

CREATE TABLE raw.taskancestor (
	"TaskAncestorNum" int8 NOT NULL,
	"TaskNum" int8 NULL,
	"TaskListNum" int8 NULL,
	CONSTRAINT taskancestor_pkey PRIMARY KEY ("TaskAncestorNum")
);


-- raw.taskattachment definition

-- Drop table

-- DROP TABLE raw.taskattachment;

CREATE TABLE raw.taskattachment (
	"TaskAttachmentNum" int8 NOT NULL,
	"TaskNum" int8 NULL,
	"DocNum" int8 NULL,
	"TextValue" text NULL,
	"Description" varchar(255) NULL,
	CONSTRAINT taskattachment_pkey PRIMARY KEY ("TaskAttachmentNum")
);


-- raw.taskhist definition

-- Drop table

-- DROP TABLE raw.taskhist;

CREATE TABLE raw.taskhist (
	"TaskHistNum" int8 NOT NULL,
	"UserNumHist" int8 NULL,
	"DateTStamp" timestamp NULL,
	"IsNoteChange" bool NULL,
	"TaskNum" int8 NULL,
	"TaskListNum" int8 NULL,
	"DateTask" date NULL,
	"KeyNum" int8 NULL,
	"Descript" text NULL,
	"TaskStatus" int2 NULL,
	"IsRepeating" bool NULL,
	"DateType" bool NULL,
	"FromNum" int8 NULL,
	"ObjectType" int2 NULL,
	"DateTimeEntry" timestamp NULL,
	"UserNum" int8 NULL,
	"DateTimeFinished" timestamp NULL,
	"PriorityDefNum" int8 NULL,
	"ReminderGroupId" varchar(20) NULL,
	"ReminderType" int2 NULL,
	"ReminderFrequency" int4 NULL,
	"DateTimeOriginal" timestamp NULL,
	"SecDateTEdit" timestamp NULL,
	"DescriptOverride" varchar(255) NULL,
	"IsReadOnly" bool NULL,
	"TriageCategory" int8 NULL,
	CONSTRAINT taskhist_pkey PRIMARY KEY ("TaskHistNum")
);


-- raw.tasklist definition

-- Drop table

-- DROP TABLE raw.tasklist;

CREATE TABLE raw.tasklist (
	"TaskListNum" int8 NOT NULL,
	"Descript" varchar(255) NULL,
	"Parent" int8 NULL,
	"DateTL" date NULL,
	"IsRepeating" bool NULL,
	"DateType" bool NULL,
	"FromNum" int8 NULL,
	"ObjectType" int2 NULL,
	"DateTimeEntry" timestamp NULL,
	"GlobalTaskFilterType" bool NULL,
	"TaskListStatus" int2 NULL,
	CONSTRAINT tasklist_pkey PRIMARY KEY ("TaskListNum")
);


-- raw.tasknote definition

-- Drop table

-- DROP TABLE raw.tasknote;

CREATE TABLE raw.tasknote (
	"TaskNoteNum" int8 NOT NULL,
	"TaskNum" int8 NULL,
	"UserNum" int8 NULL,
	"DateTimeNote" timestamp NULL,
	"Note" text NULL,
	CONSTRAINT tasknote_pkey PRIMARY KEY ("TaskNoteNum")
);


-- raw.tasksubscription definition

-- Drop table

-- DROP TABLE raw.tasksubscription;

CREATE TABLE raw.tasksubscription (
	"TaskSubscriptionNum" int8 NOT NULL,
	"UserNum" int8 NULL,
	"TaskListNum" int8 NULL,
	"TaskNum" int8 NULL,
	CONSTRAINT tasksubscription_pkey PRIMARY KEY ("TaskSubscriptionNum")
);


-- raw.taskunread definition

-- Drop table

-- DROP TABLE raw.taskunread;

CREATE TABLE raw.taskunread (
	"TaskUnreadNum" int8 NOT NULL,
	"TaskNum" int8 NULL,
	"UserNum" int8 NULL,
	CONSTRAINT taskunread_pkey PRIMARY KEY ("TaskUnreadNum")
);


-- raw.tempimageconv definition

-- Drop table

-- DROP TABLE raw.tempimageconv;

CREATE TABLE raw.tempimageconv (
	filepath text NULL,
	filename text NULL,
	patnum int8 NULL,
	patnum_str varchar(30) NULL,
	foldername varchar(50) NULL
);


-- raw.tempimageconv2 definition

-- Drop table

-- DROP TABLE raw.tempimageconv2;

CREATE TABLE raw.tempimageconv2 (
	filepath text NULL,
	filename varchar(100) NULL,
	patnum int8 NULL,
	patnum_str varchar(30) NULL,
	imagedate varchar(50) NULL,
	description varchar(200) NULL,
	document_id varchar(50) NULL,
	"IsDeleted" int4 NULL,
	doccategory int4 NULL
);


-- raw.tempinsplan_bak_20200424 definition

-- Drop table

-- DROP TABLE raw.tempinsplan_bak_20200424;

CREATE TABLE raw.tempinsplan_bak_20200424 (
	"PlanNum" int8 NULL,
	"GroupName" varchar(50) NULL,
	"GroupNum" varchar(25) NULL,
	"PlanNote" text NULL,
	"FeeSched" int8 NULL,
	"PlanType" bpchar(1) NULL,
	"ClaimFormNum" int8 NULL,
	"UseAltCode" bool NULL,
	"ClaimsUseUCR" bool NULL,
	"CopayFeeSched" int8 NULL,
	"EmployerNum" int8 NULL,
	"CarrierNum" int8 NULL,
	"AllowedFeeSched" int8 NULL,
	"TrojanID" varchar(100) NULL,
	"DivisionNo" varchar(255) NULL,
	"IsMedical" bool NULL,
	"FilingCode" int8 NULL,
	"DentaideCardSequence" bool NULL,
	"ShowBaseUnits" bool NULL,
	"CodeSubstNone" bool NULL,
	"IsHidden" bool NULL,
	"MonthRenew" int2 NULL,
	"FilingCodeSubtype" int8 NULL,
	"CanadianPlanFlag" varchar(5) NULL,
	"CanadianDiagnosticCode" varchar(255) NULL,
	"CanadianInstitutionCode" varchar(255) NULL,
	"RxBIN" varchar(255) NULL,
	"CobRule" bool NULL,
	"SopCode" varchar(255) NULL,
	"SecUserNumEntry" int8 NULL,
	"SecDateEntry" date NULL,
	"SecDateTEdit" timestamp NULL,
	"HideFromVerifyList" bool NULL,
	"OrthoType" bool NULL,
	"OrthoAutoProcFreq" bool NULL,
	"OrthoAutoProcCodeNumOverride" int8 NULL,
	"OrthoAutoFeeBilled" float8 NULL,
	"OrthoAutoClaimDaysWait" int4 NULL,
	"BillingType" int8 NULL,
	"HasPpoSubstWriteoffs" bool NULL,
	"ExclusionFeeRule" bool NULL
);


-- raw.temppopup_bak_20200424 definition

-- Drop table

-- DROP TABLE raw.temppopup_bak_20200424;

CREATE TABLE raw.temppopup_bak_20200424 (
	"PopupNum" int8 NULL,
	"PatNum" int8 NULL,
	"Description" text NULL,
	"IsDisabled" bool NULL,
	"PopupLevel" bool NULL,
	"UserNum" int8 NULL,
	"DateTimeEntry" timestamp NULL,
	"IsArchived" bool NULL,
	"PopupNumArchive" int8 NULL
);


-- raw.tempprocedurecode_bak_20240617 definition

-- Drop table

-- DROP TABLE raw.tempprocedurecode_bak_20240617;

CREATE TABLE raw.tempprocedurecode_bak_20240617 (
	"CodeNum" int8 NULL,
	"ProcCode" varchar(15) NULL,
	"Descript" varchar(255) NULL,
	"AbbrDesc" varchar(50) NULL,
	"ProcTime" varchar(24) NULL,
	"ProcCat" int8 NULL,
	"TreatArea" int2 NULL,
	"NoBillIns" bool NULL,
	"IsProsth" bool NULL,
	"DefaultNote" text NULL,
	"IsHygiene" bool NULL,
	"GTypeNum" int2 NULL,
	"AlternateCode1" varchar(15) NULL,
	"MedicalCode" varchar(15) NULL,
	"IsTaxed" bool NULL,
	"PaintType" int2 NULL,
	"GraphicColor" int4 NULL,
	"LaymanTerm" varchar(255) NULL,
	"IsCanadianLab" bool NULL,
	"PreExisting" bool NULL,
	"BaseUnits" int4 NULL,
	"SubstitutionCode" varchar(25) NULL,
	"SubstOnlyIf" int4 NULL,
	"DateTStamp" timestamp NULL,
	"IsMultiVisit" bool NULL,
	"DrugNDC" varchar(255) NULL,
	"RevenueCodeDefault" varchar(255) NULL,
	"ProvNumDefault" int8 NULL,
	"CanadaTimeUnits" float8 NULL,
	"IsRadiology" bool NULL,
	"DefaultClaimNote" text NULL,
	"DefaultTPNote" text NULL,
	"BypassGlobalLock" bool NULL,
	"TaxCode" varchar(16) NULL,
	"PaintText" varchar(255) NULL,
	"AreaAlsoToothRange" bool NULL,
	"DiagnosticCodes" varchar(255) NULL
);


-- raw.tempschedule_bak_20200424 definition

-- Drop table

-- DROP TABLE raw.tempschedule_bak_20200424;

CREATE TABLE raw.tempschedule_bak_20200424 (
	"ScheduleNum" int8 NULL,
	"SchedDate" date NULL,
	"StartTime" time NULL,
	"StopTime" time NULL,
	"SchedType" int2 NULL,
	"ProvNum" int8 NULL,
	"BlockoutType" int8 NULL,
	"Note" text NULL,
	"Status" bool NULL,
	"EmployeeNum" int8 NULL,
	"DateTStamp" timestamp NULL,
	"ClinicNum" int8 NULL
);


-- raw.tempscheduleop_bak_20200424 definition

-- Drop table

-- DROP TABLE raw.tempscheduleop_bak_20200424;

CREATE TABLE raw.tempscheduleop_bak_20200424 (
	"ScheduleOpNum" int8 NULL,
	"ScheduleNum" int8 NULL,
	"OperatoryNum" int8 NULL
);


-- raw.terminalactive definition

-- Drop table

-- DROP TABLE raw.terminalactive;

CREATE TABLE raw.terminalactive (
	"TerminalActiveNum" int8 NOT NULL,
	"ComputerName" varchar(255) NULL,
	"TerminalStatus" bool NULL,
	"PatNum" int8 NULL,
	"SessionId" int4 NULL,
	"ProcessId" int4 NULL,
	"SessionName" varchar(255) NULL,
	CONSTRAINT terminalactive_pkey PRIMARY KEY ("TerminalActiveNum")
);


-- raw.timeadjust definition

-- Drop table

-- DROP TABLE raw.timeadjust;

CREATE TABLE raw.timeadjust (
	"TimeAdjustNum" int8 NOT NULL,
	"EmployeeNum" int8 NULL,
	"TimeEntry" timestamp NULL,
	"RegHours" time NULL,
	"OTimeHours" time NULL,
	"Note" text NULL,
	"IsAuto" bool NULL,
	"ClinicNum" int8 NULL,
	"PtoDefNum" int8 NULL,
	"PtoHours" time NULL,
	"IsUnpaidProtectedLeave" bool NULL,
	"SecuUserNumEntry" int8 NULL,
	CONSTRAINT timeadjust_pkey PRIMARY KEY ("TimeAdjustNum")
);


-- raw.timecardrule definition

-- Drop table

-- DROP TABLE raw.timecardrule;

CREATE TABLE raw.timecardrule (
	"TimeCardRuleNum" int8 NOT NULL,
	"EmployeeNum" int8 NULL,
	"OverHoursPerDay" time NULL,
	"AfterTimeOfDay" time NULL,
	"BeforeTimeOfDay" time NULL,
	"IsOvertimeExempt" bool NULL,
	"MinClockInTime" time NULL,
	"HasWeekendRate3" bool NULL,
	CONSTRAINT timecardrule_pkey PRIMARY KEY ("TimeCardRuleNum")
);


-- raw.tmpdocument definition

-- Drop table

-- DROP TABLE raw.tmpdocument;

CREATE TABLE raw.tmpdocument (
	document_id varchar(50) NULL,
	document_name text NULL,
	document_type text NULL,
	document_creation_date text NULL,
	document_modified_date text NULL,
	open_with text NULL,
	document_group_id text NULL,
	patient_id varchar(50) NULL,
	ref_table text NULL,
	sec_ref_id text NULL,
	sec_ref_table text NULL,
	user_id text NULL,
	original_user_id text NULL,
	private text NULL,
	display_in_docmgr text NULL,
	signed text NULL,
	archive_name text NULL,
	archive_path text NULL,
	needs_converted text NULL,
	notice_of_privacy text NULL,
	privacy_authorization text NULL,
	consent text NULL,
	practice_id text NULL,
	custom_document_id text NULL,
	headerfooter_added text NULL,
	doccategory int4 NULL
);


-- raw.tmpimages definition

-- Drop table

-- DROP TABLE raw.tmpimages;

CREATE TABLE raw.tmpimages (
	patient_id varchar(50) NULL,
	images_date_time text NULL,
	image_id varchar(50) NULL,
	use_as_example text NULL,
	tooth_number text NULL,
	in_use text NULL,
	"row" text NULL,
	_column text NULL,
	date_last_modified text NULL,
	last_modified_by text NULL,
	image_type text NULL,
	acquired_from text NULL,
	exam_id text NULL,
	calibration text NULL,
	signature text NULL,
	example_group text NULL,
	document_group text NULL,
	acquired_by text NULL,
	height text NULL,
	width text NULL,
	original_image_id text NULL,
	image_to_delete text NULL,
	image_description text NULL,
	instance_uid text NULL,
	archive_name text NULL,
	archive_path text NULL,
	image_status text NULL,
	practice_id text NULL,
	maximus_signature text NULL,
	"DolphinTimePointImage" text NULL,
	exposure_level text NULL,
	sidexis_image_position text NULL
);


-- raw.tmppatient definition

-- Drop table

-- DROP TABLE raw.tmppatient;

CREATE TABLE raw.tmppatient (
	patient_id varchar(50) NULL,
	patient_image_id varchar(50) NULL
);


-- raw.toolbutitem definition

-- Drop table

-- DROP TABLE raw.toolbutitem;

CREATE TABLE raw.toolbutitem (
	"ToolButItemNum" int8 NOT NULL,
	"ProgramNum" int8 NULL,
	"ToolBar" int2 NULL,
	"ButtonText" varchar(255) NULL,
	CONSTRAINT toolbutitem_pkey PRIMARY KEY ("ToolButItemNum")
);


-- raw.toothgridcell definition

-- Drop table

-- DROP TABLE raw.toothgridcell;

CREATE TABLE raw.toothgridcell (
	"ToothGridCellNum" int8 NOT NULL,
	"SheetFieldNum" int8 NULL,
	"ToothGridColNum" int8 NULL,
	"ValueEntered" varchar(255) NULL,
	"ToothNum" varchar(10) NULL,
	CONSTRAINT toothgridcell_pkey PRIMARY KEY ("ToothGridCellNum")
);


-- raw.toothgridcol definition

-- Drop table

-- DROP TABLE raw.toothgridcol;

CREATE TABLE raw.toothgridcol (
	"ToothGridColNum" int8 NOT NULL,
	"SheetFieldNum" int8 NULL,
	"NameItem" varchar(255) NULL,
	"CellType" bool NULL,
	"ItemOrder" int2 NULL,
	"ColumnWidth" int2 NULL,
	"CodeNum" int8 NULL,
	"ProcStatus" bool NULL,
	CONSTRAINT toothgridcol_pkey PRIMARY KEY ("ToothGridColNum")
);


-- raw.toothinitial definition

-- Drop table

-- DROP TABLE raw.toothinitial;

CREATE TABLE raw.toothinitial (
	"ToothInitialNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	"ToothNum" varchar(2) NULL,
	"InitialType" int2 NULL,
	"Movement" float4 NULL,
	"DrawingSegment" text NULL,
	"ColorDraw" int4 NULL,
	"SecDateTEntry" timestamp NULL,
	"SecDateTEdit" timestamp NULL,
	"DrawText" varchar(255) NULL,
	CONSTRAINT toothinitial_pkey PRIMARY KEY ("ToothInitialNum")
);


-- raw."transaction" definition

-- Drop table

-- DROP TABLE raw."transaction";

CREATE TABLE raw."transaction" (
	"TransactionNum" int8 NOT NULL,
	"DateTimeEntry" timestamp NULL,
	"UserNum" int8 NULL,
	"DepositNum" int8 NULL,
	"PayNum" int8 NULL,
	"SecUserNumEdit" int8 NULL,
	"SecDateTEdit" timestamp NULL,
	"TransactionInvoiceNum" int8 NULL,
	CONSTRAINT transaction_pkey PRIMARY KEY ("TransactionNum")
);


-- raw.transactioninvoice definition

-- Drop table

-- DROP TABLE raw.transactioninvoice;

CREATE TABLE raw.transactioninvoice (
	"TransactionInvoiceNum" int8 NOT NULL,
	"FileName" varchar(255) NULL,
	"InvoiceData" text NULL,
	"FilePath" varchar(255) NULL,
	CONSTRAINT transactioninvoice_pkey PRIMARY KEY ("TransactionInvoiceNum")
);


-- raw.treatplan definition

-- Drop table

-- DROP TABLE raw.treatplan;

CREATE TABLE raw.treatplan (
	"TreatPlanNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	"DateTP" date NULL,
	"Heading" varchar(255) NULL,
	"Note" text NULL,
	"Signature" text NULL,
	"SigIsTopaz" bool NULL,
	"ResponsParty" int8 NULL,
	"DocNum" int8 NULL,
	"TPStatus" int2 NULL,
	"SecUserNumEntry" int8 NULL,
	"SecDateEntry" date NULL,
	"SecDateTEdit" timestamp NULL,
	"UserNumPresenter" int8 NULL,
	"TPType" bool NULL,
	"SignaturePractice" text NULL,
	"DateTSigned" timestamp NULL,
	"DateTPracticeSigned" timestamp NULL,
	"SignatureText" varchar(255) NULL,
	"SignaturePracticeText" varchar(255) NULL,
	"MobileAppDeviceNum" int8 NULL,
	CONSTRAINT treatplan_pkey PRIMARY KEY ("TreatPlanNum")
);


-- raw.treatplanattach definition

-- Drop table

-- DROP TABLE raw.treatplanattach;

CREATE TABLE raw.treatplanattach (
	"TreatPlanAttachNum" int8 NOT NULL,
	"TreatPlanNum" int8 NULL,
	"ProcNum" int8 NULL,
	"Priority" int8 NULL,
	CONSTRAINT treatplanattach_pkey PRIMARY KEY ("TreatPlanAttachNum")
);


-- raw.treatplanparam definition

-- Drop table

-- DROP TABLE raw.treatplanparam;

CREATE TABLE raw.treatplanparam (
	"TreatPlanParamNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	"TreatPlanNum" int8 NULL,
	"ShowDiscount" bool NULL,
	"ShowMaxDed" bool NULL,
	"ShowSubTotals" bool NULL,
	"ShowTotals" bool NULL,
	"ShowCompleted" bool NULL,
	"ShowFees" bool NULL,
	"ShowIns" bool NULL,
	CONSTRAINT treatplanparam_pkey PRIMARY KEY ("TreatPlanParamNum")
);


-- raw.tsitranslog definition

-- Drop table

-- DROP TABLE raw.tsitranslog;

CREATE TABLE raw.tsitranslog (
	"TsiTransLogNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	"UserNum" int8 NULL,
	"TransType" bool NULL,
	"TransDateTime" timestamp NULL,
	"ServiceType" bool NULL,
	"ServiceCode" bool NULL,
	"TransAmt" float8 NULL,
	"AccountBalance" float8 NULL,
	"FKeyType" bool NULL,
	"FKey" int8 NULL,
	"RawMsgText" varchar(1000) NULL,
	"ClientId" varchar(25) NULL,
	"TransJson" text NULL,
	"ClinicNum" int8 NULL,
	"AggTransLogNum" int8 NULL,
	CONSTRAINT tsitranslog_pkey PRIMARY KEY ("TsiTransLogNum")
);


-- raw.ucum definition

-- Drop table

-- DROP TABLE raw.ucum;

CREATE TABLE raw.ucum (
	"UcumNum" int8 NOT NULL,
	"UcumCode" varchar(255) NULL,
	"Description" varchar(255) NULL,
	"IsInUse" bool NULL,
	CONSTRAINT ucum_pkey PRIMARY KEY ("UcumNum")
);


-- raw.updatehistory definition

-- Drop table

-- DROP TABLE raw.updatehistory;

CREATE TABLE raw.updatehistory (
	"UpdateHistoryNum" int8 NOT NULL,
	"DateTimeUpdated" timestamp NULL,
	"ProgramVersion" varchar(255) NULL,
	"Signature" text NULL,
	CONSTRAINT updatehistory_pkey PRIMARY KEY ("UpdateHistoryNum")
);


-- raw.userclinic definition

-- Drop table

-- DROP TABLE raw.userclinic;

CREATE TABLE raw.userclinic (
	"UserClinicNum" int8 NOT NULL,
	"UserNum" int8 NULL,
	"ClinicNum" int8 NULL,
	CONSTRAINT userclinic_pkey PRIMARY KEY ("UserClinicNum")
);


-- raw.usergroup definition

-- Drop table

-- DROP TABLE raw.usergroup;

CREATE TABLE raw.usergroup (
	"UserGroupNum" int8 NOT NULL,
	"Description" varchar(255) NULL,
	"UserGroupNumCEMT" int8 NULL,
	CONSTRAINT usergroup_pkey PRIMARY KEY ("UserGroupNum")
);


-- raw.usergroupattach definition

-- Drop table

-- DROP TABLE raw.usergroupattach;

CREATE TABLE raw.usergroupattach (
	"UserGroupAttachNum" int8 NOT NULL,
	"UserNum" int8 NULL,
	"UserGroupNum" int8 NULL,
	CONSTRAINT usergroupattach_pkey PRIMARY KEY ("UserGroupAttachNum")
);


-- raw.userod definition

-- Drop table

-- DROP TABLE raw.userod;

CREATE TABLE raw.userod (
	"UserNum" int8 NOT NULL,
	"UserName" varchar(255) NULL,
	"Password" varchar(255) NULL,
	"UserGroupNum" int8 NULL,
	"EmployeeNum" int8 NULL,
	"ClinicNum" int8 NULL,
	"ProvNum" int8 NULL,
	"IsHidden" bool NULL,
	"TaskListInBox" int8 NULL,
	"AnesthProvType" int4 NULL,
	"DefaultHidePopups" bool NULL,
	"PasswordIsStrong" bool NULL,
	"ClinicIsRestricted" bool NULL,
	"InboxHidePopups" bool NULL,
	"UserNumCEMT" int8 NULL,
	"DateTFail" timestamp NULL,
	"FailedAttempts" int2 NULL,
	"DomainUser" varchar(255) NULL,
	"IsPasswordResetRequired" bool NULL,
	"MobileWebPin" varchar(255) NULL,
	"MobileWebPinFailedAttempts" bool NULL,
	"DateTLastLogin" timestamp NULL,
	"EClipboardClinicalPin" varchar(128) NULL,
	"BadgeId" varchar(255) NULL,
	CONSTRAINT userod_pkey PRIMARY KEY ("UserNum")
);


-- raw.userodapptview definition

-- Drop table

-- DROP TABLE raw.userodapptview;

CREATE TABLE raw.userodapptview (
	"UserodApptViewNum" int8 NOT NULL,
	"UserNum" int8 NULL,
	"ClinicNum" int8 NULL,
	"ApptViewNum" int8 NULL,
	CONSTRAINT userodapptview_pkey PRIMARY KEY ("UserodApptViewNum")
);


-- raw.userodpref definition

-- Drop table

-- DROP TABLE raw.userodpref;

CREATE TABLE raw.userodpref (
	"UserOdPrefNum" int8 NOT NULL,
	"UserNum" int8 NULL,
	"Fkey" int8 NULL,
	"FkeyType" int2 NULL,
	"ValueString" text NULL,
	"ClinicNum" int8 NULL,
	CONSTRAINT userodpref_pkey PRIMARY KEY ("UserOdPrefNum")
);


-- raw.userquery definition

-- Drop table

-- DROP TABLE raw.userquery;

CREATE TABLE raw.userquery (
	"QueryNum" int8 NOT NULL,
	"Description" varchar(255) NULL,
	"FileName" varchar(255) NULL,
	"QueryText" text NULL,
	"IsReleased" bool NULL,
	"IsPromptSetup" bool NULL,
	"DefaultFormatRaw" bool NULL,
	CONSTRAINT userquery_pkey PRIMARY KEY ("QueryNum")
);


-- raw.userweb definition

-- Drop table

-- DROP TABLE raw.userweb;

CREATE TABLE raw.userweb (
	"UserWebNum" int8 NOT NULL,
	"FKey" int8 NULL,
	"FKeyType" bool NULL,
	"UserName" varchar(255) NULL,
	"Password" varchar(255) NULL,
	"PasswordResetCode" varchar(255) NULL,
	"RequireUserNameChange" bool NULL,
	"DateTimeLastLogin" timestamp NULL,
	"RequirePasswordChange" bool NULL,
	CONSTRAINT userweb_pkey PRIMARY KEY ("UserWebNum")
);


-- raw.utm definition

-- Drop table

-- DROP TABLE raw.utm;

CREATE TABLE raw.utm (
	"UtmNum" int8 NOT NULL,
	"CampaignName" varchar(500) NULL,
	"MediumInfo" varchar(500) NULL,
	"SourceInfo" varchar(500) NULL,
	CONSTRAINT utm_pkey PRIMARY KEY ("UtmNum")
);


-- raw.vaccineobs definition

-- Drop table

-- DROP TABLE raw.vaccineobs;

CREATE TABLE raw.vaccineobs (
	"VaccineObsNum" int8 NOT NULL,
	"VaccinePatNum" int8 NULL,
	"ValType" bool NULL,
	"IdentifyingCode" bool NULL,
	"ValReported" varchar(255) NULL,
	"ValCodeSystem" bool NULL,
	"VaccineObsNumGroup" int8 NULL,
	"UcumCode" varchar(255) NULL,
	"DateObs" date NULL,
	"MethodCode" varchar(255) NULL,
	CONSTRAINT vaccineobs_pkey PRIMARY KEY ("VaccineObsNum")
);


-- raw.vaccinepat definition

-- Drop table

-- DROP TABLE raw.vaccinepat;

CREATE TABLE raw.vaccinepat (
	"VaccinePatNum" int8 NOT NULL,
	"VaccineDefNum" int8 NULL,
	"DateTimeStart" timestamp NULL,
	"DateTimeEnd" timestamp NULL,
	"AdministeredAmt" float4 NULL,
	"DrugUnitNum" int8 NULL,
	"LotNumber" varchar(255) NULL,
	"PatNum" int8 NULL,
	"Note" text NULL,
	"FilledCity" varchar(255) NULL,
	"FilledST" varchar(255) NULL,
	"CompletionStatus" bool NULL,
	"AdministrationNoteCode" bool NULL,
	"UserNum" int8 NULL,
	"ProvNumOrdering" int8 NULL,
	"ProvNumAdminister" int8 NULL,
	"DateExpire" date NULL,
	"RefusalReason" bool NULL,
	"ActionCode" bool NULL,
	"AdministrationRoute" bool NULL,
	"AdministrationSite" bool NULL,
	CONSTRAINT vaccinepat_pkey PRIMARY KEY ("VaccinePatNum")
);


-- raw.vitalsign definition

-- Drop table

-- DROP TABLE raw.vitalsign;

CREATE TABLE raw.vitalsign (
	"VitalsignNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	"Height" float4 NULL,
	"Weight" float4 NULL,
	"BpSystolic" int2 NULL,
	"BpDiastolic" int2 NULL,
	"DateTaken" date NULL,
	"HasFollowupPlan" bool NULL,
	"IsIneligible" bool NULL,
	"Documentation" text NULL,
	"ChildGotNutrition" bool NULL,
	"ChildGotPhysCouns" bool NULL,
	"WeightCode" varchar(255) NULL,
	"HeightExamCode" varchar(30) NULL,
	"WeightExamCode" varchar(30) NULL,
	"BMIExamCode" varchar(30) NULL,
	"EhrNotPerformedNum" int8 NULL,
	"PregDiseaseNum" int8 NULL,
	"BMIPercentile" int4 NULL,
	"Pulse" int4 NULL,
	CONSTRAINT vitalsign_pkey PRIMARY KEY ("VitalsignNum")
);


-- raw.webschedcarrierrule definition

-- Drop table

-- DROP TABLE raw.webschedcarrierrule;

CREATE TABLE raw.webschedcarrierrule (
	"WebSchedCarrierRuleNum" int8 NOT NULL,
	"ClinicNum" int8 NULL,
	"CarrierName" varchar(255) NULL,
	"DisplayName" varchar(255) NULL,
	"Message" text NULL,
	"Rule" bool NULL,
	CONSTRAINT webschedcarrierrule_pkey PRIMARY KEY ("WebSchedCarrierRuleNum")
);


-- raw.webschedrecall definition

-- Drop table

-- DROP TABLE raw.webschedrecall;

CREATE TABLE raw.webschedrecall (
	"WebSchedRecallNum" int8 NOT NULL,
	"ClinicNum" int8 NULL,
	"PatNum" int8 NULL,
	"RecallNum" int8 NULL,
	"DateTimeEntry" timestamp NULL,
	"DateDue" timestamp NULL,
	"ReminderCount" int4 NULL,
	"DateTimeSent" timestamp NULL,
	"DateTimeSendFailed" timestamp NULL,
	"SendStatus" bool NULL,
	"ShortGUID" varchar(255) NULL,
	"ResponseDescript" text NULL,
	"Source" bool NULL,
	"CommlogNum" int8 NULL,
	"MessageType" bool NULL,
	"MessageFk" int8 NULL,
	"ApptReminderRuleNum" int8 NULL,
	CONSTRAINT webschedrecall_pkey PRIMARY KEY ("WebSchedRecallNum")
);


-- raw.wikilistheaderwidth definition

-- Drop table

-- DROP TABLE raw.wikilistheaderwidth;

CREATE TABLE raw.wikilistheaderwidth (
	"WikiListHeaderWidthNum" int8 NOT NULL,
	"ListName" varchar(255) NULL,
	"ColName" varchar(255) NULL,
	"ColWidth" int4 NULL,
	"PickList" text NULL,
	"IsHidden" bool NULL,
	CONSTRAINT wikilistheaderwidth_pkey PRIMARY KEY ("WikiListHeaderWidthNum")
);


-- raw.wikilisthist definition

-- Drop table

-- DROP TABLE raw.wikilisthist;

CREATE TABLE raw.wikilisthist (
	"WikiListHistNum" int8 NOT NULL,
	"UserNum" int8 NULL,
	"ListName" varchar(255) NULL,
	"ListHeaders" text NULL,
	"ListContent" text NULL,
	"DateTimeSaved" timestamp NULL,
	CONSTRAINT wikilisthist_pkey PRIMARY KEY ("WikiListHistNum")
);


-- raw.wikipage definition

-- Drop table

-- DROP TABLE raw.wikipage;

CREATE TABLE raw.wikipage (
	"WikiPageNum" int8 NOT NULL,
	"UserNum" int8 NULL,
	"PageTitle" varchar(255) NULL,
	"KeyWords" varchar(255) NULL,
	"PageContent" text NULL,
	"DateTimeSaved" timestamp NULL,
	"IsDraft" bool NULL,
	"IsLocked" bool NULL,
	"IsDeleted" bool NULL,
	"PageContentPlainText" text NULL,
	CONSTRAINT wikipage_pkey PRIMARY KEY ("WikiPageNum")
);


-- raw.wikipagehist definition

-- Drop table

-- DROP TABLE raw.wikipagehist;

CREATE TABLE raw.wikipagehist (
	"WikiPageNum" int8 NOT NULL,
	"UserNum" int8 NULL,
	"PageTitle" varchar(255) NULL,
	"PageContent" text NULL,
	"DateTimeSaved" timestamp NULL,
	"IsDeleted" bool NULL,
	CONSTRAINT wikipagehist_pkey PRIMARY KEY ("WikiPageNum")
);


-- raw.xchargetransaction definition

-- Drop table

-- DROP TABLE raw.xchargetransaction;

CREATE TABLE raw.xchargetransaction (
	"XChargeTransactionNum" int8 NOT NULL,
	"TransType" varchar(255) NULL,
	"Amount" float8 NULL,
	"CCEntry" varchar(255) NULL,
	"PatNum" int8 NULL,
	"Result" varchar(255) NULL,
	"ClerkID" varchar(255) NULL,
	"ResultCode" varchar(255) NULL,
	"Expiration" varchar(255) NULL,
	"CCType" varchar(255) NULL,
	"CreditCardNum" varchar(255) NULL,
	"BatchNum" varchar(255) NULL,
	"ItemNum" varchar(255) NULL,
	"ApprCode" varchar(255) NULL,
	"TransactionDateTime" timestamp NULL,
	"BatchTotal" float8 NULL,
	CONSTRAINT xchargetransaction_pkey PRIMARY KEY ("XChargeTransactionNum")
);


-- raw.xwebresponse definition

-- Drop table

-- DROP TABLE raw.xwebresponse;

CREATE TABLE raw.xwebresponse (
	"XWebResponseNum" int8 NOT NULL,
	"PatNum" int8 NULL,
	"ProvNum" int8 NULL,
	"ClinicNum" int8 NULL,
	"PaymentNum" int8 NULL,
	"DateTEntry" timestamp NULL,
	"DateTUpdate" timestamp NULL,
	"TransactionStatus" bool NULL,
	"ResponseCode" int4 NULL,
	"XWebResponseCode" varchar(255) NULL,
	"ResponseDescription" varchar(255) NULL,
	"OTK" varchar(255) NULL,
	"HpfUrl" text NULL,
	"HpfExpiration" timestamp NULL,
	"TransactionID" varchar(255) NULL,
	"TransactionType" varchar(255) NULL,
	"Alias" varchar(255) NULL,
	"CardType" varchar(255) NULL,
	"CardBrand" varchar(255) NULL,
	"CardBrandShort" varchar(255) NULL,
	"MaskedAcctNum" varchar(255) NULL,
	"Amount" float8 NULL,
	"ApprovalCode" varchar(255) NULL,
	"CardCodeResponse" varchar(255) NULL,
	"ReceiptID" int4 NULL,
	"ExpDate" varchar(255) NULL,
	"EntryMethod" varchar(255) NULL,
	"ProcessorResponse" varchar(255) NULL,
	"BatchNum" int4 NULL,
	"BatchAmount" float8 NULL,
	"AccountExpirationDate" date NULL,
	"DebugError" text NULL,
	"PayNote" text NULL,
	"CCSource" bool NULL,
	"OrderId" varchar(255) NULL,
	"EmailResponse" varchar(255) NULL,
	"LogGuid" varchar(36) NULL,
	CONSTRAINT xwebresponse_pkey PRIMARY KEY ("XWebResponseNum")
);


-- raw.zipcode definition

-- Drop table

-- DROP TABLE raw.zipcode;

CREATE TABLE raw.zipcode (
	"ZipCodeNum" int8 NOT NULL,
	"ZipCodeDigits" varchar(20) NULL,
	"City" varchar(100) NULL,
	"State" varchar(20) NULL,
	"IsFrequent" bool NULL,
	CONSTRAINT zipcode_pkey PRIMARY KEY ("ZipCodeNum")
);



-- Note: All trigram functions are provided by pg_trgm extension, no need to create manually

-- DROP FUNCTION raw.gin_extract_query_trgm(text, internal, int2, internal, internal, internal, internal);

-- CREATE OR REPLACE FUNCTION raw.gin_extract_query_trgm(text, internal, smallint, internal, internal, internal, internal)
--  RETURNS internal
--  LANGUAGE c
--  IMMUTABLE PARALLEL SAFE STRICT
-- AS '$libdir/pg_trgm', $function$gin_extract_query_trgm$function$
-- ;

-- DROP FUNCTION raw.gin_extract_value_trgm(text, internal);

-- CREATE OR REPLACE FUNCTION raw.gin_extract_value_trgm(text, internal)
 RETURNS internal
 LANGUAGE c
 IMMUTABLE PARALLEL SAFE STRICT
AS '$libdir/pg_trgm', $function$gin_extract_value_trgm$function$
;

-- DROP FUNCTION raw.gin_trgm_consistent(internal, int2, text, int4, internal, internal, internal, internal);

CREATE OR REPLACE FUNCTION raw.gin_trgm_consistent(internal, smallint, text, integer, internal, internal, internal, internal)
 RETURNS boolean
 LANGUAGE c
 IMMUTABLE PARALLEL SAFE STRICT
AS '$libdir/pg_trgm', $function$gin_trgm_consistent$function$
;

-- DROP FUNCTION raw.gin_trgm_triconsistent(internal, int2, text, int4, internal, internal, internal);

CREATE OR REPLACE FUNCTION raw.gin_trgm_triconsistent(internal, smallint, text, integer, internal, internal, internal)
 RETURNS "char"
 LANGUAGE c
 IMMUTABLE PARALLEL SAFE STRICT
AS '$libdir/pg_trgm', $function$gin_trgm_triconsistent$function$
;

-- DROP FUNCTION raw.gtrgm_compress(internal);

CREATE OR REPLACE FUNCTION raw.gtrgm_compress(internal)
 RETURNS internal
 LANGUAGE c
 IMMUTABLE PARALLEL SAFE STRICT
AS '$libdir/pg_trgm', $function$gtrgm_compress$function$
;

-- DROP FUNCTION raw.gtrgm_consistent(internal, text, int2, oid, internal);

CREATE OR REPLACE FUNCTION raw.gtrgm_consistent(internal, text, smallint, oid, internal)
 RETURNS boolean
 LANGUAGE c
 IMMUTABLE PARALLEL SAFE STRICT
AS '$libdir/pg_trgm', $function$gtrgm_consistent$function$
;

-- DROP FUNCTION raw.gtrgm_decompress(internal);

CREATE OR REPLACE FUNCTION raw.gtrgm_decompress(internal)
 RETURNS internal
 LANGUAGE c
 IMMUTABLE PARALLEL SAFE STRICT
AS '$libdir/pg_trgm', $function$gtrgm_decompress$function$
;

-- DROP FUNCTION raw.gtrgm_distance(internal, text, int2, oid, internal);

CREATE OR REPLACE FUNCTION raw.gtrgm_distance(internal, text, smallint, oid, internal)
 RETURNS double precision
 LANGUAGE c
 IMMUTABLE PARALLEL SAFE STRICT
AS '$libdir/pg_trgm', $function$gtrgm_distance$function$
;

-- DROP FUNCTION raw.gtrgm_in(cstring);

CREATE OR REPLACE FUNCTION raw.gtrgm_in(cstring)
 RETURNS raw.gtrgm
 LANGUAGE c
 IMMUTABLE PARALLEL SAFE STRICT
AS '$libdir/pg_trgm', $function$gtrgm_in$function$
;

-- DROP FUNCTION raw.gtrgm_options(internal);

CREATE OR REPLACE FUNCTION raw.gtrgm_options(internal)
 RETURNS void
 LANGUAGE c
 IMMUTABLE PARALLEL SAFE
AS '$libdir/pg_trgm', $function$gtrgm_options$function$
;

-- DROP FUNCTION raw.gtrgm_out(raw.gtrgm);

CREATE OR REPLACE FUNCTION raw.gtrgm_out(raw.gtrgm)
 RETURNS cstring
 LANGUAGE c
 IMMUTABLE PARALLEL SAFE STRICT
AS '$libdir/pg_trgm', $function$gtrgm_out$function$
;

-- DROP FUNCTION raw.gtrgm_penalty(internal, internal, internal);

CREATE OR REPLACE FUNCTION raw.gtrgm_penalty(internal, internal, internal)
 RETURNS internal
 LANGUAGE c
 IMMUTABLE PARALLEL SAFE STRICT
AS '$libdir/pg_trgm', $function$gtrgm_penalty$function$
;

-- DROP FUNCTION raw.gtrgm_picksplit(internal, internal);

CREATE OR REPLACE FUNCTION raw.gtrgm_picksplit(internal, internal)
 RETURNS internal
 LANGUAGE c
 IMMUTABLE PARALLEL SAFE STRICT
AS '$libdir/pg_trgm', $function$gtrgm_picksplit$function$
;

-- DROP FUNCTION raw.gtrgm_same(raw.gtrgm, raw.gtrgm, internal);

CREATE OR REPLACE FUNCTION raw.gtrgm_same(raw.gtrgm, raw.gtrgm, internal)
 RETURNS internal
 LANGUAGE c
 IMMUTABLE PARALLEL SAFE STRICT
AS '$libdir/pg_trgm', $function$gtrgm_same$function$
;

-- DROP FUNCTION raw.gtrgm_union(internal, internal);

CREATE OR REPLACE FUNCTION raw.gtrgm_union(internal, internal)
 RETURNS raw.gtrgm
 LANGUAGE c
 IMMUTABLE PARALLEL SAFE STRICT
AS '$libdir/pg_trgm', $function$gtrgm_union$function$
;

-- DROP FUNCTION raw.set_limit(float4);

CREATE OR REPLACE FUNCTION raw.set_limit(real)
 RETURNS real
 LANGUAGE c
 STRICT
AS '$libdir/pg_trgm', $function$set_limit$function$
;

-- DROP FUNCTION raw.show_limit();

CREATE OR REPLACE FUNCTION raw.show_limit()
 RETURNS real
 LANGUAGE c
 STABLE PARALLEL SAFE STRICT
AS '$libdir/pg_trgm', $function$show_limit$function$
;

-- DROP FUNCTION raw.show_trgm(text);

CREATE OR REPLACE FUNCTION raw.show_trgm(text)
 RETURNS text[]
 LANGUAGE c
 IMMUTABLE PARALLEL SAFE STRICT
AS '$libdir/pg_trgm', $function$show_trgm$function$
;

-- DROP FUNCTION raw.similarity(text, text);

CREATE OR REPLACE FUNCTION raw.similarity(text, text)
 RETURNS real
 LANGUAGE c
 IMMUTABLE PARALLEL SAFE STRICT
AS '$libdir/pg_trgm', $function$similarity$function$
;

-- DROP FUNCTION raw.similarity_dist(text, text);

CREATE OR REPLACE FUNCTION raw.similarity_dist(text, text)
 RETURNS real
 LANGUAGE c
 IMMUTABLE PARALLEL SAFE STRICT
AS '$libdir/pg_trgm', $function$similarity_dist$function$
;

-- DROP FUNCTION raw.similarity_op(text, text);

CREATE OR REPLACE FUNCTION raw.similarity_op(text, text)
 RETURNS boolean
 LANGUAGE c
 STABLE PARALLEL SAFE STRICT
AS '$libdir/pg_trgm', $function$similarity_op$function$
;

-- DROP FUNCTION raw.strict_word_similarity(text, text);

CREATE OR REPLACE FUNCTION raw.strict_word_similarity(text, text)
 RETURNS real
 LANGUAGE c
 IMMUTABLE PARALLEL SAFE STRICT
AS '$libdir/pg_trgm', $function$strict_word_similarity$function$
;

-- DROP FUNCTION raw.strict_word_similarity_commutator_op(text, text);

CREATE OR REPLACE FUNCTION raw.strict_word_similarity_commutator_op(text, text)
 RETURNS boolean
 LANGUAGE c
 STABLE PARALLEL SAFE STRICT
AS '$libdir/pg_trgm', $function$strict_word_similarity_commutator_op$function$
;

-- DROP FUNCTION raw.strict_word_similarity_dist_commutator_op(text, text);

CREATE OR REPLACE FUNCTION raw.strict_word_similarity_dist_commutator_op(text, text)
 RETURNS real
 LANGUAGE c
 IMMUTABLE PARALLEL SAFE STRICT
AS '$libdir/pg_trgm', $function$strict_word_similarity_dist_commutator_op$function$
;

-- DROP FUNCTION raw.strict_word_similarity_dist_op(text, text);

CREATE OR REPLACE FUNCTION raw.strict_word_similarity_dist_op(text, text)
 RETURNS real
 LANGUAGE c
 IMMUTABLE PARALLEL SAFE STRICT
AS '$libdir/pg_trgm', $function$strict_word_similarity_dist_op$function$
;

-- DROP FUNCTION raw.strict_word_similarity_op(text, text);

CREATE OR REPLACE FUNCTION raw.strict_word_similarity_op(text, text)
 RETURNS boolean
 LANGUAGE c
 STABLE PARALLEL SAFE STRICT
AS '$libdir/pg_trgm', $function$strict_word_similarity_op$function$
;

-- DROP FUNCTION raw.word_similarity(text, text);

CREATE OR REPLACE FUNCTION raw.word_similarity(text, text)
 RETURNS real
 LANGUAGE c
 IMMUTABLE PARALLEL SAFE STRICT
AS '$libdir/pg_trgm', $function$word_similarity$function$
;

-- DROP FUNCTION raw.word_similarity_commutator_op(text, text);

CREATE OR REPLACE FUNCTION raw.word_similarity_commutator_op(text, text)
 RETURNS boolean
 LANGUAGE c
 STABLE PARALLEL SAFE STRICT
AS '$libdir/pg_trgm', $function$word_similarity_commutator_op$function$
;

-- DROP FUNCTION raw.word_similarity_dist_commutator_op(text, text);

CREATE OR REPLACE FUNCTION raw.word_similarity_dist_commutator_op(text, text)
 RETURNS real
 LANGUAGE c
 IMMUTABLE PARALLEL SAFE STRICT
AS '$libdir/pg_trgm', $function$word_similarity_dist_commutator_op$function$
;

-- DROP FUNCTION raw.word_similarity_dist_op(text, text);

CREATE OR REPLACE FUNCTION raw.word_similarity_dist_op(text, text)
 RETURNS real
 LANGUAGE c
 IMMUTABLE PARALLEL SAFE STRICT
AS '$libdir/pg_trgm', $function$word_similarity_dist_op$function$
;

-- DROP FUNCTION raw.word_similarity_op(text, text);

CREATE OR REPLACE FUNCTION raw.word_similarity_op(text, text)
 RETURNS boolean
 LANGUAGE c
 STABLE PARALLEL SAFE STRICT
AS '$libdir/pg_trgm', $function$word_similarity_op$function$
;
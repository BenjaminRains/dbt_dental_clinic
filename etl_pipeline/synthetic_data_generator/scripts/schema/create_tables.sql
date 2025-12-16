--
-- PostgreSQL database dump
--

-- Dumped from database version 17.4
-- Dumped by pg_dump version 17.4

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: raw; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA raw;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: account; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.account (
    "AccountNum" bigint NOT NULL,
    "Description" character varying(255),
    "AcctType" smallint,
    "BankNumber" character varying(255),
    "Inactive" boolean,
    "AccountColor" integer,
    "IsRetainedEarnings" boolean
);


--
-- Name: accountingautopay; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.accountingautopay (
    "AccountingAutoPayNum" bigint NOT NULL,
    "PayType" bigint,
    "PickList" character varying(255)
);


--
-- Name: activeinstance; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.activeinstance (
    "ActiveInstanceNum" bigint NOT NULL,
    "ComputerNum" bigint,
    "UserNum" bigint,
    "ProcessId" bigint,
    "DateTimeLastActive" timestamp without time zone,
    "DateTRecorded" timestamp without time zone,
    "ConnectionType" boolean
);


--
-- Name: adjustment; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.adjustment (
    "AdjNum" bigint NOT NULL,
    "AdjDate" date,
    "AdjAmt" double precision,
    "PatNum" bigint,
    "AdjType" bigint,
    "ProvNum" bigint,
    "AdjNote" text,
    "ProcDate" date,
    "ProcNum" bigint,
    "DateEntry" date,
    "ClinicNum" bigint,
    "StatementNum" bigint,
    "SecUserNumEntry" bigint,
    "SecDateTEdit" timestamp without time zone,
    "TaxTransID" bigint
);


--
-- Name: alertcategory; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.alertcategory (
    "AlertCategoryNum" bigint NOT NULL,
    "IsHQCategory" boolean,
    "InternalName" character varying(255),
    "Description" character varying(255)
);


--
-- Name: alertcategorylink; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.alertcategorylink (
    "AlertCategoryLinkNum" bigint NOT NULL,
    "AlertCategoryNum" bigint,
    "AlertType" smallint
);


--
-- Name: alertitem; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.alertitem (
    "AlertItemNum" bigint NOT NULL,
    "ClinicNum" bigint,
    "Description" character varying(2000),
    "Type" smallint,
    "Severity" boolean,
    "Actions" smallint,
    "FormToOpen" smallint,
    "FKey" bigint,
    "ItemValue" character varying(4000),
    "UserNum" bigint,
    "SecDateTEntry" timestamp without time zone
);


--
-- Name: alertread; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.alertread (
    "AlertReadNum" bigint NOT NULL,
    "AlertItemNum" bigint,
    "UserNum" bigint
);


--
-- Name: alertsub; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.alertsub (
    "AlertSubNum" bigint NOT NULL,
    "UserNum" bigint,
    "ClinicNum" bigint,
    "Type" boolean,
    "AlertCategoryNum" bigint
);


--
-- Name: allergy; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.allergy (
    "AllergyNum" bigint NOT NULL,
    "AllergyDefNum" bigint,
    "PatNum" bigint,
    "Reaction" character varying(255),
    "StatusIsActive" boolean,
    "DateTStamp" timestamp without time zone,
    "DateAdverseReaction" date,
    "SnomedReaction" character varying(255)
);


--
-- Name: allergydef; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.allergydef (
    "AllergyDefNum" bigint NOT NULL,
    "Description" character varying(255),
    "IsHidden" boolean,
    "DateTStamp" timestamp without time zone,
    "SnomedType" smallint,
    "MedicationNum" bigint,
    "UniiCode" character varying(255)
);


--
-- Name: anestheticdata; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.anestheticdata (
    "AnestheticDataNum" integer NOT NULL,
    "AnestheticRecordNum" integer,
    "AnesthOpen" character(32),
    "AnesthClose" character(32),
    "SurgOpen" character(32),
    "SurgClose" character(32),
    "Anesthetist" character(32),
    "Surgeon" character(32),
    "Asst" character(32),
    "Circulator" character(32),
    "VSMName" character(20),
    "VSMSerNum" character(20),
    "ASA" character(3),
    "ASA_EModifier" character(1),
    "O2LMin" smallint,
    "N2OLMin" smallint,
    "RteNasCan" boolean,
    "RteNasHood" boolean,
    "RteETT" boolean,
    "MedRouteIVCath" boolean,
    "MedRouteIVButtFly" boolean,
    "MedRouteIM" boolean,
    "MedRoutePO" boolean,
    "MedRouteNasal" boolean,
    "MedRouteRectal" boolean,
    "IVSite" character(20),
    "IVGauge" smallint,
    "IVSideR" smallint,
    "IVSideL" smallint,
    "IVAtt" smallint,
    "IVF" character(20),
    "IVFVol" real,
    "MonBP" boolean,
    "MonSpO2" boolean,
    "MonEtCO2" boolean,
    "MonTemp" boolean,
    "MonPrecordial" boolean,
    "MonEKG" boolean,
    "Notes" text,
    "PatWgt" smallint,
    "WgtUnitsLbs" boolean,
    "WgtUnitsKgs" boolean,
    "PatHgt" smallint,
    "EscortName" character(32),
    "EscortCellNum" character(13),
    "EscortRel" character(16),
    "NPOTime" character(5),
    "HgtUnitsIn" boolean,
    "HgtUnitsCm" boolean,
    "Signature" text,
    "SigIsTopaz" boolean
);


--
-- Name: anestheticrecord; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.anestheticrecord (
    "AnestheticRecordNum" integer NOT NULL,
    "PatNum" integer,
    "AnestheticDate" timestamp without time zone,
    "ProvNum" smallint
);


--
-- Name: anesthmedsgiven; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.anesthmedsgiven (
    "AnestheticMedNum" integer NOT NULL,
    "AnestheticRecordNum" integer,
    "AnesthMedName" character(32),
    "QtyGiven" double precision,
    "QtyWasted" double precision,
    "DoseTimeStamp" character(32),
    "QtyOnHandOld" double precision,
    "AnesthMedNum" integer
);


--
-- Name: anesthmedsintake; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.anesthmedsintake (
    "AnestheticMedNum" integer NOT NULL,
    "IntakeDate" timestamp without time zone,
    "AnesthMedName" character(32),
    "Qty" integer,
    "SupplierIDNum" character(11),
    "InvoiceNum" character(20)
);


--
-- Name: anesthmedsinventory; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.anesthmedsinventory (
    "AnestheticMedNum" integer NOT NULL,
    "AnesthMedName" character(30),
    "AnesthHowSupplied" character(20),
    "QtyOnHand" double precision,
    "DEASchedule" character(3)
);


--
-- Name: anesthmedsinventoryadj; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.anesthmedsinventoryadj (
    "AdjustNum" integer NOT NULL,
    "AnestheticMedNum" integer,
    "QtyAdj" double precision,
    "UserNum" integer,
    "Notes" character varying(255),
    "TimeStamp" timestamp without time zone
);


--
-- Name: anesthmedsuppliers; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.anesthmedsuppliers (
    "SupplierIDNum" integer NOT NULL,
    "SupplierName" character varying(255),
    "Phone" character(13),
    "PhoneExt" character(6),
    "Fax" character(13),
    "Addr1" character varying(48),
    "Addr2" character(32),
    "City" character varying(48),
    "State" character(20),
    "Zip" character(10),
    "Contact" character(32),
    "WebSite" character varying(48),
    "Notes" text
);


--
-- Name: anesthscore; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.anesthscore (
    "AnesthScoreNum" integer NOT NULL,
    "AnestheticRecordNum" integer,
    "QActivity" smallint,
    "QResp" smallint,
    "QCirc" smallint,
    "QConc" smallint,
    "QColor" smallint,
    "AnesthesiaScore" smallint,
    "DischAmb" boolean,
    "DischWheelChr" boolean,
    "DischAmbulance" boolean,
    "DischCondStable" boolean,
    "DischCondUnStable" boolean
);


--
-- Name: anesthvsdata; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.anesthvsdata (
    "AnesthVSDataNum" integer NOT NULL,
    "AnestheticRecordNum" integer,
    "PatNum" integer,
    "VSMName" character(20),
    "VSMSerNum" character(32),
    "BPSys" smallint,
    "BPDias" smallint,
    "BPMAP" smallint,
    "HR" smallint,
    "SpO2" smallint,
    "EtCo2" smallint,
    "Temp" smallint,
    "VSTimeStamp" character(32),
    "MessageID" character varying(50),
    "HL7Message" text
);


--
-- Name: apikey; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.apikey (
    "APIKeyNum" bigint NOT NULL,
    "CustApiKey" character varying(255),
    "DevName" character varying(255)
);


--
-- Name: apisubscription; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.apisubscription (
    "ApiSubscriptionNum" bigint NOT NULL,
    "EndPointUrl" character varying(255),
    "Workstation" character varying(255),
    "CustomerKey" character varying(255),
    "WatchTable" character varying(255),
    "PollingSeconds" integer,
    "UiEventType" character varying(255),
    "DateTimeStart" timestamp without time zone,
    "DateTimeStop" timestamp without time zone,
    "Note" character varying(255)
);


--
-- Name: appointment; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.appointment (
    "AptNum" bigint NOT NULL,
    "PatNum" bigint,
    "AptStatus" smallint,
    "Pattern" character varying(255),
    "Confirmed" bigint,
    "TimeLocked" boolean,
    "Op" bigint,
    "Note" text,
    "ProvNum" bigint,
    "ProvHyg" bigint,
    "AptDateTime" timestamp without time zone,
    "NextAptNum" bigint,
    "UnschedStatus" bigint,
    "IsNewPatient" boolean,
    "ProcDescript" text,
    "Assistant" bigint,
    "ClinicNum" bigint,
    "IsHygiene" boolean,
    "DateTStamp" timestamp without time zone,
    "DateTimeArrived" timestamp without time zone,
    "DateTimeSeated" timestamp without time zone,
    "DateTimeDismissed" timestamp without time zone,
    "InsPlan1" bigint,
    "InsPlan2" bigint,
    "DateTimeAskedToArrive" timestamp without time zone,
    "ProcsColored" text,
    "ColorOverride" integer,
    "AppointmentTypeNum" bigint,
    "SecUserNumEntry" bigint,
    "SecDateTEntry" timestamp without time zone,
    "Priority" boolean,
    "ProvBarText" character varying(60),
    "PatternSecondary" character varying(255),
    "SecurityHash" character varying(255),
    "ItemOrderPlanned" integer,
    "IsMirrored" boolean
);


--
-- Name: appointment_AptNum_seq; Type: SEQUENCE; Schema: raw; Owner: -
--

CREATE SEQUENCE raw."appointment_AptNum_seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: appointmentrule; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.appointmentrule (
    "AppointmentRuleNum" bigint NOT NULL,
    "RuleDesc" character varying(255),
    "CodeStart" character varying(15),
    "CodeEnd" character varying(15),
    "IsEnabled" boolean
);


--
-- Name: appointmenttype; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.appointmenttype (
    "AppointmentTypeNum" bigint NOT NULL,
    "AppointmentTypeName" character varying(255),
    "AppointmentTypeColor" integer,
    "ItemOrder" integer,
    "IsHidden" boolean,
    "Pattern" character varying(255),
    "CodeStr" character varying(4000),
    "CodeStrRequired" character varying(4000),
    "RequiredProcCodesNeeded" smallint,
    "BlockoutTypes" character varying(255)
);


--
-- Name: apptfield; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.apptfield (
    "ApptFieldNum" bigint NOT NULL,
    "AptNum" bigint,
    "FieldName" character varying(255),
    "FieldValue" text
);


--
-- Name: apptfielddef; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.apptfielddef (
    "ApptFieldDefNum" bigint NOT NULL,
    "FieldName" character varying(255),
    "FieldType" boolean,
    "PickList" text,
    "ItemOrder" integer
);


--
-- Name: apptgeneralmessagesent; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.apptgeneralmessagesent (
    "ApptGeneralMessageSentNum" bigint NOT NULL,
    "ApptNum" bigint,
    "PatNum" bigint,
    "ClinicNum" bigint,
    "DateTimeEntry" timestamp without time zone,
    "TSPrior" bigint,
    "ApptReminderRuleNum" bigint,
    "SendStatus" smallint,
    "ApptDateTime" timestamp without time zone,
    "MessageType" smallint,
    "MessageFk" bigint,
    "DateTimeSent" timestamp without time zone,
    "ResponseDescript" text
);


--
-- Name: apptnewpatthankyousent; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.apptnewpatthankyousent (
    "ApptNewPatThankYouSentNum" bigint NOT NULL,
    "ApptNum" bigint,
    "ApptDateTime" timestamp without time zone,
    "ApptSecDateTEntry" timestamp without time zone,
    "TSPrior" bigint,
    "ApptReminderRuleNum" bigint,
    "ClinicNum" bigint,
    "PatNum" bigint,
    "ResponseDescript" text,
    "DateTimeNewPatThankYouTransmit" timestamp without time zone,
    "ShortGUID" character varying(255),
    "SendStatus" boolean,
    "MessageType" boolean,
    "MessageFk" bigint,
    "DateTimeEntry" timestamp without time zone,
    "DateTimeSent" timestamp without time zone
);


--
-- Name: apptreminderrule; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.apptreminderrule (
    "ApptReminderRuleNum" bigint NOT NULL,
    "TypeCur" smallint,
    "TSPrior" bigint,
    "SendOrder" character varying(255),
    "IsSendAll" boolean,
    "TemplateSMS" text,
    "TemplateEmailSubject" text,
    "TemplateEmail" text,
    "ClinicNum" bigint,
    "TemplateSMSAggShared" text,
    "TemplateSMSAggPerAppt" text,
    "TemplateEmailSubjAggShared" text,
    "TemplateEmailAggShared" text,
    "TemplateEmailAggPerAppt" text,
    "DoNotSendWithin" bigint,
    "IsEnabled" boolean,
    "TemplateAutoReply" text,
    "TemplateAutoReplyAgg" text,
    "IsAutoReplyEnabled" boolean,
    "Language" character varying(255),
    "TemplateComeInMessage" text,
    "EmailTemplateType" character varying(255),
    "AggEmailTemplateType" character varying(255),
    "IsSendForMinorsBirthday" boolean,
    "EmailHostingTemplateNum" bigint,
    "MinorAge" integer,
    "TemplateFailureAutoReply" text,
    "SendMultipleInvites" boolean,
    "TimeSpanMultipleInvites" bigint
);


--
-- Name: apptremindersent; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.apptremindersent (
    "ApptReminderSentNum" bigint NOT NULL,
    "ApptNum" bigint,
    "ApptDateTime" timestamp without time zone,
    "DateTimeSent" timestamp without time zone,
    "TSPrior" bigint,
    "ApptReminderRuleNum" bigint,
    "PatNum" bigint,
    "ClinicNum" bigint,
    "SendStatus" boolean,
    "MessageType" boolean,
    "MessageFk" bigint,
    "DateTimeEntry" timestamp without time zone,
    "ResponseDescript" text
);


--
-- Name: apptthankyousent; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.apptthankyousent (
    "ApptThankYouSentNum" bigint NOT NULL,
    "ApptNum" bigint,
    "ApptDateTime" timestamp without time zone,
    "ApptSecDateTEntry" timestamp without time zone,
    "TSPrior" bigint,
    "ApptReminderRuleNum" bigint,
    "ClinicNum" bigint,
    "PatNum" bigint,
    "ResponseDescript" text,
    "DateTimeThankYouTransmit" timestamp without time zone,
    "ShortGUID" character varying(255),
    "SendStatus" smallint,
    "DoNotResend" boolean,
    "MessageType" smallint,
    "MessageFk" bigint,
    "DateTimeEntry" timestamp without time zone,
    "DateTimeSent" timestamp without time zone
);


--
-- Name: apptview; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.apptview (
    "ApptViewNum" bigint NOT NULL,
    "Description" character varying(255),
    "ItemOrder" smallint,
    "RowsPerIncr" boolean,
    "OnlyScheduledProvs" boolean,
    "OnlySchedBeforeTime" time without time zone,
    "OnlySchedAfterTime" time without time zone,
    "StackBehavUR" smallint,
    "StackBehavLR" smallint,
    "ClinicNum" bigint,
    "ApptTimeScrollStart" time without time zone,
    "IsScrollStartDynamic" boolean,
    "IsApptBubblesDisabled" boolean,
    "WidthOpMinimum" smallint,
    "WaitingRmName" boolean,
    "OnlyScheduledProvDays" boolean
);


--
-- Name: apptviewitem; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.apptviewitem (
    "ApptViewItemNum" bigint NOT NULL,
    "ApptViewNum" bigint,
    "OpNum" bigint,
    "ProvNum" bigint,
    "ElementDesc" character varying(255),
    "ElementOrder" smallint,
    "ElementColor" integer,
    "ElementAlignment" smallint,
    "ApptFieldDefNum" bigint,
    "PatFieldDefNum" bigint,
    "IsMobile" boolean
);


--
-- Name: asapcomm; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.asapcomm (
    "AsapCommNum" bigint NOT NULL,
    "FKey" bigint,
    "FKeyType" boolean,
    "ScheduleNum" bigint,
    "PatNum" bigint,
    "ClinicNum" bigint,
    "ShortGUID" character varying(255),
    "DateTimeEntry" timestamp without time zone,
    "DateTimeExpire" timestamp without time zone,
    "DateTimeSmsScheduled" timestamp without time zone,
    "SmsSendStatus" boolean,
    "EmailSendStatus" smallint,
    "DateTimeSmsSent" timestamp without time zone,
    "DateTimeEmailSent" timestamp without time zone,
    "EmailMessageNum" bigint,
    "ResponseStatus" smallint,
    "DateTimeOrig" timestamp without time zone,
    "TemplateText" text,
    "TemplateEmail" text,
    "TemplateEmailSubj" character varying(100),
    "Note" text,
    "GuidMessageToMobile" text,
    "EmailTemplateType" character varying(255)
);


--
-- Name: autocode; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.autocode (
    "AutoCodeNum" bigint NOT NULL,
    "Description" character varying(255),
    "IsHidden" boolean,
    "LessIntrusive" boolean
);


--
-- Name: autocodecond; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.autocodecond (
    "AutoCodeCondNum" bigint NOT NULL,
    "AutoCodeItemNum" bigint,
    "Cond" smallint
);


--
-- Name: autocodeitem; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.autocodeitem (
    "AutoCodeItemNum" bigint NOT NULL,
    "AutoCodeNum" bigint,
    "OldCode" character varying(15),
    "CodeNum" bigint
);


--
-- Name: autocommexcludedate; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.autocommexcludedate (
    "AutoCommExcludeDateNum" bigint NOT NULL,
    "ClinicNum" bigint,
    "DateExclude" timestamp without time zone
);


--
-- Name: automation; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.automation (
    "AutomationNum" bigint NOT NULL,
    "Description" text,
    "Autotrigger" smallint,
    "ProcCodes" text,
    "AutoAction" smallint,
    "SheetDefNum" bigint,
    "CommType" bigint,
    "MessageContent" text,
    "AptStatus" boolean,
    "AppointmentTypeNum" bigint,
    "PatStatus" boolean
);


--
-- Name: automationcondition; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.automationcondition (
    "AutomationConditionNum" bigint NOT NULL,
    "AutomationNum" bigint,
    "CompareField" boolean,
    "Comparison" boolean,
    "CompareString" character varying(255)
);


--
-- Name: autonote; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.autonote (
    "AutoNoteNum" bigint NOT NULL,
    "AutoNoteName" character varying(50),
    "MainText" text,
    "Category" bigint
);


--
-- Name: autonotecontrol; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.autonotecontrol (
    "AutoNoteControlNum" bigint NOT NULL,
    "Descript" character varying(50),
    "ControlType" character varying(50),
    "ControlLabel" character varying(255),
    "ControlOptions" text
);


--
-- Name: benefit; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.benefit (
    "BenefitNum" bigint NOT NULL,
    "PlanNum" bigint,
    "PatPlanNum" bigint,
    "CovCatNum" bigint,
    "BenefitType" smallint,
    "Percent" smallint,
    "MonetaryAmt" double precision,
    "TimePeriod" smallint,
    "QuantityQualifier" smallint,
    "Quantity" smallint,
    "CodeNum" bigint,
    "CoverageLevel" integer,
    "SecDateTEntry" timestamp without time zone,
    "SecDateTEdit" timestamp without time zone,
    "CodeGroupNum" bigint,
    "TreatArea" smallint
);


--
-- Name: branding; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.branding (
    "BrandingNum" bigint NOT NULL,
    "BrandingType" smallint,
    "ClinicNum" bigint,
    "ValueString" text,
    "DateTimeUpdated" timestamp without time zone
);


--
-- Name: canadiannetwork; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.canadiannetwork (
    "CanadianNetworkNum" bigint NOT NULL,
    "Abbrev" character varying(20),
    "Descript" character varying(255),
    "CanadianTransactionPrefix" character varying(255),
    "CanadianIsRprHandler" boolean
);


--
-- Name: carecreditwebresponse; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.carecreditwebresponse (
    "CareCreditWebResponseNum" bigint NOT NULL,
    "PatNum" bigint,
    "PayNum" bigint,
    "RefNumber" character varying(255),
    "Amount" double precision,
    "WebToken" character varying(255),
    "ProcessingStatus" character varying(255),
    "DateTimeEntry" timestamp without time zone,
    "DateTimePending" timestamp without time zone,
    "DateTimeCompleted" timestamp without time zone,
    "DateTimeExpired" timestamp without time zone,
    "DateTimeLastError" timestamp without time zone,
    "LastResponseStr" text,
    "ClinicNum" bigint,
    "ServiceType" character varying(255),
    "TransType" character varying(255),
    "MerchantNumber" character varying(20),
    "HasLogged" boolean
);


--
-- Name: carrier; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.carrier (
    "CarrierNum" bigint NOT NULL,
    "CarrierName" character varying(255),
    "Address" character varying(255),
    "Address2" character varying(255),
    "City" character varying(255),
    "State" character varying(255),
    "Zip" character varying(255),
    "Phone" character varying(255),
    "ElectID" character varying(255),
    "NoSendElect" smallint,
    "IsCDA" boolean,
    "CDAnetVersion" character varying(100),
    "CanadianNetworkNum" bigint,
    "IsHidden" boolean,
    "CanadianEncryptionMethod" boolean,
    "CanadianSupportedTypes" integer,
    "SecUserNumEntry" bigint,
    "SecDateEntry" date,
    "SecDateTEdit" timestamp without time zone,
    "TIN" character varying(255),
    "CarrierGroupName" bigint,
    "ApptTextBackColor" integer,
    "IsCoinsuranceInverted" boolean,
    "TrustedEtransFlags" boolean,
    "CobInsPaidBehaviorOverride" boolean,
    "EraAutomationOverride" boolean,
    "OrthoInsPayConsolidate" boolean
);


--
-- Name: cdcrec; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.cdcrec (
    "CdcrecNum" bigint NOT NULL,
    "CdcrecCode" character varying(255),
    "HeirarchicalCode" character varying(255),
    "Description" character varying(255)
);


--
-- Name: cdspermission; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.cdspermission (
    "CDSPermissionNum" bigint NOT NULL,
    "UserNum" bigint,
    "SetupCDS" boolean,
    "ShowCDS" boolean,
    "ShowInfobutton" boolean,
    "EditBibliography" boolean,
    "ProblemCDS" boolean,
    "MedicationCDS" boolean,
    "AllergyCDS" boolean,
    "DemographicCDS" boolean,
    "LabTestCDS" boolean,
    "VitalCDS" boolean
);


--
-- Name: centralconnection; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.centralconnection (
    "CentralConnectionNum" bigint NOT NULL,
    "ServerName" character varying(255),
    "DatabaseName" character varying(255),
    "MySqlUser" character varying(255),
    "MySqlPassword" character varying(255),
    "ServiceURI" character varying(255),
    "OdUser" character varying(255),
    "OdPassword" character varying(255),
    "Note" text,
    "ItemOrder" integer,
    "WebServiceIsEcw" boolean,
    "ConnectionStatus" character varying(255),
    "HasClinicBreakdownReports" boolean
);


--
-- Name: cert; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.cert (
    "CertNum" bigint NOT NULL,
    "Description" character varying(255),
    "WikiPageLink" character varying(255),
    "ItemOrder" integer,
    "IsHidden" boolean,
    "CertCategoryNum" bigint
);


--
-- Name: certemployee; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.certemployee (
    "CertEmployeeNum" bigint NOT NULL,
    "CertNum" bigint,
    "EmployeeNum" bigint,
    "DateCompleted" date,
    "Note" character varying(255),
    "UserNum" bigint
);


--
-- Name: chartview; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.chartview (
    "ChartViewNum" bigint NOT NULL,
    "Description" character varying(255),
    "ItemOrder" integer,
    "ProcStatuses" smallint,
    "ObjectTypes" smallint,
    "ShowProcNotes" boolean,
    "IsAudit" boolean,
    "SelectedTeethOnly" boolean,
    "OrionStatusFlags" integer,
    "DatesShowing" boolean,
    "IsTpCharting" boolean
);


--
-- Name: claim; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.claim (
    "ClaimNum" bigint NOT NULL,
    "PatNum" bigint,
    "DateService" date,
    "DateSent" date,
    "ClaimStatus" character(1),
    "DateReceived" date,
    "PlanNum" bigint,
    "ProvTreat" bigint,
    "ClaimFee" double precision,
    "InsPayEst" double precision,
    "InsPayAmt" double precision,
    "DedApplied" double precision,
    "PreAuthString" character varying(40),
    "IsProsthesis" character(1),
    "PriorDate" date,
    "ReasonUnderPaid" character varying(255),
    "ClaimNote" character varying(400),
    "ClaimType" character varying(255),
    "ProvBill" bigint,
    "ReferringProv" bigint,
    "RefNumString" character varying(40),
    "PlaceService" smallint,
    "AccidentRelated" character(1),
    "AccidentDate" date,
    "AccidentST" character varying(2),
    "EmployRelated" smallint,
    "IsOrtho" boolean,
    "OrthoRemainM" boolean,
    "OrthoDate" date,
    "PatRelat" smallint,
    "PlanNum2" bigint,
    "PatRelat2" smallint,
    "WriteOff" double precision,
    "Radiographs" boolean,
    "ClinicNum" bigint,
    "ClaimForm" bigint,
    "AttachedImages" integer,
    "AttachedModels" integer,
    "AttachedFlags" character varying(255),
    "AttachmentID" character varying(255),
    "CanadianMaterialsForwarded" character varying(10),
    "CanadianReferralProviderNum" character varying(20),
    "CanadianReferralReason" boolean,
    "CanadianIsInitialLower" character varying(5),
    "CanadianDateInitialLower" date,
    "CanadianMandProsthMaterial" boolean,
    "CanadianIsInitialUpper" character varying(5),
    "CanadianDateInitialUpper" date,
    "CanadianMaxProsthMaterial" boolean,
    "InsSubNum" bigint,
    "InsSubNum2" bigint,
    "CanadaTransRefNum" character varying(255),
    "CanadaEstTreatStartDate" date,
    "CanadaInitialPayment" double precision,
    "CanadaPaymentMode" boolean,
    "CanadaTreatDuration" boolean,
    "CanadaNumAnticipatedPayments" boolean,
    "CanadaAnticipatedPayAmount" double precision,
    "PriorAuthorizationNumber" character varying(255),
    "SpecialProgramCode" boolean,
    "UniformBillType" character varying(255),
    "MedType" boolean,
    "AdmissionTypeCode" character varying(255),
    "AdmissionSourceCode" character varying(255),
    "PatientStatusCode" character varying(255),
    "CustomTracking" bigint,
    "DateResent" date,
    "CorrectionType" boolean,
    "ClaimIdentifier" character varying(255),
    "OrigRefNum" character varying(255),
    "ProvOrderOverride" bigint,
    "OrthoTotalM" boolean,
    "ShareOfCost" double precision,
    "SecUserNumEntry" bigint,
    "SecDateEntry" date,
    "SecDateTEdit" timestamp without time zone,
    "OrderingReferralNum" bigint,
    "DateSentOrig" date,
    "DateIllnessInjuryPreg" date,
    "DateIllnessInjuryPregQualifier" smallint,
    "DateOther" date,
    "DateOtherQualifier" smallint,
    "IsOutsideLab" boolean,
    "SecurityHash" character varying(255),
    "Narrative" text
);


--
-- Name: claimattach; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.claimattach (
    "ClaimAttachNum" bigint NOT NULL,
    "ClaimNum" bigint,
    "DisplayedFileName" character varying(255),
    "ActualFileName" character varying(255),
    "ImageReferenceId" integer
);


--
-- Name: claimcondcodelog; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.claimcondcodelog (
    "ClaimCondCodeLogNum" bigint NOT NULL,
    "ClaimNum" bigint,
    "Code0" character varying(2),
    "Code1" character varying(2),
    "Code2" character varying(2),
    "Code3" character varying(2),
    "Code4" character varying(2),
    "Code5" character varying(2),
    "Code6" character varying(2),
    "Code7" character varying(2),
    "Code8" character varying(2),
    "Code9" character varying(2),
    "Code10" character varying(2)
);


--
-- Name: claimform; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.claimform (
    "ClaimFormNum" bigint NOT NULL,
    "Description" character varying(50),
    "IsHidden" boolean,
    "FontName" character varying(255),
    "FontSize" real,
    "UniqueID" character varying(255),
    "PrintImages" boolean,
    "OffsetX" smallint,
    "OffsetY" smallint,
    "Width" integer,
    "Height" integer
);


--
-- Name: claimformitem; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.claimformitem (
    "ClaimFormItemNum" bigint NOT NULL,
    "ClaimFormNum" bigint,
    "ImageFileName" character varying(255),
    "FieldName" character varying(255),
    "FormatString" character varying(255),
    "XPos" real,
    "YPos" real,
    "Width" real,
    "Height" real
);


--
-- Name: claimpayment; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.claimpayment (
    "ClaimPaymentNum" bigint NOT NULL,
    "CheckDate" date,
    "CheckAmt" double precision,
    "CheckNum" character varying(25),
    "BankBranch" character varying(25),
    "Note" character varying(255),
    "ClinicNum" bigint,
    "DepositNum" bigint,
    "CarrierName" character varying(255),
    "DateIssued" date,
    "IsPartial" boolean,
    "PayType" bigint,
    "SecUserNumEntry" bigint,
    "SecDateEntry" date,
    "SecDateTEdit" timestamp without time zone,
    "PayGroup" bigint
);


--
-- Name: claimproc; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.claimproc (
    "ClaimProcNum" bigint NOT NULL,
    "ProcNum" bigint,
    "ClaimNum" bigint,
    "PatNum" bigint,
    "ProvNum" bigint,
    "FeeBilled" double precision,
    "InsPayEst" double precision,
    "DedApplied" double precision,
    "Status" smallint,
    "InsPayAmt" double precision,
    "Remarks" character varying(255),
    "ClaimPaymentNum" bigint,
    "PlanNum" bigint,
    "DateCP" date,
    "WriteOff" double precision,
    "CodeSent" character varying(15),
    "AllowedOverride" double precision,
    "Percentage" smallint,
    "PercentOverride" smallint,
    "CopayAmt" double precision,
    "NoBillIns" boolean,
    "PaidOtherIns" double precision,
    "BaseEst" double precision,
    "CopayOverride" double precision,
    "ProcDate" date,
    "DateEntry" date,
    "LineNumber" smallint,
    "DedEst" double precision,
    "DedEstOverride" double precision,
    "InsEstTotal" double precision,
    "InsEstTotalOverride" double precision,
    "PaidOtherInsOverride" double precision,
    "EstimateNote" character varying(255),
    "WriteOffEst" double precision,
    "WriteOffEstOverride" double precision,
    "ClinicNum" bigint,
    "InsSubNum" bigint,
    "PaymentRow" integer,
    "PayPlanNum" bigint,
    "ClaimPaymentTracking" bigint,
    "SecUserNumEntry" bigint,
    "SecDateEntry" date,
    "SecDateTEdit" timestamp without time zone,
    "DateSuppReceived" date,
    "DateInsFinalized" date,
    "IsTransfer" boolean,
    "ClaimAdjReasonCodes" character varying(255),
    "IsOverpay" boolean,
    "SecurityHash" character varying(255)
);


--
-- Name: claimsnapshot; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.claimsnapshot (
    "ClaimSnapshotNum" bigint NOT NULL,
    "ProcNum" bigint,
    "ClaimType" character varying(255),
    "Writeoff" double precision,
    "InsPayEst" double precision,
    "Fee" double precision,
    "DateTEntry" timestamp without time zone,
    "ClaimProcNum" bigint,
    "SnapshotTrigger" boolean
);


--
-- Name: claimtracking; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.claimtracking (
    "ClaimTrackingNum" bigint NOT NULL,
    "ClaimNum" bigint,
    "TrackingType" character varying(255),
    "UserNum" bigint,
    "DateTimeEntry" timestamp without time zone,
    "Note" text,
    "TrackingDefNum" bigint,
    "TrackingErrorDefNum" bigint
);


--
-- Name: claimvalcodelog; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.claimvalcodelog (
    "ClaimValCodeLogNum" bigint NOT NULL,
    "ClaimNum" bigint,
    "ClaimField" character varying(5),
    "ValCode" character(2),
    "ValAmount" double precision,
    "Ordinal" integer
);


--
-- Name: clearinghouse; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.clearinghouse (
    "ClearinghouseNum" bigint NOT NULL,
    "Description" character varying(255),
    "ExportPath" text,
    "Payors" text,
    "Eformat" smallint,
    "ISA05" character varying(255),
    "SenderTIN" character varying(255),
    "ISA07" character varying(255),
    "ISA08" character varying(255),
    "ISA15" character varying(255),
    "Password" character varying(255),
    "ResponsePath" character varying(255),
    "CommBridge" smallint,
    "ClientProgram" character varying(255),
    "LastBatchNumber" smallint,
    "ModemPort" boolean,
    "LoginID" character varying(255),
    "SenderName" character varying(255),
    "SenderTelephone" character varying(255),
    "GS03" character varying(255),
    "ISA02" character varying(10),
    "ISA04" character varying(10),
    "ISA16" character varying(2),
    "SeparatorData" character varying(2),
    "SeparatorSegment" character varying(2),
    "ClinicNum" bigint,
    "HqClearinghouseNum" bigint,
    "IsEraDownloadAllowed" smallint,
    "IsClaimExportAllowed" boolean,
    "IsAttachmentSendAllowed" boolean,
    "LocationID" character varying(255)
);


--
-- Name: clinic; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.clinic (
    "ClinicNum" bigint NOT NULL,
    "Description" character varying(255),
    "Address" character varying(255),
    "Address2" character varying(255),
    "City" character varying(255),
    "State" character varying(255),
    "Zip" character varying(255),
    "Phone" character varying(255),
    "BankNumber" character varying(255),
    "DefaultPlaceService" boolean,
    "InsBillingProv" bigint,
    "Fax" character varying(50),
    "EmailAddressNum" bigint,
    "DefaultProv" bigint,
    "SmsContractDate" timestamp without time zone,
    "SmsMonthlyLimit" double precision,
    "IsMedicalOnly" boolean,
    "BillingAddress" character varying(255),
    "BillingAddress2" character varying(255),
    "BillingCity" character varying(255),
    "BillingState" character varying(255),
    "BillingZip" character varying(255),
    "PayToAddress" character varying(255),
    "PayToAddress2" character varying(255),
    "PayToCity" character varying(255),
    "PayToState" character varying(255),
    "PayToZip" character varying(255),
    "UseBillAddrOnClaims" boolean,
    "Region" bigint,
    "ItemOrder" integer,
    "IsInsVerifyExcluded" boolean,
    "Abbr" character varying(255),
    "MedLabAccountNum" character varying(16),
    "IsConfirmEnabled" boolean,
    "IsConfirmDefault" boolean,
    "IsNewPatApptExcluded" boolean,
    "IsHidden" boolean,
    "ExternalID" bigint,
    "SchedNote" character varying(255),
    "HasProcOnRx" boolean,
    "TimeZone" character varying(75),
    "EmailAliasOverride" character varying(255)
);


--
-- Name: clinicerx; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.clinicerx (
    "ClinicErxNum" bigint NOT NULL,
    "PatNum" bigint,
    "ClinicDesc" character varying(255),
    "ClinicNum" bigint,
    "EnabledStatus" boolean,
    "ClinicId" character varying(255),
    "ClinicKey" character varying(255),
    "AccountId" character varying(25),
    "RegistrationKeyNum" bigint
);


--
-- Name: clinicpref; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.clinicpref (
    "ClinicPrefNum" bigint NOT NULL,
    "ClinicNum" bigint,
    "PrefName" character varying(255),
    "ValueString" text
);


--
-- Name: clockevent; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.clockevent (
    "ClockEventNum" bigint NOT NULL,
    "EmployeeNum" bigint,
    "TimeEntered1" timestamp without time zone,
    "TimeDisplayed1" timestamp without time zone,
    "ClockStatus" smallint,
    "Note" text,
    "TimeEntered2" timestamp without time zone,
    "TimeDisplayed2" timestamp without time zone,
    "OTimeHours" time without time zone,
    "OTimeAuto" time without time zone,
    "Adjust" time without time zone,
    "AdjustAuto" time without time zone,
    "AdjustIsOverridden" boolean,
    "Rate2Hours" time without time zone,
    "Rate2Auto" time without time zone,
    "ClinicNum" bigint,
    "Rate3Hours" time without time zone,
    "Rate3Auto" time without time zone,
    "IsWorkingHome" boolean
);


--
-- Name: cloudaddress; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.cloudaddress (
    "CloudAddressNum" bigint NOT NULL,
    "IpAddress" character varying(50),
    "UserNumLastConnect" bigint,
    "DateTimeLastConnect" timestamp without time zone
);


--
-- Name: codegroup; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.codegroup (
    "CodeGroupNum" bigint NOT NULL,
    "GroupName" character varying(50),
    "ProcCodes" text,
    "ItemOrder" integer,
    "CodeGroupFixed" smallint,
    "IsHidden" boolean,
    "ShowInAgeLimit" boolean
);


--
-- Name: codesystem; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.codesystem (
    "CodeSystemNum" bigint NOT NULL,
    "CodeSystemName" character varying(255),
    "VersionCur" character varying(255),
    "VersionAvail" character varying(255),
    "HL7OID" character varying(255),
    "Note" character varying(255)
);


--
-- Name: commlog; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.commlog (
    "CommlogNum" bigint NOT NULL,
    "PatNum" bigint,
    "CommDateTime" timestamp without time zone,
    "CommType" bigint,
    "Note" text,
    "Mode_" smallint,
    "SentOrReceived" smallint,
    "UserNum" bigint,
    "Signature" text,
    "SigIsTopaz" boolean,
    "DateTStamp" timestamp without time zone,
    "DateTimeEnd" timestamp without time zone,
    "CommSource" smallint,
    "ProgramNum" bigint,
    "DateTEntry" timestamp without time zone,
    "ReferralNum" bigint,
    "CommReferralBehavior" boolean
);


--
-- Name: commoptout; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.commoptout (
    "CommOptOutNum" bigint NOT NULL,
    "PatNum" bigint,
    "OptOutSms" integer,
    "OptOutEmail" integer
);


--
-- Name: computer; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.computer (
    "ComputerNum" bigint NOT NULL,
    "CompName" character varying(100),
    "LastHeartBeat" timestamp without time zone
);


--
-- Name: computerpref; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.computerpref (
    "ComputerPrefNum" bigint NOT NULL,
    "ComputerName" character varying(64),
    "GraphicsUseHardware" boolean,
    "GraphicsSimple" smallint,
    "SensorType" character varying(255),
    "SensorBinned" boolean,
    "SensorPort" integer,
    "SensorExposure" integer,
    "GraphicsDoubleBuffering" boolean,
    "PreferredPixelFormatNum" integer,
    "AtoZpath" character varying(255),
    "TaskKeepListHidden" boolean,
    "TaskDock" integer,
    "TaskX" integer,
    "TaskY" integer,
    "DirectXFormat" character varying(255),
    "ScanDocSelectSource" boolean,
    "ScanDocShowOptions" boolean,
    "ScanDocDuplex" boolean,
    "ScanDocGrayscale" boolean,
    "ScanDocResolution" integer,
    "ScanDocQuality" smallint,
    "ClinicNum" bigint,
    "ApptViewNum" bigint,
    "RecentApptView" boolean,
    "PatSelectSearchMode" smallint,
    "NoShowLanguage" boolean,
    "NoShowDecimal" boolean,
    "ComputerOS" character varying(255),
    "HelpButtonXAdjustment" double precision,
    "GraphicsUseDirectX11" boolean,
    "Zoom" integer,
    "VideoRectangle" character varying(255),
    "CreditCardTerminalId" character varying(255)
);


--
-- Name: confirmationrequest; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.confirmationrequest (
    "ConfirmationRequestNum" bigint NOT NULL,
    "ClinicNum" bigint,
    "PatNum" bigint,
    "ApptNum" bigint,
    "DateTimeConfirmExpire" timestamp without time zone,
    "ShortGUID" character varying(255),
    "ConfirmCode" character varying(255),
    "DateTimeEntry" timestamp without time zone,
    "DateTimeConfirmTransmit" timestamp without time zone,
    "DateTimeRSVP" timestamp without time zone,
    "RSVPStatus" smallint,
    "ResponseDescript" text,
    "GuidMessageFromMobile" text,
    "ApptDateTime" timestamp without time zone,
    "TSPrior" bigint,
    "DoNotResend" boolean,
    "SendStatus" smallint,
    "ApptReminderRuleNum" bigint,
    "MessageType" smallint,
    "MessageFk" bigint,
    "DateTimeSent" timestamp without time zone
);


--
-- Name: connectiongroup; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.connectiongroup (
    "ConnectionGroupNum" bigint NOT NULL,
    "Description" character varying(255)
);


--
-- Name: conngroupattach; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.conngroupattach (
    "ConnGroupAttachNum" bigint NOT NULL,
    "ConnectionGroupNum" bigint,
    "CentralConnectionNum" bigint
);


--
-- Name: contact; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.contact (
    "ContactNum" bigint NOT NULL,
    "LName" character varying(255),
    "FName" character varying(255),
    "WkPhone" character varying(255),
    "Fax" character varying(255),
    "Category" bigint,
    "Notes" text
);


--
-- Name: county; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.county (
    "CountyNum" bigint NOT NULL,
    "CountyName" character varying(255),
    "CountyCode" character varying(255)
);


--
-- Name: covcat; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.covcat (
    "CovCatNum" bigint NOT NULL,
    "Description" character varying(50),
    "DefaultPercent" smallint,
    "CovOrder" integer,
    "IsHidden" boolean,
    "EbenefitCat" smallint
);


--
-- Name: covspan; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.covspan (
    "CovSpanNum" bigint NOT NULL,
    "CovCatNum" bigint,
    "FromCode" character varying(15),
    "ToCode" character varying(15)
);


--
-- Name: cpt; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.cpt (
    "CptNum" bigint NOT NULL,
    "CptCode" character varying(255),
    "Description" character varying(4000),
    "VersionIDs" character varying(255)
);


--
-- Name: creditcard; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.creditcard (
    "CreditCardNum" bigint NOT NULL,
    "PatNum" bigint,
    "Address" character varying(255),
    "Zip" character varying(255),
    "XChargeToken" character varying(255),
    "CCNumberMasked" character varying(255),
    "CCExpiration" date,
    "ItemOrder" integer,
    "ChargeAmt" double precision,
    "DateStart" date,
    "DateStop" date,
    "Note" character varying(255),
    "PayPlanNum" bigint,
    "PayConnectToken" character varying(255),
    "PayConnectTokenExp" date,
    "Procedures" text,
    "CCSource" boolean,
    "ClinicNum" bigint,
    "ExcludeProcSync" boolean,
    "PaySimpleToken" character varying(255),
    "ChargeFrequency" character varying(150),
    "CanChargeWhenNoBal" boolean,
    "PaymentType" bigint,
    "IsRecurringActive" boolean,
    "Nickname" character varying(255)
);


--
-- Name: custrefentry; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.custrefentry (
    "CustRefEntryNum" bigint NOT NULL,
    "PatNumCust" bigint,
    "PatNumRef" bigint,
    "DateEntry" date,
    "Note" character varying(255)
);


--
-- Name: custreference; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.custreference (
    "CustReferenceNum" bigint NOT NULL,
    "PatNum" bigint,
    "DateMostRecent" date,
    "Note" character varying(255),
    "IsBadRef" boolean
);


--
-- Name: cvx; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.cvx (
    "CvxNum" bigint NOT NULL,
    "CvxCode" character varying(255),
    "Description" character varying(255),
    "IsActive" character varying(255)
);


--
-- Name: dashboardar; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.dashboardar (
    "DashboardARNum" bigint NOT NULL,
    "DateCalc" date,
    "BalTotal" double precision,
    "InsEst" double precision
);


--
-- Name: dashboardcell; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.dashboardcell (
    "DashboardCellNum" bigint NOT NULL,
    "DashboardLayoutNum" bigint,
    "CellRow" integer,
    "CellColumn" integer,
    "CellType" character varying(255),
    "CellSettings" text,
    "LastQueryTime" timestamp without time zone,
    "LastQueryData" text,
    "RefreshRateSeconds" integer
);


--
-- Name: dashboardlayout; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.dashboardlayout (
    "DashboardLayoutNum" bigint NOT NULL,
    "UserNum" bigint,
    "UserGroupNum" bigint,
    "DashboardTabName" character varying(255),
    "DashboardTabOrder" integer,
    "DashboardRows" integer,
    "DashboardColumns" integer,
    "DashboardGroupName" character varying(255)
);


--
-- Name: databasemaintenance; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.databasemaintenance (
    "DatabaseMaintenanceNum" bigint NOT NULL,
    "MethodName" character varying(255),
    "IsHidden" boolean,
    "IsOld" boolean,
    "DateLastRun" timestamp without time zone
);


--
-- Name: dbmlog; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.dbmlog (
    "DbmLogNum" bigint NOT NULL,
    "UserNum" bigint,
    "FKey" bigint,
    "FKeyType" smallint,
    "ActionType" smallint,
    "DateTimeEntry" timestamp without time zone,
    "MethodName" character varying(255),
    "LogText" text
);


--
-- Name: definition; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.definition (
    "DefNum" bigint NOT NULL,
    "Category" smallint,
    "ItemOrder" smallint,
    "ItemName" character varying(255),
    "ItemValue" character varying(255),
    "ItemColor" integer,
    "IsHidden" boolean
);


--
-- Name: deflink; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.deflink (
    "DefLinkNum" bigint NOT NULL,
    "DefNum" bigint,
    "FKey" bigint,
    "LinkType" smallint
);


--
-- Name: deletedobject; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.deletedobject (
    "DeletedObjectNum" bigint NOT NULL,
    "ObjectNum" bigint,
    "ObjectType" integer,
    "DateTStamp" timestamp without time zone
);


--
-- Name: deposit; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.deposit (
    "DepositNum" bigint NOT NULL,
    "DateDeposit" date,
    "BankAccountInfo" text,
    "Amount" double precision,
    "Memo" character varying(255),
    "Batch" character varying(25),
    "DepositAccountNum" bigint,
    "IsSentToQuickBooksOnline" boolean
);


--
-- Name: dictcustom; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.dictcustom (
    "DictCustomNum" bigint NOT NULL,
    "WordText" character varying(255)
);


--
-- Name: discountplan; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.discountplan (
    "DiscountPlanNum" bigint NOT NULL,
    "Description" character varying(255),
    "FeeSchedNum" bigint,
    "DefNum" bigint,
    "IsHidden" boolean,
    "PlanNote" text,
    "ExamFreqLimit" integer,
    "XrayFreqLimit" integer,
    "ProphyFreqLimit" integer,
    "FluorideFreqLimit" integer,
    "PerioFreqLimit" integer,
    "LimitedExamFreqLimit" integer,
    "PAFreqLimit" integer,
    "AnnualMax" double precision
);


--
-- Name: discountplansub; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.discountplansub (
    "DiscountSubNum" bigint NOT NULL,
    "DiscountPlanNum" bigint,
    "PatNum" bigint,
    "DateEffective" date,
    "DateTerm" date,
    "SubNote" text
);


--
-- Name: disease; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.disease (
    "DiseaseNum" bigint NOT NULL,
    "PatNum" bigint,
    "DiseaseDefNum" bigint,
    "PatNote" text,
    "DateTStamp" timestamp without time zone,
    "ProbStatus" smallint,
    "DateStart" date,
    "DateStop" date,
    "SnomedProblemType" character varying(255),
    "FunctionStatus" boolean
);


--
-- Name: diseasedef; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.diseasedef (
    "DiseaseDefNum" bigint NOT NULL,
    "DiseaseName" character varying(255),
    "ItemOrder" smallint,
    "IsHidden" boolean,
    "DateTStamp" timestamp without time zone,
    "ICD9Code" character varying(255),
    "SnomedCode" character varying(255),
    "Icd10Code" character varying(255)
);


--
-- Name: displayfield; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.displayfield (
    "DisplayFieldNum" bigint NOT NULL,
    "InternalName" character varying(255),
    "ItemOrder" integer,
    "Description" character varying(255),
    "ColumnWidth" integer,
    "Category" integer,
    "ChartViewNum" bigint,
    "PickList" text,
    "DescriptionOverride" character varying(255)
);


--
-- Name: displayreport; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.displayreport (
    "DisplayReportNum" bigint NOT NULL,
    "InternalName" character varying(255),
    "ItemOrder" integer,
    "Description" character varying(255),
    "Category" smallint,
    "IsHidden" boolean,
    "IsVisibleInSubMenu" boolean
);


--
-- Name: dispsupply; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.dispsupply (
    "DispSupplyNum" bigint NOT NULL,
    "SupplyNum" bigint,
    "ProvNum" bigint,
    "DateDispensed" date,
    "DispQuantity" real,
    "Note" text
);


--
-- Name: document; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.document (
    "DocNum" bigint NOT NULL,
    "Description" character varying(255),
    "DateCreated" timestamp without time zone,
    "DocCategory" bigint,
    "PatNum" bigint,
    "FileName" character varying(255),
    "ImgType" smallint,
    "IsFlipped" boolean,
    "DegreesRotated" real,
    "ToothNumbers" character varying(255),
    "Note" text,
    "SigIsTopaz" boolean,
    "Signature" text,
    "CropX" integer,
    "CropY" integer,
    "CropW" integer,
    "CropH" integer,
    "WindowingMin" integer,
    "WindowingMax" integer,
    "MountItemNum" bigint,
    "DateTStamp" timestamp without time zone,
    "RawBase64" text,
    "Thumbnail" text,
    "ExternalGUID" character varying(255),
    "ExternalSource" character varying(255),
    "ProvNum" bigint,
    "IsCropOld" boolean,
    "OcrResponseData" text,
    "ImageCaptureType" boolean,
    "PrintHeading" boolean,
    "ChartLetterStatus" boolean,
    "UserNum" bigint,
    "ChartLetterHash" character varying(255)
);


--
-- Name: documentmisc; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.documentmisc (
    "DocMiscNum" bigint NOT NULL,
    "DateCreated" date,
    "FileName" character varying(255),
    "DocMiscType" boolean,
    "RawBase64" text
);


--
-- Name: drugmanufacturer; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.drugmanufacturer (
    "DrugManufacturerNum" bigint NOT NULL,
    "ManufacturerName" character varying(255),
    "ManufacturerCode" character varying(20)
);


--
-- Name: drugunit; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.drugunit (
    "DrugUnitNum" bigint NOT NULL,
    "UnitIdentifier" character varying(20),
    "UnitText" character varying(255)
);


--
-- Name: dunning; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.dunning (
    "DunningNum" bigint NOT NULL,
    "DunMessage" text,
    "BillingType" bigint,
    "AgeAccount" boolean,
    "InsIsPending" boolean,
    "MessageBold" text,
    "EmailSubject" character varying(255),
    "EmailBody" text,
    "DaysInAdvance" integer,
    "ClinicNum" bigint,
    "IsSuperFamily" boolean
);


--
-- Name: ebill; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.ebill (
    "EbillNum" bigint NOT NULL,
    "ClinicNum" bigint,
    "ClientAcctNumber" character varying(255),
    "ElectUserName" character varying(255),
    "ElectPassword" character varying(255),
    "PracticeAddress" boolean,
    "RemitAddress" boolean
);


--
-- Name: eclipboardimagecapture; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.eclipboardimagecapture (
    "EClipboardImageCaptureNum" bigint NOT NULL,
    "PatNum" bigint,
    "DefNum" bigint,
    "IsSelfPortrait" boolean,
    "DateTimeUpserted" timestamp without time zone,
    "DocNum" bigint,
    "OcrCaptureType" boolean
);


--
-- Name: eclipboardimagecapturedef; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.eclipboardimagecapturedef (
    "EClipboardImageCaptureDefNum" bigint NOT NULL,
    "DefNum" bigint,
    "IsSelfPortrait" boolean,
    "FrequencyDays" integer,
    "ClinicNum" bigint,
    "OcrCaptureType" boolean,
    "Frequency" boolean,
    "ResubmitInterval" bigint
);


--
-- Name: eclipboardsheetdef; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.eclipboardsheetdef (
    "EClipboardSheetDefNum" bigint NOT NULL,
    "SheetDefNum" bigint,
    "ClinicNum" bigint,
    "ResubmitInterval" bigint,
    "ItemOrder" integer,
    "PrefillStatus" boolean,
    "MinAge" integer,
    "MaxAge" integer,
    "IgnoreSheetDefNums" text,
    "PrefillStatusOverride" bigint,
    "EFormDefNum" bigint,
    "Frequency" smallint
);


--
-- Name: eduresource; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.eduresource (
    "EduResourceNum" bigint NOT NULL,
    "DiseaseDefNum" bigint,
    "MedicationNum" bigint,
    "LabResultID" character varying(255),
    "LabResultName" character varying(255),
    "LabResultCompare" character varying(255),
    "ResourceUrl" character varying(255),
    "SmokingSnoMed" character varying(255)
);


--
-- Name: eform; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.eform (
    "EFormNum" bigint NOT NULL,
    "FormType" boolean,
    "PatNum" bigint,
    "DateTimeShown" timestamp without time zone,
    "Description" character varying(255),
    "DateTEdited" timestamp without time zone,
    "MaxWidth" integer,
    "EFormDefNum" bigint,
    "Status" boolean,
    "RevID" integer,
    "ShowLabelsBold" boolean,
    "SpaceBelowEachField" integer,
    "SpaceToRightEachField" integer,
    "SaveImageCategory" bigint
);


--
-- Name: eformdef; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.eformdef (
    "EFormDefNum" bigint NOT NULL,
    "FormType" boolean,
    "Description" character varying(255),
    "DateTCreated" timestamp without time zone,
    "IsInternalHidden" boolean,
    "MaxWidth" integer,
    "RevID" integer,
    "ShowLabelsBold" boolean,
    "SpaceBelowEachField" integer,
    "SpaceToRightEachField" integer,
    "SaveImageCategory" bigint
);


--
-- Name: eformfield; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.eformfield (
    "EFormFieldNum" bigint NOT NULL,
    "EFormNum" bigint,
    "PatNum" bigint,
    "FieldType" boolean,
    "DbLink" character varying(255),
    "ValueLabel" text,
    "ValueString" text,
    "ItemOrder" integer,
    "PickListVis" character varying(255),
    "PickListDb" character varying(255),
    "IsHorizStacking" boolean,
    "IsTextWrap" boolean,
    "Width" integer,
    "FontScale" integer,
    "IsRequired" boolean,
    "ConditionalParent" character varying(255),
    "ConditionalValue" character varying(255),
    "LabelAlign" boolean,
    "SpaceBelow" integer,
    "ReportableName" character varying(255),
    "IsLocked" boolean,
    "Border" boolean,
    "IsWidthPercentage" boolean,
    "MinWidth" integer,
    "WidthLabel" integer,
    "SpaceToRight" integer
);


--
-- Name: eformfielddef; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.eformfielddef (
    "EFormFieldDefNum" bigint NOT NULL,
    "EFormDefNum" bigint,
    "FieldType" boolean,
    "DbLink" character varying(255),
    "ValueLabel" text,
    "ItemOrder" integer,
    "PickListVis" character varying(255),
    "PickListDb" character varying(255),
    "IsHorizStacking" boolean,
    "IsTextWrap" boolean,
    "Width" integer,
    "FontScale" integer,
    "IsRequired" boolean,
    "ConditionalParent" character varying(255),
    "ConditionalValue" character varying(255),
    "LabelAlign" boolean,
    "SpaceBelow" integer,
    "ReportableName" character varying(255),
    "IsLocked" boolean,
    "Border" boolean,
    "IsWidthPercentage" boolean,
    "MinWidth" integer,
    "WidthLabel" integer,
    "SpaceToRight" integer
);


--
-- Name: eformimportrule; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.eformimportrule (
    "EFormImportRuleNum" bigint NOT NULL,
    "FieldName" character varying(255),
    "Situation" boolean,
    "Action" boolean
);


--
-- Name: ehramendment; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.ehramendment (
    "EhrAmendmentNum" bigint NOT NULL,
    "PatNum" bigint,
    "IsAccepted" boolean,
    "Description" text,
    "Source" boolean,
    "SourceName" text,
    "FileName" character varying(255),
    "RawBase64" text,
    "DateTRequest" timestamp without time zone,
    "DateTAcceptDeny" timestamp without time zone,
    "DateTAppend" timestamp without time zone
);


--
-- Name: ehraptobs; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.ehraptobs (
    "EhrAptObsNum" bigint NOT NULL,
    "AptNum" bigint,
    "IdentifyingCode" boolean,
    "ValType" boolean,
    "ValReported" character varying(255),
    "UcumCode" character varying(255),
    "ValCodeSystem" character varying(255)
);


--
-- Name: ehrcareplan; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.ehrcareplan (
    "EhrCarePlanNum" bigint NOT NULL,
    "PatNum" bigint,
    "SnomedEducation" character varying(255),
    "Instructions" character varying(255),
    "DatePlanned" date
);


--
-- Name: ehrlab; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.ehrlab (
    "EhrLabNum" bigint NOT NULL,
    "PatNum" bigint,
    "OrderControlCode" character varying(255),
    "PlacerOrderNum" character varying(255),
    "PlacerOrderNamespace" character varying(255),
    "PlacerOrderUniversalID" character varying(255),
    "PlacerOrderUniversalIDType" character varying(255),
    "FillerOrderNum" character varying(255),
    "FillerOrderNamespace" character varying(255),
    "FillerOrderUniversalID" character varying(255),
    "FillerOrderUniversalIDType" character varying(255),
    "PlacerGroupNum" character varying(255),
    "PlacerGroupNamespace" character varying(255),
    "PlacerGroupUniversalID" character varying(255),
    "PlacerGroupUniversalIDType" character varying(255),
    "OrderingProviderID" character varying(255),
    "OrderingProviderLName" character varying(255),
    "OrderingProviderFName" character varying(255),
    "OrderingProviderMiddleNames" character varying(255),
    "OrderingProviderSuffix" character varying(255),
    "OrderingProviderPrefix" character varying(255),
    "OrderingProviderAssigningAuthorityNamespaceID" character varying(255),
    "OrderingProviderAssigningAuthorityUniversalID" character varying(255),
    "OrderingProviderAssigningAuthorityIDType" character varying(255),
    "OrderingProviderNameTypeCode" character varying(255),
    "OrderingProviderIdentifierTypeCode" character varying(255),
    "SetIdOBR" bigint,
    "UsiID" character varying(255),
    "UsiText" character varying(255),
    "UsiCodeSystemName" character varying(255),
    "UsiIDAlt" character varying(255),
    "UsiTextAlt" character varying(255),
    "UsiCodeSystemNameAlt" character varying(255),
    "UsiTextOriginal" character varying(255),
    "ObservationDateTimeStart" character varying(255),
    "ObservationDateTimeEnd" character varying(255),
    "SpecimenActionCode" character varying(255),
    "ResultDateTime" character varying(255),
    "ResultStatus" character varying(255),
    "ParentObservationID" character varying(255),
    "ParentObservationText" character varying(255),
    "ParentObservationCodeSystemName" character varying(255),
    "ParentObservationIDAlt" character varying(255),
    "ParentObservationTextAlt" character varying(255),
    "ParentObservationCodeSystemNameAlt" character varying(255),
    "ParentObservationTextOriginal" character varying(255),
    "ParentObservationSubID" character varying(255),
    "ParentPlacerOrderNum" character varying(255),
    "ParentPlacerOrderNamespace" character varying(255),
    "ParentPlacerOrderUniversalID" character varying(255),
    "ParentPlacerOrderUniversalIDType" character varying(255),
    "ParentFillerOrderNum" character varying(255),
    "ParentFillerOrderNamespace" character varying(255),
    "ParentFillerOrderUniversalID" character varying(255),
    "ParentFillerOrderUniversalIDType" character varying(255),
    "ListEhrLabResultsHandlingF" boolean,
    "ListEhrLabResultsHandlingN" boolean,
    "TQ1SetId" bigint,
    "TQ1DateTimeStart" character varying(255),
    "TQ1DateTimeEnd" character varying(255),
    "IsCpoe" boolean,
    "OriginalPIDSegment" text
);


--
-- Name: ehrlabclinicalinfo; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.ehrlabclinicalinfo (
    "EhrLabClinicalInfoNum" bigint NOT NULL,
    "EhrLabNum" bigint,
    "ClinicalInfoID" character varying(255),
    "ClinicalInfoText" character varying(255),
    "ClinicalInfoCodeSystemName" character varying(255),
    "ClinicalInfoIDAlt" character varying(255),
    "ClinicalInfoTextAlt" character varying(255),
    "ClinicalInfoCodeSystemNameAlt" character varying(255),
    "ClinicalInfoTextOriginal" character varying(255)
);


--
-- Name: ehrlabimage; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.ehrlabimage (
    "EhrLabImageNum" bigint NOT NULL,
    "EhrLabNum" bigint,
    "DocNum" bigint
);


--
-- Name: ehrlabnote; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.ehrlabnote (
    "EhrLabNoteNum" bigint NOT NULL,
    "EhrLabNum" bigint,
    "EhrLabResultNum" bigint,
    "Comments" text
);


--
-- Name: ehrlabresult; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.ehrlabresult (
    "EhrLabResultNum" bigint NOT NULL,
    "EhrLabNum" bigint,
    "SetIdOBX" bigint,
    "ValueType" character varying(255),
    "ObservationIdentifierID" character varying(255),
    "ObservationIdentifierText" character varying(255),
    "ObservationIdentifierCodeSystemName" character varying(255),
    "ObservationIdentifierIDAlt" character varying(255),
    "ObservationIdentifierTextAlt" character varying(255),
    "ObservationIdentifierCodeSystemNameAlt" character varying(255),
    "ObservationIdentifierTextOriginal" character varying(255),
    "ObservationIdentifierSub" character varying(255),
    "ObservationValueCodedElementID" character varying(255),
    "ObservationValueCodedElementText" character varying(255),
    "ObservationValueCodedElementCodeSystemName" character varying(255),
    "ObservationValueCodedElementIDAlt" character varying(255),
    "ObservationValueCodedElementTextAlt" character varying(255),
    "ObservationValueCodedElementCodeSystemNameAlt" character varying(255),
    "ObservationValueCodedElementTextOriginal" character varying(255),
    "ObservationValueDateTime" character varying(255),
    "ObservationValueTime" time without time zone,
    "ObservationValueComparator" character varying(255),
    "ObservationValueNumber1" double precision,
    "ObservationValueSeparatorOrSuffix" character varying(255),
    "ObservationValueNumber2" double precision,
    "ObservationValueNumeric" double precision,
    "ObservationValueText" character varying(255),
    "UnitsID" character varying(255),
    "UnitsText" character varying(255),
    "UnitsCodeSystemName" character varying(255),
    "UnitsIDAlt" character varying(255),
    "UnitsTextAlt" character varying(255),
    "UnitsCodeSystemNameAlt" character varying(255),
    "UnitsTextOriginal" character varying(255),
    "referenceRange" character varying(255),
    "AbnormalFlags" character varying(255),
    "ObservationResultStatus" character varying(255),
    "ObservationDateTime" character varying(255),
    "AnalysisDateTime" character varying(255),
    "PerformingOrganizationName" character varying(255),
    "PerformingOrganizationNameAssigningAuthorityNamespaceId" character varying(255),
    "PerformingOrganizationNameAssigningAuthorityUniversalId" character varying(255),
    "PerformingOrganizationNameAssigningAuthorityUniversalIdType" character varying(255),
    "PerformingOrganizationIdentifierTypeCode" character varying(255),
    "PerformingOrganizationIdentifier" character varying(255),
    "PerformingOrganizationAddressStreet" character varying(255),
    "PerformingOrganizationAddressOtherDesignation" character varying(255),
    "PerformingOrganizationAddressCity" character varying(255),
    "PerformingOrganizationAddressStateOrProvince" character varying(255),
    "PerformingOrganizationAddressZipOrPostalCode" character varying(255),
    "PerformingOrganizationAddressCountryCode" character varying(255),
    "PerformingOrganizationAddressAddressType" character varying(255),
    "PerformingOrganizationAddressCountyOrParishCode" character varying(255),
    "MedicalDirectorID" character varying(255),
    "MedicalDirectorLName" character varying(255),
    "MedicalDirectorFName" character varying(255),
    "MedicalDirectorMiddleNames" character varying(255),
    "MedicalDirectorSuffix" character varying(255),
    "MedicalDirectorPrefix" character varying(255),
    "MedicalDirectorAssigningAuthorityNamespaceID" character varying(255),
    "MedicalDirectorAssigningAuthorityUniversalID" character varying(255),
    "MedicalDirectorAssigningAuthorityIDType" character varying(255),
    "MedicalDirectorNameTypeCode" character varying(255),
    "MedicalDirectorIdentifierTypeCode" character varying(255)
);


--
-- Name: ehrlabresultscopyto; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.ehrlabresultscopyto (
    "EhrLabResultsCopyToNum" bigint NOT NULL,
    "EhrLabNum" bigint,
    "CopyToID" character varying(255),
    "CopyToLName" character varying(255),
    "CopyToFName" character varying(255),
    "CopyToMiddleNames" character varying(255),
    "CopyToSuffix" character varying(255),
    "CopyToPrefix" character varying(255),
    "CopyToAssigningAuthorityNamespaceID" character varying(255),
    "CopyToAssigningAuthorityUniversalID" character varying(255),
    "CopyToAssigningAuthorityIDType" character varying(255),
    "CopyToNameTypeCode" character varying(255),
    "CopyToIdentifierTypeCode" character varying(255)
);


--
-- Name: ehrlabspecimen; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.ehrlabspecimen (
    "EhrLabSpecimenNum" bigint NOT NULL,
    "EhrLabNum" bigint,
    "SetIdSPM" bigint,
    "SpecimenTypeID" character varying(255),
    "SpecimenTypeText" character varying(255),
    "SpecimenTypeCodeSystemName" character varying(255),
    "SpecimenTypeIDAlt" character varying(255),
    "SpecimenTypeTextAlt" character varying(255),
    "SpecimenTypeCodeSystemNameAlt" character varying(255),
    "SpecimenTypeTextOriginal" character varying(255),
    "CollectionDateTimeStart" character varying(255),
    "CollectionDateTimeEnd" character varying(255)
);


--
-- Name: ehrlabspecimencondition; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.ehrlabspecimencondition (
    "EhrLabSpecimenConditionNum" bigint NOT NULL,
    "EhrLabSpecimenNum" bigint,
    "SpecimenConditionID" character varying(255),
    "SpecimenConditionText" character varying(255),
    "SpecimenConditionCodeSystemName" character varying(255),
    "SpecimenConditionIDAlt" character varying(255),
    "SpecimenConditionTextAlt" character varying(255),
    "SpecimenConditionCodeSystemNameAlt" character varying(255),
    "SpecimenConditionTextOriginal" character varying(255)
);


--
-- Name: ehrlabspecimenrejectreason; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.ehrlabspecimenrejectreason (
    "EhrLabSpecimenRejectReasonNum" bigint NOT NULL,
    "EhrLabSpecimenNum" bigint,
    "SpecimenRejectReasonID" character varying(255),
    "SpecimenRejectReasonText" character varying(255),
    "SpecimenRejectReasonCodeSystemName" character varying(255),
    "SpecimenRejectReasonIDAlt" character varying(255),
    "SpecimenRejectReasonTextAlt" character varying(255),
    "SpecimenRejectReasonCodeSystemNameAlt" character varying(255),
    "SpecimenRejectReasonTextOriginal" character varying(255)
);


--
-- Name: ehrmeasure; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.ehrmeasure (
    "EhrMeasureNum" bigint NOT NULL,
    "MeasureType" smallint,
    "Numerator" smallint,
    "Denominator" smallint
);


--
-- Name: ehrmeasureevent; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.ehrmeasureevent (
    "EhrMeasureEventNum" bigint NOT NULL,
    "DateTEvent" timestamp without time zone,
    "EventType" smallint,
    "PatNum" bigint,
    "MoreInfo" character varying(255),
    "CodeValueEvent" character varying(30),
    "CodeSystemEvent" character varying(30),
    "CodeValueResult" character varying(30),
    "CodeSystemResult" character varying(30),
    "FKey" bigint,
    "TobaccoCessationDesire" boolean,
    "DateStartTobacco" date
);


--
-- Name: ehrnotperformed; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.ehrnotperformed (
    "EhrNotPerformedNum" bigint NOT NULL,
    "PatNum" bigint,
    "ProvNum" bigint,
    "CodeValue" character varying(30),
    "CodeSystem" character varying(30),
    "CodeValueReason" character varying(30),
    "CodeSystemReason" character varying(30),
    "Note" text,
    "DateEntry" date
);


--
-- Name: ehrpatient; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.ehrpatient (
    "PatNum" bigint NOT NULL,
    "MotherMaidenFname" character varying(255),
    "MotherMaidenLname" character varying(255),
    "VacShareOk" boolean,
    "MedicaidState" character varying(50),
    "SexualOrientation" character varying(255),
    "GenderIdentity" character varying(255),
    "SexualOrientationNote" character varying(255),
    "GenderIdentityNote" character varying(255),
    "DischargeDate" timestamp without time zone
);


--
-- Name: ehrprovkey; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.ehrprovkey (
    "EhrProvKeyNum" bigint NOT NULL,
    "PatNum" bigint,
    "LName" character varying(255),
    "FName" character varying(255),
    "ProvKey" character varying(255),
    "FullTimeEquiv" real,
    "Notes" text,
    "YearValue" integer
);


--
-- Name: ehrquarterlykey; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.ehrquarterlykey (
    "EhrQuarterlyKeyNum" bigint NOT NULL,
    "YearValue" integer,
    "QuarterValue" integer,
    "PracticeName" character varying(255),
    "KeyValue" character varying(255),
    "PatNum" bigint,
    "Notes" text
);


--
-- Name: ehrsummaryccd; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.ehrsummaryccd (
    "EhrSummaryCcdNum" bigint NOT NULL,
    "PatNum" bigint,
    "DateSummary" date,
    "ContentSummary" text,
    "EmailAttachNum" bigint
);


--
-- Name: ehrtrigger; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.ehrtrigger (
    "EhrTriggerNum" bigint NOT NULL,
    "Description" character varying(255),
    "ProblemSnomedList" text,
    "ProblemIcd9List" text,
    "ProblemIcd10List" text,
    "ProblemDefNumList" text,
    "MedicationNumList" text,
    "RxCuiList" text,
    "CvxList" text,
    "AllergyDefNumList" text,
    "DemographicsList" text,
    "LabLoincList" text,
    "VitalLoincList" text,
    "Instructions" text,
    "Bibliography" text,
    "Cardinality" boolean
);


--
-- Name: electid; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.electid (
    "ElectIDNum" bigint NOT NULL,
    "PayorID" character varying(255),
    "CarrierName" character varying(255),
    "IsMedicaid" boolean,
    "ProviderTypes" character varying(255),
    "Comments" text,
    "CommBridge" boolean,
    "Attributes" character varying(255)
);


--
-- Name: emailaddress; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.emailaddress (
    "EmailAddressNum" bigint NOT NULL,
    "SMTPserver" character varying(255),
    "EmailUsername" character varying(255),
    "EmailPassword" character varying(255),
    "ServerPort" integer,
    "UseSSL" boolean,
    "SenderAddress" character varying(255),
    "Pop3ServerIncoming" character varying(255),
    "ServerPortIncoming" integer,
    "UserNum" bigint,
    "AccessToken" character varying(2000),
    "RefreshToken" text,
    "DownloadInbox" boolean,
    "QueryString" character varying(1000),
    "AuthenticationType" boolean
);


--
-- Name: emailattach; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.emailattach (
    "EmailAttachNum" bigint NOT NULL,
    "EmailMessageNum" bigint,
    "DisplayedFileName" character varying(255),
    "ActualFileName" character varying(255),
    "EmailTemplateNum" bigint
);


--
-- Name: emailautograph; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.emailautograph (
    "EmailAutographNum" bigint NOT NULL,
    "Description" text,
    "EmailAddress" character varying(255),
    "AutographText" text
);


--
-- Name: emailhostingtemplate; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.emailhostingtemplate (
    "EmailHostingTemplateNum" bigint NOT NULL,
    "TemplateName" character varying(255),
    "Subject" text,
    "BodyPlainText" text,
    "BodyHTML" text,
    "TemplateId" bigint,
    "ClinicNum" bigint,
    "EmailTemplateType" character varying(255),
    "TemplateType" character varying(255)
);


--
-- Name: emailmessage; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.emailmessage (
    "EmailMessageNum" bigint NOT NULL,
    "PatNum" bigint,
    "ToAddress" text,
    "FromAddress" text,
    "Subject" text,
    "BodyText" text,
    "MsgDateTime" timestamp without time zone,
    "SentOrReceived" smallint,
    "RecipientAddress" character varying(255),
    "RawEmailIn" text,
    "ProvNumWebMail" bigint,
    "PatNumSubj" bigint,
    "CcAddress" text,
    "BccAddress" text,
    "HideIn" boolean,
    "AptNum" bigint,
    "UserNum" bigint,
    "HtmlType" boolean,
    "SecDateTEntry" timestamp without time zone,
    "SecDateTEdit" timestamp without time zone,
    "MsgType" character varying(255),
    "FailReason" character varying(255)
);


--
-- Name: emailmessageuid; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.emailmessageuid (
    "EmailMessageUidNum" bigint NOT NULL,
    "MsgId" text,
    "RecipientAddress" character varying(255)
);


--
-- Name: emailsecure; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.emailsecure (
    "EmailSecureNum" bigint NOT NULL,
    "ClinicNum" bigint,
    "PatNum" bigint,
    "EmailMessageNum" bigint,
    "EmailChainFK" bigint,
    "EmailFK" bigint,
    "DateTEntry" timestamp without time zone,
    "SecDateTEdit" timestamp without time zone
);


--
-- Name: emailsecureattach; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.emailsecureattach (
    "EmailSecureAttachNum" bigint NOT NULL,
    "ClinicNum" bigint,
    "EmailAttachNum" bigint,
    "EmailSecureNum" bigint,
    "AttachmentGuid" character varying(50),
    "DisplayedFileName" character varying(255),
    "Extension" character varying(255),
    "DateTEntry" timestamp without time zone,
    "SecDateTEdit" timestamp without time zone
);


--
-- Name: emailtemplate; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.emailtemplate (
    "EmailTemplateNum" bigint NOT NULL,
    "Subject" text,
    "BodyText" text,
    "Description" text,
    "TemplateType" boolean
);


--
-- Name: employee; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.employee (
    "EmployeeNum" bigint NOT NULL,
    "LName" character varying(255),
    "FName" character varying(255),
    "MiddleI" character varying(255),
    "IsHidden" boolean,
    "ClockStatus" character varying(255),
    "PhoneExt" integer,
    "PayrollID" character varying(255),
    "WirelessPhone" character varying(255),
    "EmailWork" character varying(255),
    "EmailPersonal" character varying(255),
    "IsFurloughed" boolean,
    "IsWorkingHome" boolean,
    "ReportsTo" bigint
);


--
-- Name: employer; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.employer (
    "EmployerNum" bigint NOT NULL,
    "EmpName" character varying(255),
    "Address" character varying(255),
    "Address2" character varying(255),
    "City" character varying(255),
    "State" character varying(255),
    "Zip" character varying(255),
    "Phone" character varying(255)
);


--
-- Name: encounter; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.encounter (
    "EncounterNum" bigint NOT NULL,
    "PatNum" bigint,
    "ProvNum" bigint,
    "CodeValue" character varying(30),
    "CodeSystem" character varying(30),
    "Note" text,
    "DateEncounter" date
);


--
-- Name: entrylog; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.entrylog (
    "EntryLogNum" bigint NOT NULL,
    "UserNum" bigint,
    "FKeyType" boolean,
    "FKey" bigint,
    "LogSource" smallint,
    "EntryDateTime" timestamp without time zone
);


--
-- Name: eobattach; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.eobattach (
    "EobAttachNum" bigint NOT NULL,
    "ClaimPaymentNum" bigint,
    "DateTCreated" timestamp without time zone,
    "FileName" character varying(255),
    "RawBase64" text,
    "ClaimNumPreAuth" bigint
);


--
-- Name: equipment; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.equipment (
    "EquipmentNum" bigint NOT NULL,
    "Description" text,
    "SerialNumber" character varying(255),
    "ModelYear" character varying(2),
    "DatePurchased" date,
    "DateSold" date,
    "PurchaseCost" double precision,
    "MarketValue" double precision,
    "Location" text,
    "DateEntry" date,
    "ProvNumCheckedOut" bigint,
    "DateCheckedOut" date,
    "DateExpectedBack" date,
    "DispenseNote" text,
    "Status" text
);


--
-- Name: erouting; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.erouting (
    "ERoutingNum" bigint NOT NULL,
    "Description" character varying(255),
    "PatNum" bigint,
    "ClinicNum" bigint,
    "SecDateTEntry" timestamp without time zone,
    "IsComplete" boolean
);


--
-- Name: eroutingaction; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.eroutingaction (
    "ERoutingActionNum" bigint NOT NULL,
    "ERoutingNum" bigint,
    "ItemOrder" integer,
    "ERoutingActionType" boolean,
    "UserNum" bigint,
    "IsComplete" boolean,
    "DateTimeComplete" timestamp without time zone,
    "ForeignKeyType" boolean,
    "ForeignKey" bigint,
    "LabelOverride" character varying(255)
);


--
-- Name: erxlog; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.erxlog (
    "ErxLogNum" bigint NOT NULL,
    "PatNum" bigint,
    "MsgText" text,
    "DateTStamp" timestamp without time zone,
    "ProvNum" bigint,
    "UserNum" bigint
);


--
-- Name: eservicelog; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.eservicelog (
    "EServiceLogNum" bigint NOT NULL,
    "LogDateTime" timestamp without time zone,
    "PatNum" bigint,
    "EServiceType" smallint,
    "EServiceAction" smallint,
    "KeyType" smallint,
    "LogGuid" character varying(36),
    "ClinicNum" bigint,
    "FKey" bigint,
    "DateTimeUploaded" timestamp without time zone,
    "Note" character varying(255)
);


--
-- Name: eserviceshortguid; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.eserviceshortguid (
    "EServiceShortGuidNum" bigint NOT NULL,
    "EServiceCode" character varying(255),
    "ShortGuid" character varying(255),
    "ShortURL" character varying(255),
    "FKey" bigint,
    "FKeyType" character varying(255),
    "DateTimeExpiration" timestamp without time zone,
    "DateTEntry" timestamp without time zone
);


--
-- Name: eservicesignal; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.eservicesignal (
    "EServiceSignalNum" bigint NOT NULL,
    "ServiceCode" integer,
    "ReasonCategory" integer,
    "ReasonCode" integer,
    "Severity" smallint,
    "Description" text,
    "SigDateTime" timestamp without time zone,
    "Tag" text,
    "IsProcessed" boolean
);


--
-- Name: etl_load_status; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.etl_load_status (
    id integer NOT NULL,
    table_name character varying(255) NOT NULL,
    last_loaded timestamp without time zone DEFAULT '1970-01-01 00:00:01'::timestamp without time zone NOT NULL,
    last_primary_value character varying(255),
    primary_column_name character varying(255),
    rows_loaded integer DEFAULT 0,
    load_status character varying(50) DEFAULT 'pending'::character varying,
    _loaded_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: etl_load_status_id_seq; Type: SEQUENCE; Schema: raw; Owner: -
--

CREATE SEQUENCE raw.etl_load_status_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: etl_load_status_id_seq; Type: SEQUENCE OWNED BY; Schema: raw; Owner: -
--

ALTER SEQUENCE raw.etl_load_status_id_seq OWNED BY raw.etl_load_status.id;


--
-- Name: etl_pipeline_metrics; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.etl_pipeline_metrics (
    id integer NOT NULL,
    pipeline_id character varying(50) NOT NULL,
    start_time timestamp without time zone,
    end_time timestamp without time zone,
    total_time double precision,
    tables_processed integer DEFAULT 0,
    total_rows_processed integer DEFAULT 0,
    success boolean DEFAULT true,
    error_count integer DEFAULT 0,
    status character varying(20) DEFAULT 'idle'::character varying,
    metrics_json jsonb,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: etl_pipeline_metrics_id_seq; Type: SEQUENCE; Schema: raw; Owner: -
--

CREATE SEQUENCE raw.etl_pipeline_metrics_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: etl_pipeline_metrics_id_seq; Type: SEQUENCE OWNED BY; Schema: raw; Owner: -
--

ALTER SEQUENCE raw.etl_pipeline_metrics_id_seq OWNED BY raw.etl_pipeline_metrics.id;


--
-- Name: etl_table_metrics; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.etl_table_metrics (
    id integer NOT NULL,
    pipeline_id character varying(50) NOT NULL,
    table_name character varying(100) NOT NULL,
    rows_processed integer DEFAULT 0,
    processing_time double precision,
    success boolean DEFAULT true,
    error text,
    "timestamp" timestamp without time zone,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: etl_table_metrics_id_seq; Type: SEQUENCE; Schema: raw; Owner: -
--

CREATE SEQUENCE raw.etl_table_metrics_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: etl_table_metrics_id_seq; Type: SEQUENCE OWNED BY; Schema: raw; Owner: -
--

ALTER SEQUENCE raw.etl_table_metrics_id_seq OWNED BY raw.etl_table_metrics.id;


--
-- Name: etl_transform_status; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.etl_transform_status (
    id integer NOT NULL,
    table_name character varying(255) NOT NULL,
    last_transformed timestamp without time zone DEFAULT '1970-01-01 00:00:01'::timestamp without time zone NOT NULL,
    last_primary_value character varying(255),
    primary_column_name character varying(255),
    rows_transformed integer DEFAULT 0,
    transform_status character varying(50) DEFAULT 'pending'::character varying,
    _transformed_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    _created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    _updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: etl_transform_status_id_seq; Type: SEQUENCE; Schema: raw; Owner: -
--

CREATE SEQUENCE raw.etl_transform_status_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: etl_transform_status_id_seq; Type: SEQUENCE OWNED BY; Schema: raw; Owner: -
--

ALTER SEQUENCE raw.etl_transform_status_id_seq OWNED BY raw.etl_transform_status.id;


--
-- Name: etrans; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.etrans (
    "EtransNum" bigint NOT NULL,
    "DateTimeTrans" timestamp without time zone,
    "ClearingHouseNum" bigint,
    "Etype" smallint,
    "ClaimNum" bigint,
    "OfficeSequenceNumber" integer,
    "CarrierTransCounter" integer,
    "CarrierTransCounter2" integer,
    "CarrierNum" bigint,
    "CarrierNum2" bigint,
    "PatNum" bigint,
    "BatchNumber" integer,
    "AckCode" character varying(255),
    "TransSetNum" integer,
    "Note" text,
    "EtransMessageTextNum" bigint,
    "AckEtransNum" bigint,
    "PlanNum" bigint,
    "InsSubNum" bigint,
    "TranSetId835" character varying(255),
    "CarrierNameRaw" character varying(60),
    "PatientNameRaw" character varying(133),
    "UserNum" bigint
);


--
-- Name: etrans835; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.etrans835 (
    "Etrans835Num" bigint NOT NULL,
    "EtransNum" bigint,
    "PayerName" character varying(60),
    "TransRefNum" character varying(50),
    "InsPaid" double precision,
    "ControlId" character varying(9),
    "PaymentMethodCode" character varying(3),
    "PatientName" character varying(100),
    "Status" boolean,
    "AutoProcessed" boolean,
    "IsApproved" boolean
);


--
-- Name: etrans835attach; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.etrans835attach (
    "Etrans835AttachNum" bigint NOT NULL,
    "EtransNum" bigint,
    "ClaimNum" bigint,
    "ClpSegmentIndex" integer,
    "DateTimeEntry" timestamp without time zone
);


--
-- Name: etransmessagetext; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.etransmessagetext (
    "EtransMessageTextNum" bigint NOT NULL,
    "MessageText" text
);


--
-- Name: evaluation; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.evaluation (
    "EvaluationNum" bigint NOT NULL,
    "InstructNum" bigint,
    "StudentNum" bigint,
    "SchoolCourseNum" bigint,
    "EvalTitle" character varying(255),
    "DateEval" date,
    "GradingScaleNum" bigint,
    "OverallGradeShowing" character varying(255),
    "OverallGradeNumber" real,
    "Notes" text
);


--
-- Name: evaluationcriterion; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.evaluationcriterion (
    "EvaluationCriterionNum" bigint NOT NULL,
    "EvaluationNum" bigint,
    "CriterionDescript" character varying(255),
    "IsCategoryName" boolean,
    "GradingScaleNum" bigint,
    "GradeShowing" character varying(255),
    "GradeNumber" real,
    "Notes" text,
    "ItemOrder" integer,
    "MaxPointsPoss" real
);


--
-- Name: famaging; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.famaging (
    "PatNum" bigint NOT NULL,
    "Bal_0_30" double precision,
    "Bal_31_60" double precision,
    "Bal_61_90" double precision,
    "BalOver90" double precision,
    "InsEst" double precision,
    "BalTotal" double precision,
    "PayPlanDue" double precision
);


--
-- Name: familyhealth; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.familyhealth (
    "FamilyHealthNum" bigint NOT NULL,
    "PatNum" bigint,
    "Relationship" boolean,
    "DiseaseDefNum" bigint,
    "PersonName" character varying(255)
);


--
-- Name: fee; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.fee (
    "FeeNum" bigint NOT NULL,
    "Amount" double precision,
    "OldCode" character varying(15),
    "FeeSched" bigint,
    "UseDefaultFee" boolean,
    "UseDefaultCov" boolean,
    "CodeNum" bigint,
    "ClinicNum" bigint,
    "ProvNum" bigint,
    "SecUserNumEntry" bigint,
    "SecDateEntry" date,
    "SecDateTEdit" timestamp without time zone,
    "DateEffective" date
);


--
-- Name: feesched; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.feesched (
    "FeeSchedNum" bigint NOT NULL,
    "Description" character varying(255),
    "FeeSchedType" integer,
    "ItemOrder" integer,
    "IsHidden" boolean,
    "IsGlobal" boolean,
    "SecUserNumEntry" bigint,
    "SecDateEntry" date,
    "SecDateTEdit" timestamp without time zone
);


--
-- Name: feeschedgroup; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.feeschedgroup (
    "FeeSchedGroupNum" bigint NOT NULL,
    "Description" character varying(255),
    "FeeSchedNum" bigint,
    "ClinicNums" character varying(255)
);


--
-- Name: fhircontactpoint; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.fhircontactpoint (
    "FHIRContactPointNum" bigint NOT NULL,
    "FHIRSubscriptionNum" bigint,
    "ContactSystem" boolean,
    "ContactValue" character varying(255),
    "ContactUse" boolean,
    "ItemOrder" integer,
    "DateStart" date,
    "DateEnd" date
);


--
-- Name: fhirsubscription; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.fhirsubscription (
    "FHIRSubscriptionNum" bigint NOT NULL,
    "Criteria" character varying(255),
    "Reason" character varying(255),
    "SubStatus" boolean,
    "ErrorNote" text,
    "ChannelType" boolean,
    "ChannelEndpoint" character varying(255),
    "ChannelPayLoad" character varying(255),
    "ChannelHeader" character varying(255),
    "DateEnd" timestamp without time zone,
    "APIKeyHash" character varying(255)
);


--
-- Name: files; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.files (
    "DocNum" bigint NOT NULL,
    "Data" bytea,
    "Thumbnail" bytea
);


--
-- Name: formpat; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.formpat (
    "FormPatNum" bigint NOT NULL,
    "PatNum" bigint,
    "FormDateTime" timestamp without time zone
);


--
-- Name: gradingscale; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.gradingscale (
    "GradingScaleNum" bigint NOT NULL,
    "Description" character varying(255),
    "ScaleType" boolean
);


--
-- Name: gradingscaleitem; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.gradingscaleitem (
    "GradingScaleItemNum" bigint NOT NULL,
    "GradingScaleNum" bigint,
    "GradeShowing" character varying(255),
    "GradeNumber" real,
    "Description" character varying(255)
);


--
-- Name: grouppermission; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.grouppermission (
    "GroupPermNum" bigint NOT NULL,
    "NewerDate" date,
    "NewerDays" integer,
    "UserGroupNum" bigint,
    "PermType" smallint,
    "FKey" bigint
);


--
-- Name: guardian; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.guardian (
    "GuardianNum" bigint NOT NULL,
    "PatNumChild" bigint,
    "PatNumGuardian" bigint,
    "Relationship" smallint,
    "IsGuardian" boolean
);


--
-- Name: hcpcs; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.hcpcs (
    "HcpcsNum" bigint NOT NULL,
    "HcpcsCode" character varying(255),
    "DescriptionShort" character varying(255)
);


--
-- Name: hieclinic; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.hieclinic (
    "HieClinicNum" bigint NOT NULL,
    "ClinicNum" bigint,
    "SupportedCarrierFlags" boolean,
    "PathExportCCD" character varying(255),
    "TimeOfDayExportCCD" bigint,
    "IsEnabled" boolean
);


--
-- Name: hiequeue; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.hiequeue (
    "HieQueueNum" bigint NOT NULL,
    "PatNum" bigint
);


--
-- Name: histappointment; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.histappointment (
    "HistApptNum" bigint NOT NULL,
    "HistUserNum" bigint,
    "HistDateTStamp" timestamp without time zone,
    "HistApptAction" smallint,
    "ApptSource" smallint,
    "AptNum" bigint,
    "PatNum" bigint,
    "AptStatus" smallint,
    "Pattern" character varying(255),
    "Confirmed" bigint,
    "TimeLocked" boolean,
    "Op" bigint,
    "Note" text,
    "ProvNum" bigint,
    "ProvHyg" bigint,
    "AptDateTime" timestamp without time zone,
    "NextAptNum" bigint,
    "UnschedStatus" bigint,
    "IsNewPatient" boolean,
    "ProcDescript" character varying(255),
    "Assistant" bigint,
    "ClinicNum" bigint,
    "IsHygiene" boolean,
    "DateTStamp" timestamp without time zone,
    "DateTimeArrived" timestamp without time zone,
    "DateTimeSeated" timestamp without time zone,
    "DateTimeDismissed" timestamp without time zone,
    "InsPlan1" bigint,
    "InsPlan2" bigint,
    "DateTimeAskedToArrive" timestamp without time zone,
    "ProcsColored" text,
    "ColorOverride" integer,
    "AppointmentTypeNum" bigint,
    "SecUserNumEntry" bigint,
    "SecDateTEntry" timestamp without time zone,
    "Priority" boolean,
    "ProvBarText" character varying(60),
    "PatternSecondary" character varying(255),
    "SecurityHash" character varying(255),
    "ItemOrderPlanned" integer,
    "IsMirrored" boolean
);


--
-- Name: hl7msg; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.hl7msg (
    "HL7MsgNum" bigint NOT NULL,
    "HL7Status" integer,
    "MsgText" text,
    "AptNum" bigint,
    "DateTStamp" timestamp without time zone,
    "PatNum" bigint,
    "Note" text
);


--
-- Name: hl7procattach; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.hl7procattach (
    "HL7ProcAttachNum" bigint NOT NULL,
    "HL7MsgNum" bigint,
    "ProcNum" bigint
);


--
-- Name: icd10; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.icd10 (
    "Icd10Num" bigint NOT NULL,
    "Icd10Code" character varying(255),
    "Description" character varying(255),
    "IsCode" character varying(255)
);


--
-- Name: icd9; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.icd9 (
    "ICD9Num" bigint NOT NULL,
    "ICD9Code" character varying(255),
    "Description" character varying(255),
    "DateTStamp" timestamp without time zone
);


--
-- Name: imagedraw; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.imagedraw (
    "ImageDrawNum" bigint NOT NULL,
    "DocNum" bigint,
    "MountNum" bigint,
    "ColorDraw" integer,
    "ColorBack" integer,
    "DrawingSegment" text,
    "DrawText" character varying(255),
    "FontSize" real,
    "DrawType" smallint,
    "ImageAnnotVendor" boolean,
    "Details" text,
    "PearlLayer" boolean,
    "BetterDiagLayer" boolean
);


--
-- Name: imagingdevice; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.imagingdevice (
    "ImagingDeviceNum" bigint NOT NULL,
    "Description" character varying(255),
    "ComputerName" character varying(255),
    "DeviceType" smallint,
    "TwainName" character varying(255),
    "ItemOrder" integer,
    "ShowTwainUI" boolean
);


--
-- Name: insbluebook; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.insbluebook (
    "InsBlueBookNum" bigint NOT NULL,
    "ProcCodeNum" bigint,
    "CarrierNum" bigint,
    "PlanNum" bigint,
    "GroupNum" character varying(25),
    "InsPayAmt" double precision,
    "AllowedOverride" double precision,
    "DateTEntry" timestamp without time zone,
    "ProcNum" bigint,
    "ProcDate" date,
    "ClaimType" character varying(10),
    "ClaimNum" bigint
);


--
-- Name: insbluebooklog; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.insbluebooklog (
    "InsBlueBookLogNum" bigint NOT NULL,
    "ClaimProcNum" bigint,
    "AllowedFee" double precision,
    "DateTEntry" timestamp without time zone,
    "Description" text
);


--
-- Name: insbluebookrule; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.insbluebookrule (
    "InsBlueBookRuleNum" bigint NOT NULL,
    "ItemOrder" smallint,
    "RuleType" smallint,
    "LimitValue" integer,
    "LimitType" boolean
);


--
-- Name: inseditlog; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.inseditlog (
    "InsEditLogNum" bigint NOT NULL,
    "FKey" bigint,
    "LogType" smallint,
    "FieldName" character varying(255),
    "OldValue" character varying(255),
    "NewValue" character varying(255),
    "UserNum" bigint,
    "DateTStamp" timestamp without time zone,
    "ParentKey" bigint,
    "Description" character varying(255)
);


--
-- Name: inseditpatlog; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.inseditpatlog (
    "InsEditPatLogNum" bigint NOT NULL,
    "FKey" bigint,
    "LogType" smallint,
    "FieldName" character varying(255),
    "OldValue" character varying(255),
    "NewValue" character varying(255),
    "UserNum" bigint,
    "DateTStamp" timestamp without time zone,
    "ParentKey" bigint,
    "Description" character varying(255)
);


--
-- Name: insfilingcode; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.insfilingcode (
    "InsFilingCodeNum" bigint NOT NULL,
    "Descript" character varying(255),
    "EclaimCode" character varying(100),
    "ItemOrder" integer,
    "GroupType" bigint,
    "ExcludeOtherCoverageOnPriClaims" boolean
);


--
-- Name: insplan; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.insplan (
    "PlanNum" bigint NOT NULL,
    "GroupName" character varying(50),
    "GroupNum" character varying(25),
    "PlanNote" text,
    "FeeSched" bigint,
    "PlanType" character(1),
    "ClaimFormNum" bigint,
    "UseAltCode" boolean,
    "ClaimsUseUCR" boolean,
    "CopayFeeSched" bigint,
    "EmployerNum" bigint,
    "CarrierNum" bigint,
    "AllowedFeeSched" bigint,
    "TrojanID" character varying(100),
    "DivisionNo" character varying(255),
    "IsMedical" boolean,
    "FilingCode" bigint,
    "DentaideCardSequence" boolean,
    "ShowBaseUnits" boolean,
    "CodeSubstNone" boolean,
    "IsHidden" boolean,
    "MonthRenew" smallint,
    "FilingCodeSubtype" bigint,
    "CanadianPlanFlag" character varying(5),
    "CanadianDiagnosticCode" character varying(255),
    "CanadianInstitutionCode" character varying(255),
    "RxBIN" character varying(255),
    "CobRule" smallint,
    "SopCode" character varying(255),
    "SecUserNumEntry" bigint,
    "SecDateEntry" date,
    "SecDateTEdit" timestamp without time zone,
    "HideFromVerifyList" boolean,
    "OrthoType" boolean,
    "OrthoAutoProcFreq" boolean,
    "OrthoAutoProcCodeNumOverride" bigint,
    "OrthoAutoFeeBilled" double precision,
    "OrthoAutoClaimDaysWait" integer,
    "BillingType" bigint,
    "HasPpoSubstWriteoffs" boolean,
    "ExclusionFeeRule" boolean,
    "ManualFeeSchedNum" bigint,
    "IsBlueBookEnabled" boolean,
    "InsPlansZeroWriteOffsOnAnnualMaxOverride" boolean,
    "InsPlansZeroWriteOffsOnFreqOrAgingOverride" boolean,
    "PerVisitPatAmount" double precision,
    "PerVisitInsAmount" double precision
);


--
-- Name: insplanpreference; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.insplanpreference (
    "InsPlanPrefNum" bigint NOT NULL,
    "PlanNum" bigint,
    "FKey" bigint,
    "FKeyType" boolean,
    "ValueString" text
);


--
-- Name: inssub; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.inssub (
    "InsSubNum" bigint NOT NULL,
    "PlanNum" bigint,
    "Subscriber" bigint,
    "DateEffective" date,
    "DateTerm" date,
    "ReleaseInfo" boolean,
    "AssignBen" boolean,
    "SubscriberID" character varying(255),
    "BenefitNotes" text,
    "SubscNote" text,
    "SecUserNumEntry" bigint,
    "SecDateEntry" date,
    "SecDateTEdit" timestamp without time zone,
    "SecurityHash" character varying(255)
);


--
-- Name: installmentplan; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.installmentplan (
    "InstallmentPlanNum" bigint NOT NULL,
    "PatNum" bigint,
    "DateAgreement" date,
    "DateFirstPayment" date,
    "MonthlyPayment" double precision,
    "APR" real,
    "Note" character varying(255)
);


--
-- Name: instructor; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.instructor (
    "InstructorNum" integer NOT NULL,
    "LName" character varying(255),
    "FName" character varying(255),
    "Suffix" character varying(100)
);


--
-- Name: insverify; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.insverify (
    "InsVerifyNum" bigint NOT NULL,
    "DateLastVerified" date,
    "UserNum" bigint,
    "VerifyType" smallint,
    "FKey" bigint,
    "DefNum" bigint,
    "Note" text,
    "DateLastAssigned" date,
    "DateTimeEntry" timestamp without time zone,
    "HoursAvailableForVerification" double precision,
    "SecDateTEdit" timestamp without time zone,
    "SecurityHash" character varying(255)
);


--
-- Name: insverifyhist; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.insverifyhist (
    "InsVerifyHistNum" bigint NOT NULL,
    "InsVerifyNum" bigint,
    "DateLastVerified" date,
    "UserNum" bigint,
    "VerifyType" smallint,
    "FKey" bigint,
    "DefNum" bigint,
    "Note" text,
    "DateLastAssigned" date,
    "DateTimeEntry" timestamp without time zone,
    "HoursAvailableForVerification" double precision,
    "VerifyUserNum" bigint,
    "SecDateTEdit" timestamp without time zone,
    "SecurityHash" character varying(255)
);


--
-- Name: intervention; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.intervention (
    "InterventionNum" bigint NOT NULL,
    "PatNum" bigint,
    "ProvNum" bigint,
    "CodeValue" character varying(30),
    "CodeSystem" character varying(30),
    "Note" text,
    "DateEntry" date,
    "CodeSet" boolean,
    "IsPatDeclined" boolean
);


--
-- Name: journalentry; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.journalentry (
    "JournalEntryNum" bigint NOT NULL,
    "TransactionNum" bigint,
    "AccountNum" bigint,
    "DateDisplayed" date,
    "DebitAmt" double precision,
    "CreditAmt" double precision,
    "Memo" text,
    "Splits" text,
    "CheckNumber" character varying(255),
    "ReconcileNum" bigint,
    "SecUserNumEntry" bigint,
    "SecDateTEntry" timestamp without time zone,
    "SecUserNumEdit" bigint,
    "SecDateTEdit" timestamp without time zone
);


--
-- Name: labcase; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.labcase (
    "LabCaseNum" bigint NOT NULL,
    "PatNum" bigint,
    "LaboratoryNum" bigint,
    "AptNum" bigint,
    "PlannedAptNum" bigint,
    "DateTimeDue" timestamp without time zone,
    "DateTimeCreated" timestamp without time zone,
    "DateTimeSent" timestamp without time zone,
    "DateTimeRecd" timestamp without time zone,
    "DateTimeChecked" timestamp without time zone,
    "ProvNum" bigint,
    "Instructions" text,
    "LabFee" double precision,
    "DateTStamp" timestamp without time zone,
    "InvoiceNum" character varying(255)
);


--
-- Name: laboratory; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.laboratory (
    "LaboratoryNum" bigint NOT NULL,
    "Description" character varying(255),
    "Phone" character varying(255),
    "Notes" text,
    "Slip" bigint,
    "Address" character varying(255),
    "City" character varying(255),
    "State" character varying(255),
    "Zip" character varying(255),
    "Email" character varying(255),
    "WirelessPhone" character varying(255),
    "IsHidden" boolean
);


--
-- Name: labpanel; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.labpanel (
    "LabPanelNum" bigint NOT NULL,
    "PatNum" bigint,
    "RawMessage" text,
    "LabNameAddress" character varying(255),
    "DateTStamp" timestamp without time zone,
    "SpecimenCondition" character varying(255),
    "SpecimenSource" character varying(255),
    "ServiceId" character varying(255),
    "ServiceName" character varying(255),
    "MedicalOrderNum" bigint
);


--
-- Name: labresult; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.labresult (
    "LabResultNum" bigint NOT NULL,
    "LabPanelNum" bigint,
    "DateTimeTest" timestamp without time zone,
    "TestName" character varying(255),
    "DateTStamp" timestamp without time zone,
    "TestID" character varying(255),
    "ObsValue" character varying(255),
    "ObsUnits" character varying(255),
    "ObsRange" character varying(255),
    "AbnormalFlag" boolean
);


--
-- Name: labturnaround; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.labturnaround (
    "LabTurnaroundNum" bigint NOT NULL,
    "LaboratoryNum" bigint,
    "Description" character varying(255),
    "DaysPublished" smallint,
    "DaysActual" smallint
);


--
-- Name: language; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.language (
    "LanguageNum" bigint NOT NULL,
    "EnglishComments" text,
    "ClassType" text,
    "English" text,
    "IsObsolete" boolean
);


--
-- Name: languageforeign; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.languageforeign (
    "LanguageForeignNum" bigint NOT NULL,
    "ClassType" text,
    "English" text,
    "Culture" character varying(255),
    "Translation" text,
    "Comments" text
);


--
-- Name: languagepat; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.languagepat (
    "LanguagePatNum" bigint NOT NULL,
    "PrefName" character varying(255),
    "Language" character varying(255),
    "Translation" text,
    "EFormFieldDefNum" bigint
);


--
-- Name: letter; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.letter (
    "LetterNum" bigint NOT NULL,
    "Description" character varying(255),
    "BodyText" text
);


--
-- Name: lettermerge; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.lettermerge (
    "LetterMergeNum" bigint NOT NULL,
    "Description" character varying(255),
    "TemplateName" character varying(255),
    "DataFileName" character varying(255),
    "Category" bigint,
    "ImageFolder" bigint
);


--
-- Name: lettermergefield; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.lettermergefield (
    "FieldNum" bigint NOT NULL,
    "LetterMergeNum" bigint,
    "FieldName" character varying(255)
);


--
-- Name: limitedbetafeature; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.limitedbetafeature (
    "LimitedBetaFeatureNum" bigint NOT NULL,
    "LimitedBetaFeatureTypeNum" bigint,
    "ClinicNum" bigint,
    "IsSignedUp" boolean
);


--
-- Name: loginattempt; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.loginattempt (
    "LoginAttemptNum" bigint NOT NULL,
    "UserName" character varying(255),
    "LoginType" boolean,
    "DateTFail" timestamp without time zone
);


--
-- Name: loinc; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.loinc (
    "LoincNum" bigint NOT NULL,
    "LoincCode" character varying(255),
    "Component" character varying(255),
    "PropertyObserved" character varying(255),
    "TimeAspct" character varying(255),
    "SystemMeasured" character varying(255),
    "ScaleType" character varying(255),
    "MethodType" character varying(255),
    "StatusOfCode" character varying(255),
    "NameShort" character varying(255),
    "ClassType" character varying(255),
    "UnitsRequired" boolean,
    "OrderObs" character varying(255),
    "HL7FieldSubfieldID" character varying(255),
    "ExternalCopyrightNotice" text,
    "NameLongCommon" character varying(255),
    "UnitsUCUM" character varying(255),
    "RankCommonTests" integer,
    "RankCommonOrders" integer
);


--
-- Name: maparea; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.maparea (
    "MapAreaNum" bigint NOT NULL,
    "Extension" integer,
    "XPos" double precision,
    "YPos" double precision,
    "Width" double precision,
    "Height" double precision,
    "Description" character varying(255),
    "ItemType" boolean,
    "MapAreaContainerNum" bigint
);


--
-- Name: medicalorder; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.medicalorder (
    "MedicalOrderNum" bigint NOT NULL,
    "MedOrderType" boolean,
    "PatNum" bigint,
    "DateTimeOrder" timestamp without time zone,
    "Description" character varying(255),
    "IsDiscontinued" boolean,
    "ProvNum" bigint
);


--
-- Name: medication; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.medication (
    "MedicationNum" bigint NOT NULL,
    "MedName" character varying(255),
    "GenericNum" bigint,
    "Notes" text,
    "DateTStamp" timestamp without time zone,
    "RxCui" bigint,
    "IsHidden" boolean
);


--
-- Name: medicationpat; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.medicationpat (
    "MedicationPatNum" bigint NOT NULL,
    "PatNum" bigint,
    "MedicationNum" bigint,
    "PatNote" text,
    "DateTStamp" timestamp without time zone,
    "DateStart" date,
    "DateStop" date,
    "ProvNum" bigint,
    "MedDescript" character varying(255),
    "RxCui" bigint,
    "ErxGuid" character varying(255),
    "IsCpoe" boolean
);


--
-- Name: medlab; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.medlab (
    "MedLabNum" bigint NOT NULL,
    "SendingApp" character varying(255),
    "SendingFacility" character varying(255),
    "PatNum" bigint,
    "ProvNum" bigint,
    "PatIDLab" character varying(255),
    "PatIDAlt" character varying(255),
    "PatAge" character varying(255),
    "PatAccountNum" character varying(255),
    "PatFasting" boolean,
    "SpecimenID" character varying(255),
    "SpecimenIDFiller" character varying(255),
    "ObsTestID" character varying(255),
    "ObsTestDescript" character varying(255),
    "ObsTestLoinc" character varying(255),
    "ObsTestLoincText" character varying(255),
    "DateTimeCollected" timestamp without time zone,
    "TotalVolume" character varying(255),
    "ActionCode" character varying(255),
    "ClinicalInfo" character varying(255),
    "DateTimeEntered" timestamp without time zone,
    "OrderingProvNPI" character varying(255),
    "OrderingProvLocalID" character varying(255),
    "OrderingProvLName" character varying(255),
    "OrderingProvFName" character varying(255),
    "SpecimenIDAlt" character varying(255),
    "DateTimeReported" timestamp without time zone,
    "ResultStatus" character varying(255),
    "ParentObsID" character varying(255),
    "ParentObsTestID" character varying(255),
    "NotePat" text,
    "NoteLab" text,
    "FileName" character varying(255),
    "OriginalPIDSegment" text
);


--
-- Name: medlabfacattach; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.medlabfacattach (
    "MedLabFacAttachNum" bigint NOT NULL,
    "MedLabNum" bigint,
    "MedLabResultNum" bigint,
    "MedLabFacilityNum" bigint
);


--
-- Name: medlabfacility; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.medlabfacility (
    "MedLabFacilityNum" bigint NOT NULL,
    "FacilityName" character varying(255),
    "Address" character varying(255),
    "City" character varying(255),
    "State" character varying(255),
    "Zip" character varying(255),
    "Phone" character varying(255),
    "DirectorTitle" character varying(255),
    "DirectorLName" character varying(255),
    "DirectorFName" character varying(255)
);


--
-- Name: medlabresult; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.medlabresult (
    "MedLabResultNum" bigint NOT NULL,
    "MedLabNum" bigint,
    "ObsID" character varying(255),
    "ObsText" character varying(255),
    "ObsLoinc" character varying(255),
    "ObsLoincText" character varying(255),
    "ObsIDSub" character varying(255),
    "ObsValue" text,
    "ObsSubType" character varying(255),
    "ObsUnits" character varying(255),
    "ReferenceRange" character varying(255),
    "AbnormalFlag" character varying(255),
    "ResultStatus" character varying(255),
    "DateTimeObs" timestamp without time zone,
    "FacilityID" character varying(255),
    "DocNum" bigint,
    "Note" text
);


--
-- Name: medlabspecimen; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.medlabspecimen (
    "MedLabSpecimenNum" bigint NOT NULL,
    "MedLabNum" bigint,
    "SpecimenID" character varying(255),
    "SpecimenDescript" character varying(255),
    "DateTimeCollected" timestamp without time zone
);


--
-- Name: mobileappdevice; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.mobileappdevice (
    "MobileAppDeviceNum" bigint NOT NULL,
    "ClinicNum" bigint,
    "DeviceName" character varying(255),
    "UniqueID" character varying(255),
    "IsEclipboardEnabled" boolean,
    "EclipboardLastAttempt" timestamp without time zone,
    "EclipboardLastLogin" timestamp without time zone,
    "PatNum" bigint,
    "LastCheckInActivity" timestamp without time zone,
    "IsBYODDevice" boolean,
    "DevicePage" smallint,
    "UserNum" bigint,
    "IsODTouchEnabled" boolean,
    "ODTouchLastLogin" timestamp without time zone,
    "ODTouchLastAttempt" timestamp without time zone
);


--
-- Name: mobilebrandingprofile; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.mobilebrandingprofile (
    "MobileBrandingProfileNum" bigint NOT NULL,
    "ClinicNum" bigint,
    "OfficeDescription" character varying(255),
    "LogoFilePath" character varying(255),
    "DateTStamp" timestamp without time zone
);


--
-- Name: mobiledatabyte; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.mobiledatabyte (
    "MobileDataByteNum" bigint NOT NULL,
    "RawBase64Data" text,
    "RawBase64Code" text,
    "RawBase64Tag" text,
    "PatNum" bigint,
    "ActionType" boolean,
    "DateTimeEntry" timestamp without time zone,
    "DateTimeExpires" timestamp without time zone
);


--
-- Name: mobilenotification; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.mobilenotification (
    "MobileNotificationNum" bigint NOT NULL,
    "NotificationType" boolean,
    "DeviceId" character varying(255),
    "PrimaryKeys" text,
    "Tags" text,
    "DateTimeEntry" timestamp without time zone,
    "DateTimeExpires" timestamp without time zone,
    "AppTarget" boolean
);


--
-- Name: mount; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.mount (
    "MountNum" bigint NOT NULL,
    "PatNum" bigint,
    "DocCategory" bigint,
    "DateCreated" timestamp without time zone,
    "Description" character varying(255),
    "Note" text,
    "Width" integer,
    "Height" integer,
    "ColorBack" integer,
    "ProvNum" bigint,
    "ColorFore" integer,
    "ColorTextBack" integer,
    "FlipOnAcquire" boolean,
    "AdjModeAfterSeries" boolean
);


--
-- Name: mountdef; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.mountdef (
    "MountDefNum" bigint NOT NULL,
    "Description" character varying(255),
    "ItemOrder" integer,
    "Width" integer,
    "Height" integer,
    "ColorBack" integer,
    "ColorFore" integer,
    "ColorTextBack" integer,
    "ScaleValue" character varying(255),
    "DefaultCat" bigint,
    "FlipOnAcquire" boolean,
    "AdjModeAfterSeries" boolean
);


--
-- Name: mountitem; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.mountitem (
    "MountItemNum" bigint NOT NULL,
    "MountNum" bigint,
    "Xpos" integer,
    "Ypos" integer,
    "ItemOrder" integer,
    "Width" integer,
    "Height" integer,
    "RotateOnAcquire" integer,
    "ToothNumbers" character varying(255),
    "TextShowing" text,
    "FontSize" real
);


--
-- Name: mountitemdef; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.mountitemdef (
    "MountItemDefNum" bigint NOT NULL,
    "MountDefNum" bigint,
    "Xpos" integer,
    "Ypos" integer,
    "Width" integer,
    "Height" integer,
    "ItemOrder" integer,
    "RotateOnAcquire" integer,
    "ToothNumbers" character varying(255),
    "TextShowing" text,
    "FontSize" real
);


--
-- Name: msgtopaysent; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.msgtopaysent (
    "MsgToPaySentNum" bigint NOT NULL,
    "PatNum" bigint,
    "ClinicNum" bigint,
    "SendStatus" boolean,
    "Source" boolean,
    "MessageType" boolean,
    "MessageFk" bigint,
    "Subject" text,
    "Message" text,
    "EmailType" boolean,
    "DateTimeEntry" timestamp without time zone,
    "DateTimeSent" timestamp without time zone,
    "ResponseDescript" text,
    "ApptReminderRuleNum" bigint,
    "ShortGUID" character varying(255),
    "DateTimeSendFailed" timestamp without time zone,
    "ApptNum" bigint,
    "ApptDateTime" timestamp without time zone,
    "TSPrior" bigint,
    "StatementNum" bigint
);


--
-- Name: oidexternal; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.oidexternal (
    "OIDExternalNum" bigint NOT NULL,
    "IDType" character varying(255),
    "IDInternal" bigint,
    "IDExternal" character varying(255),
    "rootExternal" character varying(255)
);


--
-- Name: oidinternal; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.oidinternal (
    "OIDInternalNum" bigint NOT NULL,
    "IDType" character varying(255),
    "IDRoot" character varying(255)
);


--
-- Name: operatory; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.operatory (
    "OperatoryNum" bigint NOT NULL,
    "OpName" character varying(255),
    "Abbrev" character varying(255),
    "ItemOrder" smallint,
    "IsHidden" boolean,
    "ProvDentist" bigint,
    "ProvHygienist" bigint,
    "IsHygiene" boolean,
    "ClinicNum" bigint,
    "DateTStamp" timestamp without time zone,
    "SetProspective" boolean,
    "IsWebSched" boolean,
    "IsNewPatAppt" boolean,
    "OperatoryType" bigint
);


--
-- Name: orionproc; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.orionproc (
    "OrionProcNum" bigint NOT NULL,
    "ProcNum" bigint,
    "DPC" boolean,
    "DateScheduleBy" date,
    "DateStopClock" date,
    "Status2" integer,
    "IsOnCall" boolean,
    "IsEffectiveComm" boolean,
    "IsRepair" boolean,
    "DPCpost" boolean
);


--
-- Name: orthocase; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.orthocase (
    "OrthoCaseNum" bigint NOT NULL,
    "PatNum" bigint,
    "ProvNum" bigint,
    "ClinicNum" bigint,
    "Fee" double precision,
    "FeeInsPrimary" double precision,
    "FeePat" double precision,
    "BandingDate" date,
    "DebondDate" date,
    "DebondDateExpected" date,
    "IsTransfer" boolean,
    "OrthoType" bigint,
    "SecDateTEntry" timestamp without time zone,
    "SecUserNumEntry" bigint,
    "SecDateTEdit" timestamp without time zone,
    "IsActive" boolean,
    "FeeInsSecondary" double precision
);


--
-- Name: orthochart; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.orthochart (
    "OrthoChartNum" bigint NOT NULL,
    "PatNum" bigint,
    "DateService" date,
    "FieldName" character varying(255),
    "FieldValue" text,
    "UserNum" bigint,
    "ProvNum" bigint,
    "OrthoChartRowNum" bigint
);


--
-- Name: orthochartlog; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.orthochartlog (
    "OrthoChartLogNum" bigint NOT NULL,
    "PatNum" bigint,
    "ComputerName" character varying(255),
    "DateTimeLog" timestamp without time zone,
    "DateTimeService" timestamp without time zone,
    "UserNum" bigint,
    "ProvNum" bigint,
    "OrthoChartRowNum" bigint,
    "LogData" text
);


--
-- Name: orthochartrow; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.orthochartrow (
    "OrthoChartRowNum" bigint NOT NULL,
    "PatNum" bigint,
    "DateTimeService" timestamp without time zone,
    "UserNum" bigint,
    "ProvNum" bigint,
    "Signature" text
);


--
-- Name: orthocharttab; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.orthocharttab (
    "OrthoChartTabNum" bigint NOT NULL,
    "TabName" character varying(255),
    "ItemOrder" integer,
    "IsHidden" boolean
);


--
-- Name: orthocharttablink; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.orthocharttablink (
    "OrthoChartTabLinkNum" bigint NOT NULL,
    "ItemOrder" integer,
    "OrthoChartTabNum" bigint,
    "DisplayFieldNum" bigint,
    "ColumnWidthOverride" integer
);


--
-- Name: orthohardware; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.orthohardware (
    "OrthoHardwareNum" bigint NOT NULL,
    "PatNum" bigint,
    "DateExam" date,
    "OrthoHardwareType" boolean,
    "OrthoHardwareSpecNum" bigint,
    "ToothRange" character varying(255),
    "Note" character varying(255),
    "IsHidden" boolean
);


--
-- Name: orthohardwarespec; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.orthohardwarespec (
    "OrthoHardwareSpecNum" bigint NOT NULL,
    "OrthoHardwareType" smallint,
    "Description" character varying(255),
    "ItemColor" integer,
    "IsHidden" boolean,
    "ItemOrder" integer
);


--
-- Name: orthoplanlink; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.orthoplanlink (
    "OrthoPlanLinkNum" bigint NOT NULL,
    "OrthoCaseNum" bigint,
    "LinkType" boolean,
    "FKey" bigint,
    "IsActive" boolean,
    "SecDateTEntry" timestamp without time zone,
    "SecUserNumEntry" bigint
);


--
-- Name: orthoproclink; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.orthoproclink (
    "OrthoProcLinkNum" bigint NOT NULL,
    "OrthoCaseNum" bigint,
    "ProcNum" bigint,
    "SecDateTEntry" timestamp without time zone,
    "SecUserNumEntry" bigint,
    "ProcLinkType" boolean
);


--
-- Name: orthorx; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.orthorx (
    "OrthoRxNum" bigint NOT NULL,
    "OrthoHardwareSpecNum" bigint,
    "Description" character varying(255),
    "ToothRange" character varying(255),
    "ItemOrder" integer
);


--
-- Name: orthoschedule; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.orthoschedule (
    "OrthoScheduleNum" bigint NOT NULL,
    "BandingDateOverride" date,
    "DebondDateOverride" date,
    "BandingAmount" double precision,
    "VisitAmount" double precision,
    "DebondAmount" double precision,
    "IsActive" boolean,
    "SecDateTEdit" timestamp without time zone
);


--
-- Name: patfield; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.patfield (
    "PatFieldNum" bigint NOT NULL,
    "PatNum" bigint,
    "FieldName" character varying(255),
    "FieldValue" text,
    "SecUserNumEntry" bigint,
    "SecDateEntry" date,
    "SecDateTEdit" timestamp without time zone
);


--
-- Name: patfielddef; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.patfielddef (
    "PatFieldDefNum" bigint NOT NULL,
    "FieldName" character varying(255),
    "FieldType" smallint,
    "PickList" text,
    "ItemOrder" integer,
    "IsHidden" boolean
);


--
-- Name: patfieldpickitem; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.patfieldpickitem (
    "PatFieldPickItemNum" bigint NOT NULL,
    "PatFieldDefNum" bigint,
    "Name" character varying(255),
    "Abbreviation" character varying(255),
    "IsHidden" boolean,
    "ItemOrder" integer
);


--
-- Name: patient; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.patient (
    "PatNum" bigint NOT NULL,
    "LName" character varying(100),
    "FName" character varying(100),
    "MiddleI" character varying(100),
    "Preferred" character varying(100),
    "PatStatus" smallint,
    "Gender" smallint,
    "Position" smallint,
    "Birthdate" date,
    "SSN" character varying(100),
    "Address" character varying(100),
    "Address2" character varying(100),
    "City" character varying(100),
    "State" character varying(100),
    "Zip" character varying(100),
    "HmPhone" character varying(30),
    "WkPhone" character varying(30),
    "WirelessPhone" character varying(30),
    "Guarantor" bigint,
    "CreditType" character(1),
    "Email" character varying(100),
    "Salutation" character varying(100),
    "EstBalance" double precision,
    "PriProv" bigint,
    "SecProv" bigint,
    "FeeSched" bigint,
    "BillingType" bigint,
    "ImageFolder" character varying(100),
    "AddrNote" text,
    "FamFinUrgNote" text,
    "MedUrgNote" character varying(255),
    "ApptModNote" character varying(255),
    "StudentStatus" character(1),
    "SchoolName" character varying(255),
    "ChartNumber" character varying(100),
    "MedicaidID" character varying(20),
    "Bal_0_30" double precision,
    "Bal_31_60" double precision,
    "Bal_61_90" double precision,
    "BalOver90" double precision,
    "InsEst" double precision,
    "BalTotal" double precision,
    "EmployerNum" bigint,
    "EmploymentNote" character varying(255),
    "County" character varying(255),
    "GradeLevel" boolean,
    "Urgency" boolean,
    "DateFirstVisit" date,
    "ClinicNum" bigint,
    "HasIns" character varying(255),
    "TrophyFolder" character varying(255),
    "PlannedIsDone" boolean,
    "Premed" boolean,
    "Ward" character varying(255),
    "PreferConfirmMethod" smallint,
    "PreferContactMethod" smallint,
    "PreferRecallMethod" smallint,
    "SchedBeforeTime" time without time zone,
    "SchedAfterTime" time without time zone,
    "SchedDayOfWeek" boolean,
    "Language" character varying(100),
    "AdmitDate" date,
    "Title" character varying(15),
    "PayPlanDue" double precision,
    "SiteNum" bigint,
    "DateTStamp" timestamp without time zone,
    "ResponsParty" bigint,
    "CanadianEligibilityCode" boolean,
    "AskToArriveEarly" integer,
    "PreferContactConfidential" boolean,
    "SuperFamily" bigint,
    "TxtMsgOk" smallint,
    "SmokingSnoMed" character varying(32),
    "Country" character varying(255),
    "DateTimeDeceased" timestamp without time zone,
    "BillingCycleDay" integer,
    "SecUserNumEntry" bigint,
    "SecDateEntry" date,
    "HasSuperBilling" boolean,
    "PatNumCloneFrom" bigint,
    "DiscountPlanNum" bigint,
    "HasSignedTil" boolean,
    "ShortCodeOptIn" smallint,
    "SecurityHash" character varying(255)
);


--
-- Name: patient_PatNum_seq; Type: SEQUENCE; Schema: raw; Owner: -
--

CREATE SEQUENCE raw."patient_PatNum_seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: patientlink; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.patientlink (
    "PatientLinkNum" bigint NOT NULL,
    "PatNumFrom" bigint,
    "PatNumTo" bigint,
    "LinkType" boolean,
    "DateTimeLink" timestamp without time zone
);


--
-- Name: patientnote; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.patientnote (
    "PatNum" bigint NOT NULL,
    "FamFinancial" text,
    "ApptPhone" text,
    "Medical" text,
    "Service" text,
    "MedicalComp" text,
    "Treatment" text,
    "ICEName" character varying(255),
    "ICEPhone" character varying(30),
    "OrthoMonthsTreatOverride" integer,
    "DateOrthoPlacementOverride" date,
    "SecDateTEntry" timestamp without time zone,
    "SecDateTEdit" timestamp without time zone,
    "Consent" boolean,
    "UserNumOrthoLocked" bigint,
    "Pronoun" smallint
);


--
-- Name: patientportalinvite; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.patientportalinvite (
    "PatientPortalInviteNum" bigint NOT NULL,
    "PatNum" bigint,
    "ApptNum" bigint,
    "ClinicNum" bigint,
    "DateTimeEntry" timestamp without time zone,
    "TSPrior" bigint,
    "SendStatus" boolean,
    "MessageFk" bigint,
    "ResponseDescript" text,
    "MessageType" boolean,
    "DateTimeSent" timestamp without time zone,
    "ApptReminderRuleNum" bigint,
    "ApptDateTime" timestamp without time zone
);


--
-- Name: patientrace; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.patientrace (
    "PatientRaceNum" bigint NOT NULL,
    "PatNum" bigint,
    "Race" boolean,
    "CdcrecCode" character varying(255)
);


--
-- Name: patplan; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.patplan (
    "PatPlanNum" bigint NOT NULL,
    "PatNum" bigint,
    "Ordinal" smallint,
    "IsPending" boolean,
    "Relationship" smallint,
    "PatID" character varying(100),
    "InsSubNum" bigint,
    "OrthoAutoFeeBilledOverride" double precision,
    "OrthoAutoNextClaimDate" date,
    "SecDateTEntry" timestamp without time zone,
    "SecDateTEdit" timestamp without time zone
);


--
-- Name: patrestriction; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.patrestriction (
    "PatRestrictionNum" bigint NOT NULL,
    "PatNum" bigint,
    "PatRestrictType" boolean
);


--
-- Name: payconnectresponseweb; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.payconnectresponseweb (
    "PayConnectResponseWebNum" bigint NOT NULL,
    "PatNum" bigint,
    "PayNum" bigint,
    "AccountToken" character varying(255),
    "PaymentToken" character varying(255),
    "ProcessingStatus" character varying(255),
    "DateTimeEntry" timestamp without time zone,
    "DateTimePending" timestamp without time zone,
    "DateTimeCompleted" timestamp without time zone,
    "DateTimeExpired" timestamp without time zone,
    "DateTimeLastError" timestamp without time zone,
    "LastResponseStr" text,
    "CCSource" boolean,
    "Amount" double precision,
    "PayNote" character varying(255),
    "IsTokenSaved" boolean,
    "PayToken" character varying(255),
    "ExpDateToken" character varying(255),
    "RefNumber" character varying(255),
    "TransType" character varying(255),
    "EmailResponse" character varying(255),
    "LogGuid" character varying(36)
);


--
-- Name: payment; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.payment (
    "PayNum" bigint NOT NULL,
    "PayType" bigint,
    "PayDate" date,
    "PayAmt" double precision,
    "CheckNum" character varying(25),
    "BankBranch" character varying(25),
    "PayNote" text,
    "IsSplit" boolean,
    "PatNum" bigint,
    "ClinicNum" bigint,
    "DateEntry" date,
    "DepositNum" bigint,
    "Receipt" text,
    "IsRecurringCC" boolean,
    "SecUserNumEntry" bigint,
    "SecDateTEdit" timestamp without time zone,
    "PaymentSource" boolean,
    "ProcessStatus" boolean,
    "RecurringChargeDate" date,
    "ExternalId" character varying(255),
    "PaymentStatus" boolean,
    "IsCcCompleted" boolean,
    "MerchantFee" double precision
);


--
-- Name: payperiod; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.payperiod (
    "PayPeriodNum" bigint NOT NULL,
    "DateStart" date,
    "DateStop" date,
    "DatePaycheck" date
);


--
-- Name: payplan; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.payplan (
    "PayPlanNum" bigint NOT NULL,
    "PatNum" bigint,
    "Guarantor" bigint,
    "PayPlanDate" date,
    "APR" double precision,
    "Note" text,
    "PlanNum" bigint,
    "CompletedAmt" double precision,
    "InsSubNum" bigint,
    "PaySchedule" boolean,
    "NumberOfPayments" integer,
    "PayAmt" double precision,
    "DownPayment" double precision,
    "IsClosed" boolean,
    "Signature" text,
    "SigIsTopaz" boolean,
    "PlanCategory" bigint,
    "IsDynamic" boolean,
    "ChargeFrequency" boolean,
    "DatePayPlanStart" date,
    "IsLocked" boolean,
    "DateInterestStart" date,
    "DynamicPayPlanTPOption" boolean,
    "MobileAppDeviceNum" bigint,
    "SecurityHash" character varying(255)
);


--
-- Name: payplancharge; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.payplancharge (
    "PayPlanChargeNum" bigint NOT NULL,
    "PayPlanNum" bigint,
    "Guarantor" bigint,
    "PatNum" bigint,
    "ChargeDate" date,
    "Principal" double precision,
    "Interest" double precision,
    "Note" text,
    "ProvNum" bigint,
    "ClinicNum" bigint,
    "ChargeType" boolean,
    "ProcNum" bigint,
    "SecDateTEntry" timestamp without time zone,
    "SecDateTEdit" timestamp without time zone,
    "StatementNum" bigint,
    "FKey" bigint,
    "LinkType" boolean,
    "IsOffset" boolean
);


--
-- Name: payplanlink; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.payplanlink (
    "PayPlanLinkNum" bigint NOT NULL,
    "PayPlanNum" bigint,
    "LinkType" boolean,
    "FKey" bigint,
    "AmountOverride" double precision,
    "SecDateTEntry" timestamp without time zone
);


--
-- Name: payplantemplate; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.payplantemplate (
    "PayPlanTemplateNum" bigint NOT NULL,
    "PayPlanTemplateName" character varying(255),
    "ClinicNum" bigint,
    "APR" double precision,
    "InterestDelay" integer,
    "PayAmt" double precision,
    "NumberOfPayments" integer,
    "ChargeFrequency" boolean,
    "DownPayment" double precision,
    "DynamicPayPlanTPOption" boolean,
    "Note" character varying(255),
    "IsHidden" boolean
);


--
-- Name: paysplit; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.paysplit (
    "SplitNum" bigint NOT NULL,
    "SplitAmt" double precision,
    "PatNum" bigint,
    "ProcDate" date,
    "PayNum" bigint,
    "IsDiscount" boolean,
    "DiscountType" boolean,
    "ProvNum" bigint,
    "PayPlanNum" bigint,
    "DatePay" date,
    "ProcNum" bigint,
    "DateEntry" date,
    "UnearnedType" bigint,
    "ClinicNum" bigint,
    "SecUserNumEntry" bigint,
    "SecDateTEdit" timestamp without time zone,
    "FSplitNum" bigint,
    "AdjNum" bigint,
    "PayPlanChargeNum" bigint,
    "PayPlanDebitType" boolean,
    "SecurityHash" character varying(255)
);


--
-- Name: payterminal; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.payterminal (
    "PayTerminalNum" bigint NOT NULL,
    "Name" character varying(255),
    "ClinicNum" bigint,
    "TerminalID" character varying(255)
);


--
-- Name: pearlrequest; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.pearlrequest (
    "PearlRequestNum" bigint NOT NULL,
    "RequestId" character varying(255),
    "DocNum" bigint,
    "RequestStatus" boolean,
    "DateTSent" date,
    "DateTChecked" date
);


--
-- Name: perioexam; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.perioexam (
    "PerioExamNum" bigint NOT NULL,
    "PatNum" bigint,
    "ExamDate" date,
    "ProvNum" bigint,
    "DateTMeasureEdit" timestamp without time zone,
    "Note" text
);


--
-- Name: periomeasure; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.periomeasure (
    "PerioMeasureNum" bigint NOT NULL,
    "PerioExamNum" bigint,
    "SequenceType" smallint,
    "IntTooth" smallint,
    "ToothValue" smallint,
    "MBvalue" smallint,
    "Bvalue" smallint,
    "DBvalue" smallint,
    "MLvalue" smallint,
    "Lvalue" smallint,
    "DLvalue" smallint,
    "SecDateTEntry" timestamp without time zone,
    "SecDateTEdit" timestamp without time zone
);


--
-- Name: pharmacy; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.pharmacy (
    "PharmacyNum" bigint NOT NULL,
    "PharmID" character varying(255),
    "StoreName" character varying(255),
    "Phone" character varying(255),
    "Fax" character varying(255),
    "Address" character varying(255),
    "Address2" character varying(255),
    "City" character varying(255),
    "State" character varying(255),
    "Zip" character varying(255),
    "Note" text,
    "DateTStamp" timestamp without time zone
);


--
-- Name: pharmclinic; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.pharmclinic (
    "PharmClinicNum" bigint NOT NULL,
    "PharmacyNum" bigint,
    "ClinicNum" bigint
);


--
-- Name: phonenumber; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.phonenumber (
    "PhoneNumberNum" bigint NOT NULL,
    "PatNum" bigint,
    "PhoneNumberVal" character varying(255),
    "PhoneNumberDigits" character varying(30),
    "PhoneType" boolean
);


--
-- Name: popup; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.popup (
    "PopupNum" bigint NOT NULL,
    "PatNum" bigint,
    "Description" text,
    "IsDisabled" boolean,
    "PopupLevel" boolean,
    "UserNum" bigint,
    "DateTimeEntry" timestamp without time zone,
    "IsArchived" boolean,
    "PopupNumArchive" bigint,
    "DateTimeDisabled" timestamp without time zone
);


--
-- Name: preference; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.preference (
    "PrefName" character varying(255),
    "ValueString" text,
    "PrefNum" bigint NOT NULL,
    "Comments" text
);


--
-- Name: printer; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.printer (
    "PrinterNum" bigint NOT NULL,
    "ComputerNum" bigint,
    "PrintSit" smallint,
    "PrinterName" character varying(255),
    "DisplayPrompt" boolean,
    "FileExtension" character varying(255),
    "IsVirtualPrinter" boolean
);


--
-- Name: procapptcolor; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.procapptcolor (
    "ProcApptColorNum" bigint NOT NULL,
    "CodeRange" character varying(255),
    "ColorText" integer,
    "ShowPreviousDate" boolean
);


--
-- Name: procbutton; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.procbutton (
    "ProcButtonNum" bigint NOT NULL,
    "Description" character varying(255),
    "ItemOrder" smallint,
    "Category" bigint,
    "ButtonImage" text,
    "IsMultiVisit" boolean
);


--
-- Name: procbuttonitem; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.procbuttonitem (
    "ProcButtonItemNum" bigint NOT NULL,
    "ProcButtonNum" bigint,
    "OldCode" character varying(15),
    "AutoCodeNum" bigint,
    "CodeNum" bigint,
    "ItemOrder" bigint
);


--
-- Name: procbuttonquick; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.procbuttonquick (
    "ProcButtonQuickNum" bigint NOT NULL,
    "Description" character varying(255),
    "CodeValue" character varying(255),
    "Surf" character varying(255),
    "YPos" integer,
    "ItemOrder" integer,
    "IsLabel" boolean
);


--
-- Name: proccodenote; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.proccodenote (
    "ProcCodeNoteNum" bigint NOT NULL,
    "CodeNum" bigint,
    "ProvNum" bigint,
    "Note" text,
    "ProcTime" character varying(255),
    "ProcStatus" boolean
);


--
-- Name: procedurecode; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.procedurecode (
    "CodeNum" bigint NOT NULL,
    "ProcCode" character varying(15),
    "Descript" character varying(255),
    "AbbrDesc" character varying(50),
    "ProcTime" character varying(24),
    "ProcCat" bigint,
    "TreatArea" smallint,
    "NoBillIns" boolean,
    "IsProsth" boolean,
    "DefaultNote" text,
    "IsHygiene" boolean,
    "GTypeNum" smallint,
    "AlternateCode1" character varying(15),
    "MedicalCode" character varying(15),
    "IsTaxed" boolean,
    "PaintType" smallint,
    "GraphicColor" integer,
    "LaymanTerm" character varying(255),
    "IsCanadianLab" boolean,
    "PreExisting" boolean,
    "BaseUnits" integer,
    "SubstitutionCode" character varying(25),
    "SubstOnlyIf" integer,
    "DateTStamp" timestamp without time zone,
    "IsMultiVisit" boolean,
    "DrugNDC" character varying(255),
    "RevenueCodeDefault" character varying(255),
    "ProvNumDefault" bigint,
    "CanadaTimeUnits" double precision,
    "IsRadiology" boolean,
    "DefaultClaimNote" text,
    "DefaultTPNote" text,
    "BypassGlobalLock" boolean,
    "TaxCode" character varying(16),
    "PaintText" character varying(255),
    "AreaAlsoToothRange" boolean,
    "DiagnosticCodes" character varying(255)
);


--
-- Name: procedurelog; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.procedurelog (
    "ProcNum" bigint NOT NULL,
    "PatNum" bigint,
    "AptNum" bigint,
    "OldCode" character varying(15),
    "ProcDate" date,
    "ProcFee" double precision,
    "Surf" character varying(10),
    "ToothNum" character varying(2),
    "ToothRange" character varying(100),
    "Priority" bigint,
    "ProcStatus" smallint,
    "ProvNum" bigint,
    "Dx" bigint,
    "PlannedAptNum" bigint,
    "PlaceService" smallint,
    "Prosthesis" character(1),
    "DateOriginalProsth" date,
    "ClaimNote" character varying(80),
    "DateEntryC" date,
    "ClinicNum" bigint,
    "MedicalCode" character varying(15),
    "DiagnosticCode" character varying(255),
    "IsPrincDiag" boolean,
    "ProcNumLab" bigint,
    "BillingTypeOne" bigint,
    "BillingTypeTwo" bigint,
    "CodeNum" bigint,
    "CodeMod1" character(2),
    "CodeMod2" character(2),
    "CodeMod3" character(2),
    "CodeMod4" character(2),
    "RevCode" character varying(45),
    "UnitQty" integer,
    "BaseUnits" integer,
    "StartTime" integer,
    "StopTime" integer,
    "DateTP" date,
    "SiteNum" bigint,
    "HideGraphics" boolean,
    "CanadianTypeCodes" character varying(20),
    "ProcTime" time without time zone,
    "ProcTimeEnd" time without time zone,
    "DateTStamp" timestamp without time zone,
    "Prognosis" bigint,
    "DrugUnit" boolean,
    "DrugQty" real,
    "UnitQtyType" boolean,
    "StatementNum" bigint,
    "IsLocked" boolean,
    "BillingNote" character varying(255),
    "RepeatChargeNum" bigint,
    "SnomedBodySite" character varying(255),
    "DiagnosticCode2" character varying(255),
    "DiagnosticCode3" character varying(255),
    "DiagnosticCode4" character varying(255),
    "ProvOrderOverride" bigint,
    "Discount" double precision,
    "IsDateProsthEst" boolean,
    "IcdVersion" smallint,
    "IsCpoe" boolean,
    "SecUserNumEntry" bigint,
    "SecDateEntry" timestamp without time zone,
    "DateComplete" date,
    "OrderingReferralNum" bigint,
    "TaxAmt" double precision,
    "Urgency" boolean,
    "DiscountPlanAmt" double precision
);


--
-- Name: procgroupitem; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.procgroupitem (
    "ProcGroupItemNum" bigint NOT NULL,
    "ProcNum" bigint,
    "GroupNum" bigint
);


--
-- Name: procmultivisit; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.procmultivisit (
    "ProcMultiVisitNum" bigint NOT NULL,
    "GroupProcMultiVisitNum" bigint,
    "ProcNum" bigint,
    "ProcStatus" smallint,
    "IsInProcess" boolean,
    "SecDateTEntry" timestamp without time zone,
    "SecDateTEdit" timestamp without time zone,
    "PatNum" bigint
);


--
-- Name: procnote; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.procnote (
    "ProcNoteNum" bigint NOT NULL,
    "PatNum" bigint,
    "ProcNum" bigint,
    "EntryDateTime" timestamp without time zone,
    "UserNum" bigint,
    "Note" text,
    "SigIsTopaz" boolean,
    "Signature" text
);


--
-- Name: proctp; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.proctp (
    "ProcTPNum" bigint NOT NULL,
    "TreatPlanNum" bigint,
    "PatNum" bigint,
    "ProcNumOrig" bigint,
    "ItemOrder" smallint,
    "Priority" bigint,
    "ToothNumTP" character varying(255),
    "Surf" character varying(255),
    "ProcCode" character varying(15),
    "Descript" character varying(255),
    "FeeAmt" double precision,
    "PriInsAmt" double precision,
    "SecInsAmt" double precision,
    "PatAmt" double precision,
    "Discount" double precision,
    "Prognosis" character varying(255),
    "Dx" character varying(255),
    "ProcAbbr" character varying(50),
    "SecUserNumEntry" bigint,
    "SecDateEntry" date,
    "SecDateTEdit" timestamp without time zone,
    "FeeAllowed" double precision,
    "TaxAmt" double precision,
    "ProvNum" bigint,
    "DateTP" date,
    "ClinicNum" bigint,
    "CatPercUCR" double precision
);


--
-- Name: program; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.program (
    "ProgramNum" bigint NOT NULL,
    "ProgName" character varying(100),
    "ProgDesc" character varying(100),
    "Enabled" boolean,
    "Path" text,
    "CommandLine" text,
    "Note" text,
    "PluginDllName" character varying(255),
    "ButtonImage" text,
    "FileTemplate" text,
    "FilePath" character varying(255),
    "IsDisabledByHq" boolean,
    "CustErr" character varying(255)
);


--
-- Name: programproperty; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.programproperty (
    "ProgramPropertyNum" bigint NOT NULL,
    "ProgramNum" bigint,
    "PropertyDesc" character varying(255),
    "PropertyValue" text,
    "ComputerName" character varying(255),
    "ClinicNum" bigint,
    "IsMasked" boolean,
    "IsHighSecurity" boolean
);


--
-- Name: promotion; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.promotion (
    "PromotionNum" bigint NOT NULL,
    "PromotionName" character varying(255),
    "DateTimeCreated" date,
    "ClinicNum" bigint,
    "TypePromotion" boolean
);


--
-- Name: promotionlog; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.promotionlog (
    "PromotionLogNum" bigint NOT NULL,
    "PromotionNum" bigint,
    "PatNum" bigint,
    "MessageFk" bigint,
    "EmailHostingFK" bigint,
    "DateTimeSent" timestamp without time zone,
    "PromotionStatus" boolean,
    "ClinicNum" bigint,
    "SendStatus" boolean,
    "MessageType" boolean,
    "DateTimeEntry" timestamp without time zone,
    "ResponseDescript" text,
    "ApptReminderRuleNum" bigint
);


--
-- Name: provider; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.provider (
    "ProvNum" bigint NOT NULL,
    "Abbr" character varying(255),
    "ItemOrder" smallint,
    "LName" character varying(100),
    "FName" character varying(100),
    "MI" character varying(100),
    "Suffix" character varying(100),
    "FeeSched" bigint,
    "Specialty" bigint,
    "SSN" character varying(12),
    "StateLicense" character varying(15),
    "DEANum" character varying(15),
    "IsSecondary" boolean,
    "ProvColor" integer,
    "IsHidden" boolean,
    "UsingTIN" boolean,
    "BlueCrossID" character varying(25),
    "SigOnFile" boolean,
    "MedicaidID" character varying(20),
    "OutlineColor" integer,
    "SchoolClassNum" bigint,
    "NationalProvID" character varying(255),
    "CanadianOfficeNum" character varying(100),
    "DateTStamp" timestamp without time zone,
    "AnesthProvType" smallint,
    "TaxonomyCodeOverride" character varying(255),
    "IsCDAnet" boolean,
    "EcwID" character varying(255),
    "StateRxID" character varying(255),
    "IsNotPerson" boolean,
    "StateWhereLicensed" character varying(50),
    "EmailAddressNum" bigint,
    "IsInstructor" boolean,
    "EhrMuStage" smallint,
    "ProvNumBillingOverride" bigint,
    "CustomID" character varying(255),
    "ProvStatus" smallint,
    "IsHiddenReport" boolean,
    "IsErxEnabled" boolean,
    "Birthdate" date,
    "SchedNote" character varying(255),
    "WebSchedDescript" character varying(500),
    "WebSchedImageLocation" character varying(255),
    "HourlyProdGoalAmt" double precision,
    "DateTerm" date,
    "PreferredName" character varying(100)
);


--
-- Name: providerclinic; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.providerclinic (
    "ProviderClinicNum" bigint NOT NULL,
    "ProvNum" bigint,
    "ClinicNum" bigint,
    "DEANum" character varying(15),
    "StateLicense" character varying(50),
    "StateRxID" character varying(255),
    "StateWhereLicensed" character varying(15),
    "CareCreditMerchantId" character varying(20)
);


--
-- Name: providercliniclink; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.providercliniclink (
    "ProviderClinicLinkNum" bigint NOT NULL,
    "ProvNum" bigint,
    "ClinicNum" bigint
);


--
-- Name: providererx; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.providererx (
    "ProviderErxNum" bigint NOT NULL,
    "PatNum" bigint,
    "NationalProviderID" character varying(255),
    "IsEnabled" boolean,
    "IsIdentifyProofed" boolean,
    "IsSentToHq" boolean,
    "IsEpcs" boolean,
    "ErxType" boolean,
    "UserId" character varying(255),
    "AccountId" character varying(25),
    "RegistrationKeyNum" bigint
);


--
-- Name: providerident; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.providerident (
    "ProviderIdentNum" bigint NOT NULL,
    "ProvNum" bigint,
    "PayorID" character varying(255),
    "SuppIDType" boolean,
    "IDNumber" character varying(255)
);


--
-- Name: question; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.question (
    "QuestionNum" bigint NOT NULL,
    "PatNum" bigint,
    "ItemOrder" smallint,
    "Description" text,
    "Answer" text,
    "FormPatNum" bigint
);


--
-- Name: quickpastecat; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.quickpastecat (
    "QuickPasteCatNum" bigint NOT NULL,
    "Description" character varying(255),
    "ItemOrder" smallint,
    "DefaultForTypes" text
);


--
-- Name: quickpastenote; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.quickpastenote (
    "QuickPasteNoteNum" bigint NOT NULL,
    "QuickPasteCatNum" bigint,
    "ItemOrder" smallint,
    "Note" text,
    "Abbreviation" character varying(255)
);


--
-- Name: reactivation; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.reactivation (
    "ReactivationNum" bigint NOT NULL,
    "PatNum" bigint,
    "ReactivationStatus" bigint,
    "ReactivationNote" text,
    "DoNotContact" boolean
);


--
-- Name: recall; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.recall (
    "RecallNum" bigint NOT NULL,
    "PatNum" bigint,
    "DateDueCalc" date,
    "DateDue" date,
    "DatePrevious" date,
    "RecallInterval" integer,
    "RecallStatus" bigint,
    "Note" text,
    "IsDisabled" boolean,
    "DateTStamp" timestamp without time zone,
    "RecallTypeNum" bigint,
    "DisableUntilBalance" double precision,
    "DisableUntilDate" date,
    "DateScheduled" date,
    "Priority" boolean,
    "TimePatternOverride" character varying(255)
);


--
-- Name: recalltrigger; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.recalltrigger (
    "RecallTriggerNum" bigint NOT NULL,
    "RecallTypeNum" bigint,
    "CodeNum" bigint
);


--
-- Name: recalltype; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.recalltype (
    "RecallTypeNum" bigint NOT NULL,
    "Description" character varying(255),
    "DefaultInterval" integer,
    "TimePattern" character varying(255),
    "Procedures" character varying(255),
    "AppendToSpecial" boolean
);


--
-- Name: reconcile; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.reconcile (
    "ReconcileNum" bigint NOT NULL,
    "AccountNum" bigint,
    "StartingBal" double precision,
    "EndingBal" double precision,
    "DateReconcile" date,
    "IsLocked" boolean
);


--
-- Name: recurringcharge; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.recurringcharge (
    "RecurringChargeNum" bigint NOT NULL,
    "PatNum" bigint,
    "ClinicNum" bigint,
    "DateTimeCharge" timestamp without time zone,
    "ChargeStatus" boolean,
    "FamBal" double precision,
    "PayPlanDue" double precision,
    "TotalDue" double precision,
    "RepeatAmt" double precision,
    "ChargeAmt" double precision,
    "UserNum" bigint,
    "PayNum" bigint,
    "CreditCardNum" bigint,
    "ErrorMsg" text
);


--
-- Name: refattach; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.refattach (
    "RefAttachNum" bigint NOT NULL,
    "ReferralNum" bigint,
    "PatNum" bigint,
    "ItemOrder" smallint,
    "RefDate" date,
    "RefType" boolean,
    "RefToStatus" smallint,
    "Note" text,
    "IsTransitionOfCare" boolean,
    "ProcNum" bigint,
    "DateProcComplete" date,
    "ProvNum" bigint,
    "DateTStamp" timestamp without time zone
);


--
-- Name: referral; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.referral (
    "ReferralNum" bigint NOT NULL,
    "LName" character varying(100),
    "FName" character varying(100),
    "MName" character varying(100),
    "SSN" character varying(9),
    "UsingTIN" boolean,
    "Specialty" bigint,
    "ST" character varying(2),
    "Telephone" character varying(10),
    "Address" character varying(100),
    "Address2" character varying(100),
    "City" character varying(100),
    "Zip" character varying(10),
    "Note" text,
    "Phone2" character varying(30),
    "IsHidden" boolean,
    "NotPerson" boolean,
    "Title" character varying(255),
    "EMail" character varying(255),
    "PatNum" bigint,
    "NationalProvID" character varying(255),
    "Slip" bigint,
    "IsDoctor" boolean,
    "IsTrustedDirect" boolean,
    "DateTStamp" timestamp without time zone,
    "IsPreferred" boolean,
    "BusinessName" character varying(255),
    "DisplayNote" character varying(4000)
);


--
-- Name: referralcliniclink; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.referralcliniclink (
    "ReferralClinicLinkNum" bigint NOT NULL,
    "ReferralNum" bigint,
    "ClinicNum" bigint
);


--
-- Name: registrationkey; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.registrationkey (
    "RegistrationKeyNum" bigint NOT NULL,
    "PatNum" bigint,
    "RegKey" character varying(4000),
    "Note" character varying(4000),
    "DateStarted" date,
    "DateDisabled" date,
    "DateEnded" date,
    "IsForeign" boolean,
    "UsesServerVersion" boolean,
    "IsFreeVersion" boolean,
    "IsOnlyForTesting" boolean,
    "VotesAllotted" integer,
    "IsResellerCustomer" boolean,
    "HasEarlyAccess" boolean,
    "DateTBackupScheduled" timestamp without time zone,
    "BackupPassCode" character varying(32)
);


--
-- Name: reminderrule; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.reminderrule (
    "ReminderRuleNum" bigint NOT NULL,
    "ReminderCriterion" boolean,
    "CriterionFK" bigint,
    "CriterionValue" character varying(255),
    "Message" character varying(255)
);


--
-- Name: repeatcharge; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.repeatcharge (
    "RepeatChargeNum" bigint NOT NULL,
    "PatNum" bigint,
    "ProcCode" character varying(15),
    "ChargeAmt" double precision,
    "DateStart" date,
    "DateStop" date,
    "Note" text,
    "CopyNoteToProc" boolean,
    "CreatesClaim" boolean,
    "IsEnabled" boolean,
    "UsePrepay" boolean,
    "Npi" text,
    "ErxAccountId" text,
    "ProviderName" text,
    "ChargeAmtAlt" double precision,
    "UnearnedTypes" character varying(4000),
    "Frequency" boolean
);


--
-- Name: replicationserver; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.replicationserver (
    "ReplicationServerNum" bigint NOT NULL,
    "Descript" text,
    "ServerId" integer,
    "RangeStart" bigint,
    "RangeEnd" bigint,
    "AtoZpath" character varying(255),
    "UpdateBlocked" boolean,
    "SlaveMonitor" character varying(255)
);


--
-- Name: reqneeded; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.reqneeded (
    "ReqNeededNum" bigint NOT NULL,
    "Descript" character varying(255),
    "SchoolCourseNum" bigint,
    "SchoolClassNum" bigint
);


--
-- Name: reqstudent; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.reqstudent (
    "ReqStudentNum" bigint NOT NULL,
    "ReqNeededNum" bigint,
    "Descript" character varying(255),
    "SchoolCourseNum" bigint,
    "ProvNum" bigint,
    "AptNum" bigint,
    "PatNum" bigint,
    "InstructorNum" bigint,
    "DateCompleted" date
);


--
-- Name: requiredfield; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.requiredfield (
    "RequiredFieldNum" bigint NOT NULL,
    "FieldType" boolean,
    "FieldName" character varying(50)
);


--
-- Name: requiredfieldcondition; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.requiredfieldcondition (
    "RequiredFieldConditionNum" bigint NOT NULL,
    "RequiredFieldNum" bigint,
    "ConditionType" character varying(50),
    "Operator" boolean,
    "ConditionValue" character varying(255),
    "ConditionRelationship" boolean
);


--
-- Name: reseller; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.reseller (
    "ResellerNum" bigint NOT NULL,
    "PatNum" bigint,
    "UserName" character varying(255),
    "ResellerPassword" character varying(255),
    "BillingType" bigint,
    "VotesAllotted" integer,
    "Note" character varying(4000)
);


--
-- Name: resellerservice; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.resellerservice (
    "ResellerServiceNum" bigint NOT NULL,
    "ResellerNum" bigint,
    "CodeNum" bigint,
    "Fee" double precision,
    "HostedUrl" character varying(255)
);


--
-- Name: rxalert; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.rxalert (
    "RxAlertNum" bigint NOT NULL,
    "RxDefNum" bigint,
    "DiseaseDefNum" bigint,
    "AllergyDefNum" bigint,
    "MedicationNum" bigint,
    "NotificationMsg" character varying(255),
    "IsHighSignificance" boolean
);


--
-- Name: rxdef; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.rxdef (
    "RxDefNum" bigint NOT NULL,
    "Drug" character varying(255),
    "Sig" character varying(255),
    "Disp" character varying(255),
    "Refills" character varying(30),
    "Notes" character varying(255),
    "IsControlled" boolean,
    "RxCui" bigint,
    "IsProcRequired" boolean,
    "PatientInstruction" text
);


--
-- Name: rxnorm; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.rxnorm (
    "RxNormNum" bigint NOT NULL,
    "RxCui" character varying(255),
    "MmslCode" character varying(255),
    "Description" text
);


--
-- Name: rxpat; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.rxpat (
    "RxNum" bigint NOT NULL,
    "PatNum" bigint,
    "RxDate" date,
    "Drug" character varying(255),
    "Sig" character varying(255),
    "Disp" character varying(255),
    "Refills" character varying(30),
    "ProvNum" bigint,
    "Notes" character varying(255),
    "PharmacyNum" bigint,
    "IsControlled" boolean,
    "DateTStamp" timestamp without time zone,
    "SendStatus" smallint,
    "RxCui" bigint,
    "DosageCode" character varying(255),
    "ErxGuid" character varying(40),
    "IsErxOld" boolean,
    "ErxPharmacyInfo" character varying(255),
    "IsProcRequired" boolean,
    "ProcNum" bigint,
    "DaysOfSupply" double precision,
    "PatientInstruction" text,
    "ClinicNum" bigint,
    "UserNum" bigint,
    "RxType" boolean
);


--
-- Name: schedule; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.schedule (
    "ScheduleNum" bigint NOT NULL,
    "SchedDate" date,
    "StartTime" time without time zone,
    "StopTime" time without time zone,
    "SchedType" smallint,
    "ProvNum" bigint,
    "BlockoutType" bigint,
    "Note" text,
    "Status" smallint,
    "EmployeeNum" bigint,
    "DateTStamp" timestamp without time zone,
    "ClinicNum" bigint
);


--
-- Name: scheduledprocess; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.scheduledprocess (
    "ScheduledProcessNum" bigint NOT NULL,
    "ScheduledAction" character varying(50),
    "TimeToRun" timestamp without time zone,
    "FrequencyToRun" character varying(50),
    "LastRanDateTime" timestamp without time zone
);


--
-- Name: scheduleop; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.scheduleop (
    "ScheduleOpNum" bigint NOT NULL,
    "ScheduleNum" bigint,
    "OperatoryNum" bigint
);


--
-- Name: schoolclass; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.schoolclass (
    "SchoolClassNum" bigint NOT NULL,
    "GradYear" integer,
    "Descript" character varying(255)
);


--
-- Name: schoolcourse; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.schoolcourse (
    "SchoolCourseNum" bigint NOT NULL,
    "CourseID" character varying(255),
    "Descript" character varying(255)
);


--
-- Name: screen; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.screen (
    "ScreenNum" bigint NOT NULL,
    "Gender" boolean,
    "RaceOld" boolean,
    "GradeLevel" boolean,
    "Age" boolean,
    "Urgency" boolean,
    "HasCaries" boolean,
    "NeedsSealants" boolean,
    "CariesExperience" boolean,
    "EarlyChildCaries" boolean,
    "ExistingSealants" boolean,
    "MissingAllTeeth" boolean,
    "Birthdate" date,
    "ScreenGroupNum" bigint,
    "ScreenGroupOrder" smallint,
    "Comments" character varying(255),
    "ScreenPatNum" bigint,
    "SheetNum" bigint
);


--
-- Name: screengroup; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.screengroup (
    "ScreenGroupNum" bigint NOT NULL,
    "Description" character varying(255),
    "SGDate" date,
    "ProvName" character varying(255),
    "ProvNum" bigint,
    "PlaceService" boolean,
    "County" character varying(255),
    "GradeSchool" character varying(255),
    "SheetDefNum" bigint
);


--
-- Name: screenpat; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.screenpat (
    "ScreenPatNum" bigint NOT NULL,
    "PatNum" bigint,
    "ScreenGroupNum" bigint,
    "SheetNum" bigint,
    "PatScreenPerm" boolean
);


--
-- Name: securitylog; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.securitylog (
    "SecurityLogNum" bigint NOT NULL,
    "PermType" smallint,
    "UserNum" bigint,
    "LogDateTime" timestamp without time zone,
    "LogText" text,
    "PatNum" bigint,
    "CompName" character varying(255),
    "FKey" bigint,
    "LogSource" smallint,
    "DefNum" bigint,
    "DefNumError" bigint,
    "DateTPrevious" timestamp without time zone
);


--
-- Name: securityloghash; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.securityloghash (
    "SecurityLogHashNum" bigint NOT NULL,
    "SecurityLogNum" bigint,
    "LogHash" character varying(255)
);


--
-- Name: sessiontoken; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.sessiontoken (
    "SessionTokenNum" bigint NOT NULL,
    "SessionTokenHash" character varying(255),
    "Expiration" timestamp without time zone,
    "TokenType" boolean,
    "FKey" bigint
);


--
-- Name: sheet; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.sheet (
    "SheetNum" bigint NOT NULL,
    "SheetType" integer,
    "PatNum" bigint,
    "DateTimeSheet" timestamp without time zone,
    "FontSize" real,
    "FontName" character varying(255),
    "Width" integer,
    "Height" integer,
    "IsLandscape" boolean,
    "InternalNote" text,
    "Description" character varying(255),
    "ShowInTerminal" smallint,
    "IsWebForm" boolean,
    "IsMultiPage" boolean,
    "IsDeleted" boolean,
    "SheetDefNum" bigint,
    "DocNum" bigint,
    "ClinicNum" bigint,
    "DateTSheetEdited" timestamp without time zone,
    "HasMobileLayout" boolean,
    "RevID" integer,
    "WebFormSheetID" bigint
);


--
-- Name: sheetdef; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.sheetdef (
    "SheetDefNum" bigint NOT NULL,
    "Description" character varying(255),
    "SheetType" integer,
    "FontSize" real,
    "FontName" character varying(255),
    "Width" integer,
    "Height" integer,
    "IsLandscape" boolean,
    "PageCount" integer,
    "IsMultiPage" boolean,
    "BypassGlobalLock" boolean,
    "HasMobileLayout" boolean,
    "DateTCreated" timestamp without time zone,
    "RevID" integer,
    "AutoCheckSaveImage" boolean,
    "AutoCheckSaveImageDocCategory" bigint
);


--
-- Name: sheetfield; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.sheetfield (
    "SheetFieldNum" bigint NOT NULL,
    "SheetNum" bigint,
    "FieldType" integer,
    "FieldName" character varying(255),
    "FieldValue" text,
    "FontSize" real,
    "FontName" character varying(255),
    "FontIsBold" boolean,
    "XPos" integer,
    "YPos" integer,
    "Width" integer,
    "Height" integer,
    "GrowthBehavior" integer,
    "RadioButtonValue" character varying(255),
    "RadioButtonGroup" character varying(255),
    "IsRequired" boolean,
    "TabOrder" integer,
    "ReportableName" character varying(255),
    "TextAlign" smallint,
    "ItemColor" integer,
    "DateTimeSig" timestamp without time zone,
    "IsLocked" boolean,
    "TabOrderMobile" integer,
    "UiLabelMobile" text,
    "UiLabelMobileRadioButton" text,
    "SheetFieldDefNum" bigint,
    "CanElectronicallySign" boolean,
    "IsSigProvRestricted" boolean,
    "UserSigned" bigint
);


--
-- Name: sheetfielddef; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.sheetfielddef (
    "SheetFieldDefNum" bigint NOT NULL,
    "SheetDefNum" bigint,
    "FieldType" integer,
    "FieldName" character varying(255),
    "FieldValue" text,
    "FontSize" real,
    "FontName" character varying(255),
    "FontIsBold" boolean,
    "XPos" integer,
    "YPos" integer,
    "Width" integer,
    "Height" integer,
    "GrowthBehavior" integer,
    "RadioButtonValue" character varying(255),
    "RadioButtonGroup" character varying(255),
    "IsRequired" boolean,
    "TabOrder" integer,
    "ReportableName" character varying(255),
    "TextAlign" smallint,
    "IsPaymentOption" boolean,
    "ItemColor" integer,
    "IsLocked" boolean,
    "TabOrderMobile" integer,
    "UiLabelMobile" text,
    "UiLabelMobileRadioButton" text,
    "LayoutMode" boolean,
    "Language" character varying(255),
    "CanElectronicallySign" boolean,
    "IsSigProvRestricted" boolean
);


--
-- Name: sigbutdef; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.sigbutdef (
    "SigButDefNum" bigint NOT NULL,
    "ButtonText" character varying(255),
    "ButtonIndex" smallint,
    "SynchIcon" smallint,
    "ComputerName" character varying(255),
    "SigElementDefNumUser" bigint,
    "SigElementDefNumExtra" bigint,
    "SigElementDefNumMsg" bigint
);


--
-- Name: sigelementdef; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.sigelementdef (
    "SigElementDefNum" bigint NOT NULL,
    "LightRow" smallint,
    "LightColor" integer,
    "SigElementType" smallint,
    "SigText" character varying(255),
    "Sound" text,
    "ItemOrder" smallint
);


--
-- Name: sigmessage; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.sigmessage (
    "SigMessageNum" bigint NOT NULL,
    "ButtonText" character varying(255),
    "ButtonIndex" integer,
    "SynchIcon" boolean,
    "FromUser" character varying(255),
    "ToUser" character varying(255),
    "MessageDateTime" timestamp without time zone,
    "AckDateTime" timestamp without time zone,
    "SigText" character varying(255),
    "SigElementDefNumUser" bigint,
    "SigElementDefNumExtra" bigint,
    "SigElementDefNumMsg" bigint
);


--
-- Name: signalod; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.signalod (
    "SignalNum" bigint NOT NULL,
    "DateViewing" date,
    "SigDateTime" timestamp without time zone,
    "FKey" bigint,
    "FKeyType" character varying(255),
    "IType" smallint,
    "RemoteRole" boolean,
    "MsgValue" text
);


--
-- Name: site; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.site (
    "SiteNum" bigint NOT NULL,
    "Description" character varying(255),
    "Note" text,
    "Address" character varying(100),
    "Address2" character varying(100),
    "City" character varying(100),
    "State" character varying(100),
    "Zip" character varying(100),
    "ProvNum" bigint,
    "PlaceService" boolean
);


--
-- Name: smsblockphone; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.smsblockphone (
    "SmsBlockPhoneNum" bigint NOT NULL,
    "BlockWirelessNumber" character varying(255)
);


--
-- Name: smsfrommobile; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.smsfrommobile (
    "SmsFromMobileNum" bigint NOT NULL,
    "PatNum" bigint,
    "ClinicNum" bigint,
    "CommlogNum" bigint,
    "MsgText" text,
    "DateTimeReceived" timestamp without time zone,
    "SmsPhoneNumber" character varying(255),
    "MobilePhoneNumber" character varying(255),
    "MsgPart" integer,
    "MsgTotal" integer,
    "MsgRefID" character varying(255),
    "SmsStatus" boolean,
    "Flags" character varying(255),
    "IsHidden" boolean,
    "MatchCount" integer,
    "GuidMessage" character varying(255),
    "SecDateTEdit" timestamp without time zone
);


--
-- Name: smsphone; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.smsphone (
    "SmsPhoneNum" bigint NOT NULL,
    "ClinicNum" bigint,
    "PhoneNumber" character varying(255),
    "DateTimeActive" timestamp without time zone,
    "DateTimeInactive" timestamp without time zone,
    "InactiveCode" character varying(255),
    "CountryCode" character varying(255)
);


--
-- Name: smstomobile; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.smstomobile (
    "SmsToMobileNum" bigint NOT NULL,
    "PatNum" bigint,
    "GuidMessage" character varying(255),
    "GuidBatch" character varying(255),
    "SmsPhoneNumber" character varying(255),
    "MobilePhoneNumber" character varying(255),
    "IsTimeSensitive" boolean,
    "MsgType" smallint,
    "MsgText" text,
    "SmsStatus" smallint,
    "MsgParts" integer,
    "MsgChargeUSD" real,
    "ClinicNum" bigint,
    "CustErrorText" character varying(255),
    "DateTimeSent" timestamp without time zone,
    "DateTimeTerminated" timestamp without time zone,
    "IsHidden" boolean,
    "MsgDiscountUSD" real,
    "SecDateTEdit" timestamp without time zone
);


--
-- Name: snomed; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.snomed (
    "SnomedNum" bigint NOT NULL,
    "SnomedCode" character varying(255),
    "Description" character varying(255)
);


--
-- Name: sop; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.sop (
    "SopNum" bigint NOT NULL,
    "SopCode" character varying(255),
    "Description" character varying(255)
);


--
-- Name: stateabbr; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.stateabbr (
    "StateAbbrNum" bigint NOT NULL,
    "Description" character varying(50),
    "Abbr" character varying(50),
    "MedicaidIDLength" integer
);


--
-- Name: statement; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.statement (
    "StatementNum" bigint NOT NULL,
    "PatNum" bigint,
    "DateSent" date,
    "DateRangeFrom" date,
    "DateRangeTo" date,
    "Note" text,
    "NoteBold" text,
    "Mode_" smallint,
    "HidePayment" boolean,
    "SinglePatient" boolean,
    "Intermingled" boolean,
    "IsSent" boolean,
    "DocNum" bigint,
    "DateTStamp" timestamp without time zone,
    "IsReceipt" boolean,
    "IsInvoice" boolean,
    "IsInvoiceCopy" boolean,
    "EmailSubject" character varying(255),
    "EmailBody" text,
    "SuperFamily" bigint,
    "IsBalValid" boolean,
    "InsEst" double precision,
    "BalTotal" double precision,
    "StatementType" character varying(50),
    "ShortGUID" character varying(30),
    "StatementShortURL" character varying(50),
    "StatementURL" character varying(255),
    "SmsSendStatus" smallint,
    "LimitedCustomFamily" smallint,
    "ShowTransSinceBalZero" boolean
);


--
-- Name: statementprod; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.statementprod (
    "StatementProdNum" bigint NOT NULL,
    "StatementNum" bigint,
    "FKey" bigint,
    "ProdType" smallint,
    "LateChargeAdjNum" bigint,
    "DocNum" bigint
);


--
-- Name: stmtlink; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.stmtlink (
    "StmtLinkNum" bigint NOT NULL,
    "StatementNum" bigint,
    "StmtLinkType" smallint,
    "FKey" bigint
);


--
-- Name: substitutionlink; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.substitutionlink (
    "SubstitutionLinkNum" bigint NOT NULL,
    "PlanNum" bigint,
    "CodeNum" bigint,
    "SubstitutionCode" character varying(25),
    "SubstOnlyIf" integer
);


--
-- Name: supplier; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.supplier (
    "SupplierNum" bigint NOT NULL,
    "Name" character varying(255),
    "Phone" character varying(255),
    "CustomerId" character varying(255),
    "Website" text,
    "UserName" character varying(255),
    "Password" character varying(255),
    "Note" text
);


--
-- Name: supply; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.supply (
    "SupplyNum" bigint NOT NULL,
    "SupplierNum" bigint,
    "CatalogNumber" character varying(255),
    "Descript" character varying(255),
    "Category" bigint,
    "ItemOrder" integer,
    "LevelDesired" real,
    "IsHidden" boolean,
    "Price" double precision,
    "BarCodeOrID" character varying(255),
    "DispDefaultQuant" real,
    "DispUnitsCount" integer,
    "DispUnitDesc" character varying(255),
    "LevelOnHand" real,
    "OrderQty" integer
);


--
-- Name: supplyneeded; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.supplyneeded (
    "SupplyNeededNum" bigint NOT NULL,
    "Description" text,
    "DateAdded" date
);


--
-- Name: supplyorder; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.supplyorder (
    "SupplyOrderNum" bigint NOT NULL,
    "SupplierNum" bigint,
    "DatePlaced" date,
    "Note" text,
    "AmountTotal" double precision,
    "UserNum" bigint,
    "ShippingCharge" double precision,
    "DateReceived" date
);


--
-- Name: supplyorderitem; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.supplyorderitem (
    "SupplyOrderItemNum" bigint NOT NULL,
    "SupplyOrderNum" bigint,
    "SupplyNum" bigint,
    "Qty" integer,
    "Price" double precision,
    "DateReceived" date
);


--
-- Name: task; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.task (
    "TaskNum" bigint NOT NULL,
    "TaskListNum" bigint,
    "DateTask" date,
    "KeyNum" bigint,
    "Descript" text,
    "TaskStatus" smallint,
    "IsRepeating" boolean,
    "DateType" boolean,
    "FromNum" bigint,
    "ObjectType" smallint,
    "DateTimeEntry" timestamp without time zone,
    "UserNum" bigint,
    "DateTimeFinished" timestamp without time zone,
    "PriorityDefNum" bigint,
    "ReminderGroupId" character varying(20),
    "ReminderType" smallint,
    "ReminderFrequency" integer,
    "DateTimeOriginal" timestamp without time zone,
    "SecDateTEdit" timestamp without time zone,
    "DescriptOverride" character varying(255),
    "IsReadOnly" boolean,
    "TriageCategory" bigint
);


--
-- Name: taskancestor; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.taskancestor (
    "TaskAncestorNum" bigint NOT NULL,
    "TaskNum" bigint,
    "TaskListNum" bigint
);


--
-- Name: taskattachment; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.taskattachment (
    "TaskAttachmentNum" bigint NOT NULL,
    "TaskNum" bigint,
    "DocNum" bigint,
    "TextValue" text,
    "Description" character varying(255)
);


--
-- Name: taskhist; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.taskhist (
    "TaskHistNum" bigint NOT NULL,
    "UserNumHist" bigint,
    "DateTStamp" timestamp without time zone,
    "IsNoteChange" boolean,
    "TaskNum" bigint,
    "TaskListNum" bigint,
    "DateTask" date,
    "KeyNum" bigint,
    "Descript" text,
    "TaskStatus" smallint,
    "IsRepeating" boolean,
    "DateType" boolean,
    "FromNum" bigint,
    "ObjectType" smallint,
    "DateTimeEntry" timestamp without time zone,
    "UserNum" bigint,
    "DateTimeFinished" timestamp without time zone,
    "PriorityDefNum" bigint,
    "ReminderGroupId" character varying(20),
    "ReminderType" smallint,
    "ReminderFrequency" integer,
    "DateTimeOriginal" timestamp without time zone,
    "SecDateTEdit" timestamp without time zone,
    "DescriptOverride" character varying(255),
    "IsReadOnly" boolean,
    "TriageCategory" bigint
);


--
-- Name: tasklist; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.tasklist (
    "TaskListNum" bigint NOT NULL,
    "Descript" character varying(255),
    "Parent" bigint,
    "DateTL" date,
    "IsRepeating" boolean,
    "DateType" boolean,
    "FromNum" bigint,
    "ObjectType" smallint,
    "DateTimeEntry" timestamp without time zone,
    "GlobalTaskFilterType" boolean,
    "TaskListStatus" smallint
);


--
-- Name: tasknote; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.tasknote (
    "TaskNoteNum" bigint NOT NULL,
    "TaskNum" bigint,
    "UserNum" bigint,
    "DateTimeNote" timestamp without time zone,
    "Note" text
);


--
-- Name: tasksubscription; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.tasksubscription (
    "TaskSubscriptionNum" bigint NOT NULL,
    "UserNum" bigint,
    "TaskListNum" bigint,
    "TaskNum" bigint
);


--
-- Name: taskunread; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.taskunread (
    "TaskUnreadNum" bigint NOT NULL,
    "TaskNum" bigint,
    "UserNum" bigint
);


--
-- Name: tempimageconv; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.tempimageconv (
    filepath text,
    filename text,
    patnum bigint,
    patnum_str character varying(30),
    foldername character varying(50)
);


--
-- Name: tempimageconv2; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.tempimageconv2 (
    filepath text,
    filename character varying(100),
    patnum bigint,
    patnum_str character varying(30),
    imagedate character varying(50),
    description character varying(200),
    document_id character varying(50),
    "IsDeleted" integer,
    doccategory integer
);


--
-- Name: tempinsplan_bak_20200424; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.tempinsplan_bak_20200424 (
    "PlanNum" bigint,
    "GroupName" character varying(50),
    "GroupNum" character varying(25),
    "PlanNote" text,
    "FeeSched" bigint,
    "PlanType" character(1),
    "ClaimFormNum" bigint,
    "UseAltCode" boolean,
    "ClaimsUseUCR" boolean,
    "CopayFeeSched" bigint,
    "EmployerNum" bigint,
    "CarrierNum" bigint,
    "AllowedFeeSched" bigint,
    "TrojanID" character varying(100),
    "DivisionNo" character varying(255),
    "IsMedical" boolean,
    "FilingCode" bigint,
    "DentaideCardSequence" boolean,
    "ShowBaseUnits" boolean,
    "CodeSubstNone" boolean,
    "IsHidden" boolean,
    "MonthRenew" smallint,
    "FilingCodeSubtype" bigint,
    "CanadianPlanFlag" character varying(5),
    "CanadianDiagnosticCode" character varying(255),
    "CanadianInstitutionCode" character varying(255),
    "RxBIN" character varying(255),
    "CobRule" boolean,
    "SopCode" character varying(255),
    "SecUserNumEntry" bigint,
    "SecDateEntry" date,
    "SecDateTEdit" timestamp without time zone,
    "HideFromVerifyList" boolean,
    "OrthoType" boolean,
    "OrthoAutoProcFreq" boolean,
    "OrthoAutoProcCodeNumOverride" bigint,
    "OrthoAutoFeeBilled" double precision,
    "OrthoAutoClaimDaysWait" integer,
    "BillingType" bigint,
    "HasPpoSubstWriteoffs" boolean,
    "ExclusionFeeRule" boolean
);


--
-- Name: temppopup_bak_20200424; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.temppopup_bak_20200424 (
    "PopupNum" bigint,
    "PatNum" bigint,
    "Description" text,
    "IsDisabled" boolean,
    "PopupLevel" boolean,
    "UserNum" bigint,
    "DateTimeEntry" timestamp without time zone,
    "IsArchived" boolean,
    "PopupNumArchive" bigint
);


--
-- Name: tempprocedurecode_bak_20240617; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.tempprocedurecode_bak_20240617 (
    "CodeNum" bigint,
    "ProcCode" character varying(15),
    "Descript" character varying(255),
    "AbbrDesc" character varying(50),
    "ProcTime" character varying(24),
    "ProcCat" bigint,
    "TreatArea" smallint,
    "NoBillIns" boolean,
    "IsProsth" boolean,
    "DefaultNote" text,
    "IsHygiene" boolean,
    "GTypeNum" smallint,
    "AlternateCode1" character varying(15),
    "MedicalCode" character varying(15),
    "IsTaxed" boolean,
    "PaintType" smallint,
    "GraphicColor" integer,
    "LaymanTerm" character varying(255),
    "IsCanadianLab" boolean,
    "PreExisting" boolean,
    "BaseUnits" integer,
    "SubstitutionCode" character varying(25),
    "SubstOnlyIf" integer,
    "DateTStamp" timestamp without time zone,
    "IsMultiVisit" boolean,
    "DrugNDC" character varying(255),
    "RevenueCodeDefault" character varying(255),
    "ProvNumDefault" bigint,
    "CanadaTimeUnits" double precision,
    "IsRadiology" boolean,
    "DefaultClaimNote" text,
    "DefaultTPNote" text,
    "BypassGlobalLock" boolean,
    "TaxCode" character varying(16),
    "PaintText" character varying(255),
    "AreaAlsoToothRange" boolean,
    "DiagnosticCodes" character varying(255)
);


--
-- Name: tempschedule_bak_20200424; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.tempschedule_bak_20200424 (
    "ScheduleNum" bigint,
    "SchedDate" date,
    "StartTime" time without time zone,
    "StopTime" time without time zone,
    "SchedType" smallint,
    "ProvNum" bigint,
    "BlockoutType" bigint,
    "Note" text,
    "Status" boolean,
    "EmployeeNum" bigint,
    "DateTStamp" timestamp without time zone,
    "ClinicNum" bigint
);


--
-- Name: tempscheduleop_bak_20200424; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.tempscheduleop_bak_20200424 (
    "ScheduleOpNum" bigint,
    "ScheduleNum" bigint,
    "OperatoryNum" bigint
);


--
-- Name: terminalactive; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.terminalactive (
    "TerminalActiveNum" bigint NOT NULL,
    "ComputerName" character varying(255),
    "TerminalStatus" boolean,
    "PatNum" bigint,
    "SessionId" integer,
    "ProcessId" integer,
    "SessionName" character varying(255)
);


--
-- Name: timeadjust; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.timeadjust (
    "TimeAdjustNum" bigint NOT NULL,
    "EmployeeNum" bigint,
    "TimeEntry" timestamp without time zone,
    "RegHours" time without time zone,
    "OTimeHours" time without time zone,
    "Note" text,
    "IsAuto" boolean,
    "ClinicNum" bigint,
    "PtoDefNum" bigint,
    "PtoHours" time without time zone,
    "IsUnpaidProtectedLeave" boolean,
    "SecuUserNumEntry" bigint
);


--
-- Name: timecardrule; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.timecardrule (
    "TimeCardRuleNum" bigint NOT NULL,
    "EmployeeNum" bigint,
    "OverHoursPerDay" time without time zone,
    "AfterTimeOfDay" time without time zone,
    "BeforeTimeOfDay" time without time zone,
    "IsOvertimeExempt" boolean,
    "MinClockInTime" time without time zone,
    "HasWeekendRate3" boolean
);


--
-- Name: tmpdocument; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.tmpdocument (
    document_id character varying(50),
    document_name text,
    document_type text,
    document_creation_date text,
    document_modified_date text,
    open_with text,
    document_group_id text,
    patient_id character varying(50),
    ref_table text,
    sec_ref_id text,
    sec_ref_table text,
    user_id text,
    original_user_id text,
    private text,
    display_in_docmgr text,
    signed text,
    archive_name text,
    archive_path text,
    needs_converted text,
    notice_of_privacy text,
    privacy_authorization text,
    consent text,
    practice_id text,
    custom_document_id text,
    headerfooter_added text,
    doccategory integer
);


--
-- Name: tmpimages; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.tmpimages (
    patient_id character varying(50),
    images_date_time text,
    image_id character varying(50),
    use_as_example text,
    tooth_number text,
    in_use text,
    "row" text,
    _column text,
    date_last_modified text,
    last_modified_by text,
    image_type text,
    acquired_from text,
    exam_id text,
    calibration text,
    signature text,
    example_group text,
    document_group text,
    acquired_by text,
    height text,
    width text,
    original_image_id text,
    image_to_delete text,
    image_description text,
    instance_uid text,
    archive_name text,
    archive_path text,
    image_status text,
    practice_id text,
    maximus_signature text,
    "DolphinTimePointImage" text,
    exposure_level text,
    sidexis_image_position text
);


--
-- Name: tmppatient; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.tmppatient (
    patient_id character varying(50),
    patient_image_id character varying(50)
);


--
-- Name: toolbutitem; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.toolbutitem (
    "ToolButItemNum" bigint NOT NULL,
    "ProgramNum" bigint,
    "ToolBar" smallint,
    "ButtonText" character varying(255)
);


--
-- Name: toothgridcell; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.toothgridcell (
    "ToothGridCellNum" bigint NOT NULL,
    "SheetFieldNum" bigint,
    "ToothGridColNum" bigint,
    "ValueEntered" character varying(255),
    "ToothNum" character varying(10)
);


--
-- Name: toothgridcol; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.toothgridcol (
    "ToothGridColNum" bigint NOT NULL,
    "SheetFieldNum" bigint,
    "NameItem" character varying(255),
    "CellType" boolean,
    "ItemOrder" smallint,
    "ColumnWidth" smallint,
    "CodeNum" bigint,
    "ProcStatus" boolean
);


--
-- Name: toothinitial; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.toothinitial (
    "ToothInitialNum" bigint NOT NULL,
    "PatNum" bigint,
    "ToothNum" character varying(2),
    "InitialType" smallint,
    "Movement" real,
    "DrawingSegment" text,
    "ColorDraw" integer,
    "SecDateTEntry" timestamp without time zone,
    "SecDateTEdit" timestamp without time zone,
    "DrawText" character varying(255)
);


--
-- Name: transaction; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.transaction (
    "TransactionNum" bigint NOT NULL,
    "DateTimeEntry" timestamp without time zone,
    "UserNum" bigint,
    "DepositNum" bigint,
    "PayNum" bigint,
    "SecUserNumEdit" bigint,
    "SecDateTEdit" timestamp without time zone,
    "TransactionInvoiceNum" bigint
);


--
-- Name: transactioninvoice; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.transactioninvoice (
    "TransactionInvoiceNum" bigint NOT NULL,
    "FileName" character varying(255),
    "InvoiceData" text,
    "FilePath" character varying(255)
);


--
-- Name: treatplan; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.treatplan (
    "TreatPlanNum" bigint NOT NULL,
    "PatNum" bigint,
    "DateTP" date,
    "Heading" character varying(255),
    "Note" text,
    "Signature" text,
    "SigIsTopaz" boolean,
    "ResponsParty" bigint,
    "DocNum" bigint,
    "TPStatus" smallint,
    "SecUserNumEntry" bigint,
    "SecDateEntry" date,
    "SecDateTEdit" timestamp without time zone,
    "UserNumPresenter" bigint,
    "TPType" boolean,
    "SignaturePractice" text,
    "DateTSigned" timestamp without time zone,
    "DateTPracticeSigned" timestamp without time zone,
    "SignatureText" character varying(255),
    "SignaturePracticeText" character varying(255),
    "MobileAppDeviceNum" bigint
);


--
-- Name: treatplanattach; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.treatplanattach (
    "TreatPlanAttachNum" bigint NOT NULL,
    "TreatPlanNum" bigint,
    "ProcNum" bigint,
    "Priority" bigint
);


--
-- Name: treatplanparam; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.treatplanparam (
    "TreatPlanParamNum" bigint NOT NULL,
    "PatNum" bigint,
    "TreatPlanNum" bigint,
    "ShowDiscount" boolean,
    "ShowMaxDed" boolean,
    "ShowSubTotals" boolean,
    "ShowTotals" boolean,
    "ShowCompleted" boolean,
    "ShowFees" boolean,
    "ShowIns" boolean
);


--
-- Name: tsitranslog; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.tsitranslog (
    "TsiTransLogNum" bigint NOT NULL,
    "PatNum" bigint,
    "UserNum" bigint,
    "TransType" boolean,
    "TransDateTime" timestamp without time zone,
    "ServiceType" boolean,
    "ServiceCode" boolean,
    "TransAmt" double precision,
    "AccountBalance" double precision,
    "FKeyType" boolean,
    "FKey" bigint,
    "RawMsgText" character varying(1000),
    "ClientId" character varying(25),
    "TransJson" text,
    "ClinicNum" bigint,
    "AggTransLogNum" bigint
);


--
-- Name: ucum; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.ucum (
    "UcumNum" bigint NOT NULL,
    "UcumCode" character varying(255),
    "Description" character varying(255),
    "IsInUse" boolean
);


--
-- Name: updatehistory; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.updatehistory (
    "UpdateHistoryNum" bigint NOT NULL,
    "DateTimeUpdated" timestamp without time zone,
    "ProgramVersion" character varying(255),
    "Signature" text
);


--
-- Name: userclinic; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.userclinic (
    "UserClinicNum" bigint NOT NULL,
    "UserNum" bigint,
    "ClinicNum" bigint
);


--
-- Name: usergroup; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.usergroup (
    "UserGroupNum" bigint NOT NULL,
    "Description" character varying(255),
    "UserGroupNumCEMT" bigint
);


--
-- Name: usergroupattach; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.usergroupattach (
    "UserGroupAttachNum" bigint NOT NULL,
    "UserNum" bigint,
    "UserGroupNum" bigint
);


--
-- Name: userod; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.userod (
    "UserNum" bigint NOT NULL,
    "UserName" character varying(255),
    "Password" character varying(255),
    "UserGroupNum" bigint,
    "EmployeeNum" bigint,
    "ClinicNum" bigint,
    "ProvNum" bigint,
    "IsHidden" boolean,
    "TaskListInBox" bigint,
    "AnesthProvType" integer,
    "DefaultHidePopups" boolean,
    "PasswordIsStrong" boolean,
    "ClinicIsRestricted" boolean,
    "InboxHidePopups" boolean,
    "UserNumCEMT" bigint,
    "DateTFail" timestamp without time zone,
    "FailedAttempts" smallint,
    "DomainUser" character varying(255),
    "IsPasswordResetRequired" boolean,
    "MobileWebPin" character varying(255),
    "MobileWebPinFailedAttempts" boolean,
    "DateTLastLogin" timestamp without time zone,
    "EClipboardClinicalPin" character varying(128),
    "BadgeId" character varying(255)
);


--
-- Name: userodapptview; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.userodapptview (
    "UserodApptViewNum" bigint NOT NULL,
    "UserNum" bigint,
    "ClinicNum" bigint,
    "ApptViewNum" bigint
);


--
-- Name: userodpref; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.userodpref (
    "UserOdPrefNum" bigint NOT NULL,
    "UserNum" bigint,
    "Fkey" bigint,
    "FkeyType" smallint,
    "ValueString" text,
    "ClinicNum" bigint
);


--
-- Name: userquery; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.userquery (
    "QueryNum" bigint NOT NULL,
    "Description" character varying(255),
    "FileName" character varying(255),
    "QueryText" text,
    "IsReleased" boolean,
    "IsPromptSetup" boolean,
    "DefaultFormatRaw" boolean
);


--
-- Name: userweb; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.userweb (
    "UserWebNum" bigint NOT NULL,
    "FKey" bigint,
    "FKeyType" boolean,
    "UserName" character varying(255),
    "Password" character varying(255),
    "PasswordResetCode" character varying(255),
    "RequireUserNameChange" boolean,
    "DateTimeLastLogin" timestamp without time zone,
    "RequirePasswordChange" boolean
);


--
-- Name: utm; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.utm (
    "UtmNum" bigint NOT NULL,
    "CampaignName" character varying(500),
    "MediumInfo" character varying(500),
    "SourceInfo" character varying(500)
);


--
-- Name: vaccineobs; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.vaccineobs (
    "VaccineObsNum" bigint NOT NULL,
    "VaccinePatNum" bigint,
    "ValType" boolean,
    "IdentifyingCode" boolean,
    "ValReported" character varying(255),
    "ValCodeSystem" boolean,
    "VaccineObsNumGroup" bigint,
    "UcumCode" character varying(255),
    "DateObs" date,
    "MethodCode" character varying(255)
);


--
-- Name: vaccinepat; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.vaccinepat (
    "VaccinePatNum" bigint NOT NULL,
    "VaccineDefNum" bigint,
    "DateTimeStart" timestamp without time zone,
    "DateTimeEnd" timestamp without time zone,
    "AdministeredAmt" real,
    "DrugUnitNum" bigint,
    "LotNumber" character varying(255),
    "PatNum" bigint,
    "Note" text,
    "FilledCity" character varying(255),
    "FilledST" character varying(255),
    "CompletionStatus" boolean,
    "AdministrationNoteCode" boolean,
    "UserNum" bigint,
    "ProvNumOrdering" bigint,
    "ProvNumAdminister" bigint,
    "DateExpire" date,
    "RefusalReason" boolean,
    "ActionCode" boolean,
    "AdministrationRoute" boolean,
    "AdministrationSite" boolean
);


--
-- Name: vitalsign; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.vitalsign (
    "VitalsignNum" bigint NOT NULL,
    "PatNum" bigint,
    "Height" real,
    "Weight" real,
    "BpSystolic" smallint,
    "BpDiastolic" smallint,
    "DateTaken" date,
    "HasFollowupPlan" boolean,
    "IsIneligible" boolean,
    "Documentation" text,
    "ChildGotNutrition" boolean,
    "ChildGotPhysCouns" boolean,
    "WeightCode" character varying(255),
    "HeightExamCode" character varying(30),
    "WeightExamCode" character varying(30),
    "BMIExamCode" character varying(30),
    "EhrNotPerformedNum" bigint,
    "PregDiseaseNum" bigint,
    "BMIPercentile" integer,
    "Pulse" integer
);


--
-- Name: webschedcarrierrule; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.webschedcarrierrule (
    "WebSchedCarrierRuleNum" bigint NOT NULL,
    "ClinicNum" bigint,
    "CarrierName" character varying(255),
    "DisplayName" character varying(255),
    "Message" text,
    "Rule" boolean
);


--
-- Name: webschedrecall; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.webschedrecall (
    "WebSchedRecallNum" bigint NOT NULL,
    "ClinicNum" bigint,
    "PatNum" bigint,
    "RecallNum" bigint,
    "DateTimeEntry" timestamp without time zone,
    "DateDue" timestamp without time zone,
    "ReminderCount" integer,
    "DateTimeSent" timestamp without time zone,
    "DateTimeSendFailed" timestamp without time zone,
    "SendStatus" boolean,
    "ShortGUID" character varying(255),
    "ResponseDescript" text,
    "Source" boolean,
    "CommlogNum" bigint,
    "MessageType" boolean,
    "MessageFk" bigint,
    "ApptReminderRuleNum" bigint
);


--
-- Name: wikilistheaderwidth; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.wikilistheaderwidth (
    "WikiListHeaderWidthNum" bigint NOT NULL,
    "ListName" character varying(255),
    "ColName" character varying(255),
    "ColWidth" integer,
    "PickList" text,
    "IsHidden" boolean
);


--
-- Name: wikilisthist; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.wikilisthist (
    "WikiListHistNum" bigint NOT NULL,
    "UserNum" bigint,
    "ListName" character varying(255),
    "ListHeaders" text,
    "ListContent" text,
    "DateTimeSaved" timestamp without time zone
);


--
-- Name: wikipage; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.wikipage (
    "WikiPageNum" bigint NOT NULL,
    "UserNum" bigint,
    "PageTitle" character varying(255),
    "KeyWords" character varying(255),
    "PageContent" text,
    "DateTimeSaved" timestamp without time zone,
    "IsDraft" boolean,
    "IsLocked" boolean,
    "IsDeleted" boolean,
    "PageContentPlainText" text
);


--
-- Name: wikipagehist; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.wikipagehist (
    "WikiPageNum" bigint NOT NULL,
    "UserNum" bigint,
    "PageTitle" character varying(255),
    "PageContent" text,
    "DateTimeSaved" timestamp without time zone,
    "IsDeleted" boolean
);


--
-- Name: xchargetransaction; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.xchargetransaction (
    "XChargeTransactionNum" bigint NOT NULL,
    "TransType" character varying(255),
    "Amount" double precision,
    "CCEntry" character varying(255),
    "PatNum" bigint,
    "Result" character varying(255),
    "ClerkID" character varying(255),
    "ResultCode" character varying(255),
    "Expiration" character varying(255),
    "CCType" character varying(255),
    "CreditCardNum" character varying(255),
    "BatchNum" character varying(255),
    "ItemNum" character varying(255),
    "ApprCode" character varying(255),
    "TransactionDateTime" timestamp without time zone,
    "BatchTotal" double precision
);


--
-- Name: xwebresponse; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.xwebresponse (
    "XWebResponseNum" bigint NOT NULL,
    "PatNum" bigint,
    "ProvNum" bigint,
    "ClinicNum" bigint,
    "PaymentNum" bigint,
    "DateTEntry" timestamp without time zone,
    "DateTUpdate" timestamp without time zone,
    "TransactionStatus" boolean,
    "ResponseCode" integer,
    "XWebResponseCode" character varying(255),
    "ResponseDescription" character varying(255),
    "OTK" character varying(255),
    "HpfUrl" text,
    "HpfExpiration" timestamp without time zone,
    "TransactionID" character varying(255),
    "TransactionType" character varying(255),
    "Alias" character varying(255),
    "CardType" character varying(255),
    "CardBrand" character varying(255),
    "CardBrandShort" character varying(255),
    "MaskedAcctNum" character varying(255),
    "Amount" double precision,
    "ApprovalCode" character varying(255),
    "CardCodeResponse" character varying(255),
    "ReceiptID" integer,
    "ExpDate" character varying(255),
    "EntryMethod" character varying(255),
    "ProcessorResponse" character varying(255),
    "BatchNum" integer,
    "BatchAmount" double precision,
    "AccountExpirationDate" date,
    "DebugError" text,
    "PayNote" text,
    "CCSource" boolean,
    "OrderId" character varying(255),
    "EmailResponse" character varying(255),
    "LogGuid" character varying(36)
);


--
-- Name: zipcode; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.zipcode (
    "ZipCodeNum" bigint NOT NULL,
    "ZipCodeDigits" character varying(20),
    "City" character varying(100),
    "State" character varying(20),
    "IsFrequent" boolean
);


--
-- Name: etl_load_status id; Type: DEFAULT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.etl_load_status ALTER COLUMN id SET DEFAULT nextval('raw.etl_load_status_id_seq'::regclass);


--
-- Name: etl_pipeline_metrics id; Type: DEFAULT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.etl_pipeline_metrics ALTER COLUMN id SET DEFAULT nextval('raw.etl_pipeline_metrics_id_seq'::regclass);


--
-- Name: etl_table_metrics id; Type: DEFAULT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.etl_table_metrics ALTER COLUMN id SET DEFAULT nextval('raw.etl_table_metrics_id_seq'::regclass);


--
-- Name: etl_transform_status id; Type: DEFAULT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.etl_transform_status ALTER COLUMN id SET DEFAULT nextval('raw.etl_transform_status_id_seq'::regclass);


--
-- Name: account account_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.account
    ADD CONSTRAINT account_pkey PRIMARY KEY ("AccountNum");


--
-- Name: accountingautopay accountingautopay_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.accountingautopay
    ADD CONSTRAINT accountingautopay_pkey PRIMARY KEY ("AccountingAutoPayNum");


--
-- Name: activeinstance activeinstance_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.activeinstance
    ADD CONSTRAINT activeinstance_pkey PRIMARY KEY ("ActiveInstanceNum");


--
-- Name: adjustment adjustment_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.adjustment
    ADD CONSTRAINT adjustment_pkey PRIMARY KEY ("AdjNum");


--
-- Name: alertcategory alertcategory_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.alertcategory
    ADD CONSTRAINT alertcategory_pkey PRIMARY KEY ("AlertCategoryNum");


--
-- Name: alertcategorylink alertcategorylink_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.alertcategorylink
    ADD CONSTRAINT alertcategorylink_pkey PRIMARY KEY ("AlertCategoryLinkNum");


--
-- Name: alertitem alertitem_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.alertitem
    ADD CONSTRAINT alertitem_pkey PRIMARY KEY ("AlertItemNum");


--
-- Name: alertread alertread_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.alertread
    ADD CONSTRAINT alertread_pkey PRIMARY KEY ("AlertReadNum");


--
-- Name: alertsub alertsub_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.alertsub
    ADD CONSTRAINT alertsub_pkey PRIMARY KEY ("AlertSubNum");


--
-- Name: allergy allergy_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.allergy
    ADD CONSTRAINT allergy_pkey PRIMARY KEY ("AllergyNum");


--
-- Name: allergydef allergydef_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.allergydef
    ADD CONSTRAINT allergydef_pkey PRIMARY KEY ("AllergyDefNum");


--
-- Name: anestheticdata anestheticdata_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.anestheticdata
    ADD CONSTRAINT anestheticdata_pkey PRIMARY KEY ("AnestheticDataNum");


--
-- Name: anestheticrecord anestheticrecord_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.anestheticrecord
    ADD CONSTRAINT anestheticrecord_pkey PRIMARY KEY ("AnestheticRecordNum");


--
-- Name: anesthmedsgiven anesthmedsgiven_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.anesthmedsgiven
    ADD CONSTRAINT anesthmedsgiven_pkey PRIMARY KEY ("AnestheticMedNum");


--
-- Name: anesthmedsintake anesthmedsintake_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.anesthmedsintake
    ADD CONSTRAINT anesthmedsintake_pkey PRIMARY KEY ("AnestheticMedNum");


--
-- Name: anesthmedsinventory anesthmedsinventory_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.anesthmedsinventory
    ADD CONSTRAINT anesthmedsinventory_pkey PRIMARY KEY ("AnestheticMedNum");


--
-- Name: anesthmedsinventoryadj anesthmedsinventoryadj_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.anesthmedsinventoryadj
    ADD CONSTRAINT anesthmedsinventoryadj_pkey PRIMARY KEY ("AdjustNum");


--
-- Name: anesthmedsuppliers anesthmedsuppliers_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.anesthmedsuppliers
    ADD CONSTRAINT anesthmedsuppliers_pkey PRIMARY KEY ("SupplierIDNum");


--
-- Name: anesthscore anesthscore_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.anesthscore
    ADD CONSTRAINT anesthscore_pkey PRIMARY KEY ("AnesthScoreNum");


--
-- Name: anesthvsdata anesthvsdata_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.anesthvsdata
    ADD CONSTRAINT anesthvsdata_pkey PRIMARY KEY ("AnesthVSDataNum");


--
-- Name: apikey apikey_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.apikey
    ADD CONSTRAINT apikey_pkey PRIMARY KEY ("APIKeyNum");


--
-- Name: apisubscription apisubscription_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.apisubscription
    ADD CONSTRAINT apisubscription_pkey PRIMARY KEY ("ApiSubscriptionNum");


--
-- Name: appointment appointment_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.appointment
    ADD CONSTRAINT appointment_pkey PRIMARY KEY ("AptNum");


--
-- Name: appointmentrule appointmentrule_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.appointmentrule
    ADD CONSTRAINT appointmentrule_pkey PRIMARY KEY ("AppointmentRuleNum");


--
-- Name: appointmenttype appointmenttype_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.appointmenttype
    ADD CONSTRAINT appointmenttype_pkey PRIMARY KEY ("AppointmentTypeNum");


--
-- Name: apptfield apptfield_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.apptfield
    ADD CONSTRAINT apptfield_pkey PRIMARY KEY ("ApptFieldNum");


--
-- Name: apptfielddef apptfielddef_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.apptfielddef
    ADD CONSTRAINT apptfielddef_pkey PRIMARY KEY ("ApptFieldDefNum");


--
-- Name: apptgeneralmessagesent apptgeneralmessagesent_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.apptgeneralmessagesent
    ADD CONSTRAINT apptgeneralmessagesent_pkey PRIMARY KEY ("ApptGeneralMessageSentNum");


--
-- Name: apptnewpatthankyousent apptnewpatthankyousent_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.apptnewpatthankyousent
    ADD CONSTRAINT apptnewpatthankyousent_pkey PRIMARY KEY ("ApptNewPatThankYouSentNum");


--
-- Name: apptreminderrule apptreminderrule_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.apptreminderrule
    ADD CONSTRAINT apptreminderrule_pkey PRIMARY KEY ("ApptReminderRuleNum");


--
-- Name: apptremindersent apptremindersent_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.apptremindersent
    ADD CONSTRAINT apptremindersent_pkey PRIMARY KEY ("ApptReminderSentNum");


--
-- Name: apptthankyousent apptthankyousent_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.apptthankyousent
    ADD CONSTRAINT apptthankyousent_pkey PRIMARY KEY ("ApptThankYouSentNum");


--
-- Name: apptview apptview_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.apptview
    ADD CONSTRAINT apptview_pkey PRIMARY KEY ("ApptViewNum");


--
-- Name: apptviewitem apptviewitem_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.apptviewitem
    ADD CONSTRAINT apptviewitem_pkey PRIMARY KEY ("ApptViewItemNum");


--
-- Name: asapcomm asapcomm_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.asapcomm
    ADD CONSTRAINT asapcomm_pkey PRIMARY KEY ("AsapCommNum");


--
-- Name: autocode autocode_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.autocode
    ADD CONSTRAINT autocode_pkey PRIMARY KEY ("AutoCodeNum");


--
-- Name: autocodecond autocodecond_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.autocodecond
    ADD CONSTRAINT autocodecond_pkey PRIMARY KEY ("AutoCodeCondNum");


--
-- Name: autocodeitem autocodeitem_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.autocodeitem
    ADD CONSTRAINT autocodeitem_pkey PRIMARY KEY ("AutoCodeItemNum");


--
-- Name: autocommexcludedate autocommexcludedate_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.autocommexcludedate
    ADD CONSTRAINT autocommexcludedate_pkey PRIMARY KEY ("AutoCommExcludeDateNum");


--
-- Name: automation automation_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.automation
    ADD CONSTRAINT automation_pkey PRIMARY KEY ("AutomationNum");


--
-- Name: automationcondition automationcondition_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.automationcondition
    ADD CONSTRAINT automationcondition_pkey PRIMARY KEY ("AutomationConditionNum");


--
-- Name: autonote autonote_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.autonote
    ADD CONSTRAINT autonote_pkey PRIMARY KEY ("AutoNoteNum");


--
-- Name: autonotecontrol autonotecontrol_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.autonotecontrol
    ADD CONSTRAINT autonotecontrol_pkey PRIMARY KEY ("AutoNoteControlNum");


--
-- Name: benefit benefit_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.benefit
    ADD CONSTRAINT benefit_pkey PRIMARY KEY ("BenefitNum");


--
-- Name: branding branding_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.branding
    ADD CONSTRAINT branding_pkey PRIMARY KEY ("BrandingNum");


--
-- Name: canadiannetwork canadiannetwork_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.canadiannetwork
    ADD CONSTRAINT canadiannetwork_pkey PRIMARY KEY ("CanadianNetworkNum");


--
-- Name: carecreditwebresponse carecreditwebresponse_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.carecreditwebresponse
    ADD CONSTRAINT carecreditwebresponse_pkey PRIMARY KEY ("CareCreditWebResponseNum");


--
-- Name: carrier carrier_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.carrier
    ADD CONSTRAINT carrier_pkey PRIMARY KEY ("CarrierNum");


--
-- Name: cdcrec cdcrec_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.cdcrec
    ADD CONSTRAINT cdcrec_pkey PRIMARY KEY ("CdcrecNum");


--
-- Name: cdspermission cdspermission_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.cdspermission
    ADD CONSTRAINT cdspermission_pkey PRIMARY KEY ("CDSPermissionNum");


--
-- Name: centralconnection centralconnection_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.centralconnection
    ADD CONSTRAINT centralconnection_pkey PRIMARY KEY ("CentralConnectionNum");


--
-- Name: cert cert_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.cert
    ADD CONSTRAINT cert_pkey PRIMARY KEY ("CertNum");


--
-- Name: certemployee certemployee_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.certemployee
    ADD CONSTRAINT certemployee_pkey PRIMARY KEY ("CertEmployeeNum");


--
-- Name: chartview chartview_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.chartview
    ADD CONSTRAINT chartview_pkey PRIMARY KEY ("ChartViewNum");


--
-- Name: claim claim_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.claim
    ADD CONSTRAINT claim_pkey PRIMARY KEY ("ClaimNum");


--
-- Name: claimattach claimattach_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.claimattach
    ADD CONSTRAINT claimattach_pkey PRIMARY KEY ("ClaimAttachNum");


--
-- Name: claimcondcodelog claimcondcodelog_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.claimcondcodelog
    ADD CONSTRAINT claimcondcodelog_pkey PRIMARY KEY ("ClaimCondCodeLogNum");


--
-- Name: claimform claimform_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.claimform
    ADD CONSTRAINT claimform_pkey PRIMARY KEY ("ClaimFormNum");


--
-- Name: claimformitem claimformitem_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.claimformitem
    ADD CONSTRAINT claimformitem_pkey PRIMARY KEY ("ClaimFormItemNum");


--
-- Name: claimpayment claimpayment_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.claimpayment
    ADD CONSTRAINT claimpayment_pkey PRIMARY KEY ("ClaimPaymentNum");


--
-- Name: claimproc claimproc_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.claimproc
    ADD CONSTRAINT claimproc_pkey PRIMARY KEY ("ClaimProcNum");


--
-- Name: claimsnapshot claimsnapshot_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.claimsnapshot
    ADD CONSTRAINT claimsnapshot_pkey PRIMARY KEY ("ClaimSnapshotNum");


--
-- Name: claimtracking claimtracking_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.claimtracking
    ADD CONSTRAINT claimtracking_pkey PRIMARY KEY ("ClaimTrackingNum");


--
-- Name: claimvalcodelog claimvalcodelog_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.claimvalcodelog
    ADD CONSTRAINT claimvalcodelog_pkey PRIMARY KEY ("ClaimValCodeLogNum");


--
-- Name: clearinghouse clearinghouse_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.clearinghouse
    ADD CONSTRAINT clearinghouse_pkey PRIMARY KEY ("ClearinghouseNum");


--
-- Name: clinic clinic_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.clinic
    ADD CONSTRAINT clinic_pkey PRIMARY KEY ("ClinicNum");


--
-- Name: clinicerx clinicerx_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.clinicerx
    ADD CONSTRAINT clinicerx_pkey PRIMARY KEY ("ClinicErxNum");


--
-- Name: clinicpref clinicpref_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.clinicpref
    ADD CONSTRAINT clinicpref_pkey PRIMARY KEY ("ClinicPrefNum");


--
-- Name: clockevent clockevent_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.clockevent
    ADD CONSTRAINT clockevent_pkey PRIMARY KEY ("ClockEventNum");


--
-- Name: cloudaddress cloudaddress_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.cloudaddress
    ADD CONSTRAINT cloudaddress_pkey PRIMARY KEY ("CloudAddressNum");


--
-- Name: codegroup codegroup_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.codegroup
    ADD CONSTRAINT codegroup_pkey PRIMARY KEY ("CodeGroupNum");


--
-- Name: codesystem codesystem_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.codesystem
    ADD CONSTRAINT codesystem_pkey PRIMARY KEY ("CodeSystemNum");


--
-- Name: commlog commlog_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.commlog
    ADD CONSTRAINT commlog_pkey PRIMARY KEY ("CommlogNum");


--
-- Name: commoptout commoptout_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.commoptout
    ADD CONSTRAINT commoptout_pkey PRIMARY KEY ("CommOptOutNum");


--
-- Name: computer computer_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.computer
    ADD CONSTRAINT computer_pkey PRIMARY KEY ("ComputerNum");


--
-- Name: computerpref computerpref_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.computerpref
    ADD CONSTRAINT computerpref_pkey PRIMARY KEY ("ComputerPrefNum");


--
-- Name: confirmationrequest confirmationrequest_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.confirmationrequest
    ADD CONSTRAINT confirmationrequest_pkey PRIMARY KEY ("ConfirmationRequestNum");


--
-- Name: connectiongroup connectiongroup_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.connectiongroup
    ADD CONSTRAINT connectiongroup_pkey PRIMARY KEY ("ConnectionGroupNum");


--
-- Name: conngroupattach conngroupattach_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.conngroupattach
    ADD CONSTRAINT conngroupattach_pkey PRIMARY KEY ("ConnGroupAttachNum");


--
-- Name: contact contact_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.contact
    ADD CONSTRAINT contact_pkey PRIMARY KEY ("ContactNum");


--
-- Name: county county_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.county
    ADD CONSTRAINT county_pkey PRIMARY KEY ("CountyNum");


--
-- Name: covcat covcat_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.covcat
    ADD CONSTRAINT covcat_pkey PRIMARY KEY ("CovCatNum");


--
-- Name: covspan covspan_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.covspan
    ADD CONSTRAINT covspan_pkey PRIMARY KEY ("CovSpanNum");


--
-- Name: cpt cpt_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.cpt
    ADD CONSTRAINT cpt_pkey PRIMARY KEY ("CptNum");


--
-- Name: creditcard creditcard_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.creditcard
    ADD CONSTRAINT creditcard_pkey PRIMARY KEY ("CreditCardNum");


--
-- Name: custrefentry custrefentry_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.custrefentry
    ADD CONSTRAINT custrefentry_pkey PRIMARY KEY ("CustRefEntryNum");


--
-- Name: custreference custreference_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.custreference
    ADD CONSTRAINT custreference_pkey PRIMARY KEY ("CustReferenceNum");


--
-- Name: cvx cvx_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.cvx
    ADD CONSTRAINT cvx_pkey PRIMARY KEY ("CvxNum");


--
-- Name: dashboardar dashboardar_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.dashboardar
    ADD CONSTRAINT dashboardar_pkey PRIMARY KEY ("DashboardARNum");


--
-- Name: dashboardcell dashboardcell_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.dashboardcell
    ADD CONSTRAINT dashboardcell_pkey PRIMARY KEY ("DashboardCellNum");


--
-- Name: dashboardlayout dashboardlayout_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.dashboardlayout
    ADD CONSTRAINT dashboardlayout_pkey PRIMARY KEY ("DashboardLayoutNum");


--
-- Name: databasemaintenance databasemaintenance_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.databasemaintenance
    ADD CONSTRAINT databasemaintenance_pkey PRIMARY KEY ("DatabaseMaintenanceNum");


--
-- Name: dbmlog dbmlog_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.dbmlog
    ADD CONSTRAINT dbmlog_pkey PRIMARY KEY ("DbmLogNum");


--
-- Name: definition definition_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.definition
    ADD CONSTRAINT definition_pkey PRIMARY KEY ("DefNum");


--
-- Name: deflink deflink_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.deflink
    ADD CONSTRAINT deflink_pkey PRIMARY KEY ("DefLinkNum");


--
-- Name: deletedobject deletedobject_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.deletedobject
    ADD CONSTRAINT deletedobject_pkey PRIMARY KEY ("DeletedObjectNum");


--
-- Name: deposit deposit_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.deposit
    ADD CONSTRAINT deposit_pkey PRIMARY KEY ("DepositNum");


--
-- Name: dictcustom dictcustom_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.dictcustom
    ADD CONSTRAINT dictcustom_pkey PRIMARY KEY ("DictCustomNum");


--
-- Name: discountplan discountplan_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.discountplan
    ADD CONSTRAINT discountplan_pkey PRIMARY KEY ("DiscountPlanNum");


--
-- Name: discountplansub discountplansub_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.discountplansub
    ADD CONSTRAINT discountplansub_pkey PRIMARY KEY ("DiscountSubNum");


--
-- Name: disease disease_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.disease
    ADD CONSTRAINT disease_pkey PRIMARY KEY ("DiseaseNum");


--
-- Name: diseasedef diseasedef_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.diseasedef
    ADD CONSTRAINT diseasedef_pkey PRIMARY KEY ("DiseaseDefNum");


--
-- Name: displayfield displayfield_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.displayfield
    ADD CONSTRAINT displayfield_pkey PRIMARY KEY ("DisplayFieldNum");


--
-- Name: displayreport displayreport_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.displayreport
    ADD CONSTRAINT displayreport_pkey PRIMARY KEY ("DisplayReportNum");


--
-- Name: dispsupply dispsupply_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.dispsupply
    ADD CONSTRAINT dispsupply_pkey PRIMARY KEY ("DispSupplyNum");


--
-- Name: document document_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.document
    ADD CONSTRAINT document_pkey PRIMARY KEY ("DocNum");


--
-- Name: documentmisc documentmisc_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.documentmisc
    ADD CONSTRAINT documentmisc_pkey PRIMARY KEY ("DocMiscNum");


--
-- Name: drugmanufacturer drugmanufacturer_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.drugmanufacturer
    ADD CONSTRAINT drugmanufacturer_pkey PRIMARY KEY ("DrugManufacturerNum");


--
-- Name: drugunit drugunit_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.drugunit
    ADD CONSTRAINT drugunit_pkey PRIMARY KEY ("DrugUnitNum");


--
-- Name: dunning dunning_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.dunning
    ADD CONSTRAINT dunning_pkey PRIMARY KEY ("DunningNum");


--
-- Name: ebill ebill_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.ebill
    ADD CONSTRAINT ebill_pkey PRIMARY KEY ("EbillNum");


--
-- Name: eclipboardimagecapture eclipboardimagecapture_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.eclipboardimagecapture
    ADD CONSTRAINT eclipboardimagecapture_pkey PRIMARY KEY ("EClipboardImageCaptureNum");


--
-- Name: eclipboardimagecapturedef eclipboardimagecapturedef_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.eclipboardimagecapturedef
    ADD CONSTRAINT eclipboardimagecapturedef_pkey PRIMARY KEY ("EClipboardImageCaptureDefNum");


--
-- Name: eclipboardsheetdef eclipboardsheetdef_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.eclipboardsheetdef
    ADD CONSTRAINT eclipboardsheetdef_pkey PRIMARY KEY ("EClipboardSheetDefNum");


--
-- Name: eduresource eduresource_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.eduresource
    ADD CONSTRAINT eduresource_pkey PRIMARY KEY ("EduResourceNum");


--
-- Name: eform eform_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.eform
    ADD CONSTRAINT eform_pkey PRIMARY KEY ("EFormNum");


--
-- Name: eformdef eformdef_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.eformdef
    ADD CONSTRAINT eformdef_pkey PRIMARY KEY ("EFormDefNum");


--
-- Name: eformfield eformfield_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.eformfield
    ADD CONSTRAINT eformfield_pkey PRIMARY KEY ("EFormFieldNum");


--
-- Name: eformfielddef eformfielddef_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.eformfielddef
    ADD CONSTRAINT eformfielddef_pkey PRIMARY KEY ("EFormFieldDefNum");


--
-- Name: eformimportrule eformimportrule_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.eformimportrule
    ADD CONSTRAINT eformimportrule_pkey PRIMARY KEY ("EFormImportRuleNum");


--
-- Name: ehramendment ehramendment_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.ehramendment
    ADD CONSTRAINT ehramendment_pkey PRIMARY KEY ("EhrAmendmentNum");


--
-- Name: ehraptobs ehraptobs_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.ehraptobs
    ADD CONSTRAINT ehraptobs_pkey PRIMARY KEY ("EhrAptObsNum");


--
-- Name: ehrcareplan ehrcareplan_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.ehrcareplan
    ADD CONSTRAINT ehrcareplan_pkey PRIMARY KEY ("EhrCarePlanNum");


--
-- Name: ehrlab ehrlab_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.ehrlab
    ADD CONSTRAINT ehrlab_pkey PRIMARY KEY ("EhrLabNum");


--
-- Name: ehrlabclinicalinfo ehrlabclinicalinfo_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.ehrlabclinicalinfo
    ADD CONSTRAINT ehrlabclinicalinfo_pkey PRIMARY KEY ("EhrLabClinicalInfoNum");


--
-- Name: ehrlabimage ehrlabimage_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.ehrlabimage
    ADD CONSTRAINT ehrlabimage_pkey PRIMARY KEY ("EhrLabImageNum");


--
-- Name: ehrlabnote ehrlabnote_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.ehrlabnote
    ADD CONSTRAINT ehrlabnote_pkey PRIMARY KEY ("EhrLabNoteNum");


--
-- Name: ehrlabresult ehrlabresult_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.ehrlabresult
    ADD CONSTRAINT ehrlabresult_pkey PRIMARY KEY ("EhrLabResultNum");


--
-- Name: ehrlabresultscopyto ehrlabresultscopyto_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.ehrlabresultscopyto
    ADD CONSTRAINT ehrlabresultscopyto_pkey PRIMARY KEY ("EhrLabResultsCopyToNum");


--
-- Name: ehrlabspecimen ehrlabspecimen_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.ehrlabspecimen
    ADD CONSTRAINT ehrlabspecimen_pkey PRIMARY KEY ("EhrLabSpecimenNum");


--
-- Name: ehrlabspecimencondition ehrlabspecimencondition_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.ehrlabspecimencondition
    ADD CONSTRAINT ehrlabspecimencondition_pkey PRIMARY KEY ("EhrLabSpecimenConditionNum");


--
-- Name: ehrlabspecimenrejectreason ehrlabspecimenrejectreason_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.ehrlabspecimenrejectreason
    ADD CONSTRAINT ehrlabspecimenrejectreason_pkey PRIMARY KEY ("EhrLabSpecimenRejectReasonNum");


--
-- Name: ehrmeasure ehrmeasure_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.ehrmeasure
    ADD CONSTRAINT ehrmeasure_pkey PRIMARY KEY ("EhrMeasureNum");


--
-- Name: ehrmeasureevent ehrmeasureevent_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.ehrmeasureevent
    ADD CONSTRAINT ehrmeasureevent_pkey PRIMARY KEY ("EhrMeasureEventNum");


--
-- Name: ehrnotperformed ehrnotperformed_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.ehrnotperformed
    ADD CONSTRAINT ehrnotperformed_pkey PRIMARY KEY ("EhrNotPerformedNum");


--
-- Name: ehrpatient ehrpatient_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.ehrpatient
    ADD CONSTRAINT ehrpatient_pkey PRIMARY KEY ("PatNum");


--
-- Name: ehrprovkey ehrprovkey_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.ehrprovkey
    ADD CONSTRAINT ehrprovkey_pkey PRIMARY KEY ("EhrProvKeyNum");


--
-- Name: ehrquarterlykey ehrquarterlykey_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.ehrquarterlykey
    ADD CONSTRAINT ehrquarterlykey_pkey PRIMARY KEY ("EhrQuarterlyKeyNum");


--
-- Name: ehrsummaryccd ehrsummaryccd_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.ehrsummaryccd
    ADD CONSTRAINT ehrsummaryccd_pkey PRIMARY KEY ("EhrSummaryCcdNum");


--
-- Name: ehrtrigger ehrtrigger_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.ehrtrigger
    ADD CONSTRAINT ehrtrigger_pkey PRIMARY KEY ("EhrTriggerNum");


--
-- Name: electid electid_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.electid
    ADD CONSTRAINT electid_pkey PRIMARY KEY ("ElectIDNum");


--
-- Name: emailaddress emailaddress_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.emailaddress
    ADD CONSTRAINT emailaddress_pkey PRIMARY KEY ("EmailAddressNum");


--
-- Name: emailattach emailattach_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.emailattach
    ADD CONSTRAINT emailattach_pkey PRIMARY KEY ("EmailAttachNum");


--
-- Name: emailautograph emailautograph_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.emailautograph
    ADD CONSTRAINT emailautograph_pkey PRIMARY KEY ("EmailAutographNum");


--
-- Name: emailhostingtemplate emailhostingtemplate_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.emailhostingtemplate
    ADD CONSTRAINT emailhostingtemplate_pkey PRIMARY KEY ("EmailHostingTemplateNum");


--
-- Name: emailmessage emailmessage_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.emailmessage
    ADD CONSTRAINT emailmessage_pkey PRIMARY KEY ("EmailMessageNum");


--
-- Name: emailmessageuid emailmessageuid_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.emailmessageuid
    ADD CONSTRAINT emailmessageuid_pkey PRIMARY KEY ("EmailMessageUidNum");


--
-- Name: emailsecure emailsecure_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.emailsecure
    ADD CONSTRAINT emailsecure_pkey PRIMARY KEY ("EmailSecureNum");


--
-- Name: emailsecureattach emailsecureattach_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.emailsecureattach
    ADD CONSTRAINT emailsecureattach_pkey PRIMARY KEY ("EmailSecureAttachNum");


--
-- Name: emailtemplate emailtemplate_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.emailtemplate
    ADD CONSTRAINT emailtemplate_pkey PRIMARY KEY ("EmailTemplateNum");


--
-- Name: employee employee_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.employee
    ADD CONSTRAINT employee_pkey PRIMARY KEY ("EmployeeNum");


--
-- Name: employer employer_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.employer
    ADD CONSTRAINT employer_pkey PRIMARY KEY ("EmployerNum");


--
-- Name: encounter encounter_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.encounter
    ADD CONSTRAINT encounter_pkey PRIMARY KEY ("EncounterNum");


--
-- Name: entrylog entrylog_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.entrylog
    ADD CONSTRAINT entrylog_pkey PRIMARY KEY ("EntryLogNum");


--
-- Name: eobattach eobattach_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.eobattach
    ADD CONSTRAINT eobattach_pkey PRIMARY KEY ("EobAttachNum");


--
-- Name: equipment equipment_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.equipment
    ADD CONSTRAINT equipment_pkey PRIMARY KEY ("EquipmentNum");


--
-- Name: erouting erouting_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.erouting
    ADD CONSTRAINT erouting_pkey PRIMARY KEY ("ERoutingNum");


--
-- Name: eroutingaction eroutingaction_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.eroutingaction
    ADD CONSTRAINT eroutingaction_pkey PRIMARY KEY ("ERoutingActionNum");


--
-- Name: erxlog erxlog_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.erxlog
    ADD CONSTRAINT erxlog_pkey PRIMARY KEY ("ErxLogNum");


--
-- Name: eservicelog eservicelog_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.eservicelog
    ADD CONSTRAINT eservicelog_pkey PRIMARY KEY ("EServiceLogNum");


--
-- Name: eserviceshortguid eserviceshortguid_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.eserviceshortguid
    ADD CONSTRAINT eserviceshortguid_pkey PRIMARY KEY ("EServiceShortGuidNum");


--
-- Name: eservicesignal eservicesignal_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.eservicesignal
    ADD CONSTRAINT eservicesignal_pkey PRIMARY KEY ("EServiceSignalNum");


--
-- Name: etl_load_status etl_load_status_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.etl_load_status
    ADD CONSTRAINT etl_load_status_pkey PRIMARY KEY (id);


--
-- Name: etl_load_status etl_load_status_table_name_key; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.etl_load_status
    ADD CONSTRAINT etl_load_status_table_name_key UNIQUE (table_name);


--
-- Name: etl_pipeline_metrics etl_pipeline_metrics_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.etl_pipeline_metrics
    ADD CONSTRAINT etl_pipeline_metrics_pkey PRIMARY KEY (id);


--
-- Name: etl_table_metrics etl_table_metrics_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.etl_table_metrics
    ADD CONSTRAINT etl_table_metrics_pkey PRIMARY KEY (id);


--
-- Name: etl_transform_status etl_transform_status_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.etl_transform_status
    ADD CONSTRAINT etl_transform_status_pkey PRIMARY KEY (id);


--
-- Name: etl_transform_status etl_transform_status_table_name_key; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.etl_transform_status
    ADD CONSTRAINT etl_transform_status_table_name_key UNIQUE (table_name);


--
-- Name: etrans835 etrans835_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.etrans835
    ADD CONSTRAINT etrans835_pkey PRIMARY KEY ("Etrans835Num");


--
-- Name: etrans835attach etrans835attach_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.etrans835attach
    ADD CONSTRAINT etrans835attach_pkey PRIMARY KEY ("Etrans835AttachNum");


--
-- Name: etrans etrans_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.etrans
    ADD CONSTRAINT etrans_pkey PRIMARY KEY ("EtransNum");


--
-- Name: etransmessagetext etransmessagetext_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.etransmessagetext
    ADD CONSTRAINT etransmessagetext_pkey PRIMARY KEY ("EtransMessageTextNum");


--
-- Name: evaluation evaluation_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.evaluation
    ADD CONSTRAINT evaluation_pkey PRIMARY KEY ("EvaluationNum");


--
-- Name: evaluationcriterion evaluationcriterion_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.evaluationcriterion
    ADD CONSTRAINT evaluationcriterion_pkey PRIMARY KEY ("EvaluationCriterionNum");


--
-- Name: famaging famaging_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.famaging
    ADD CONSTRAINT famaging_pkey PRIMARY KEY ("PatNum");


--
-- Name: familyhealth familyhealth_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.familyhealth
    ADD CONSTRAINT familyhealth_pkey PRIMARY KEY ("FamilyHealthNum");


--
-- Name: fee fee_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.fee
    ADD CONSTRAINT fee_pkey PRIMARY KEY ("FeeNum");


--
-- Name: feesched feesched_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.feesched
    ADD CONSTRAINT feesched_pkey PRIMARY KEY ("FeeSchedNum");


--
-- Name: feeschedgroup feeschedgroup_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.feeschedgroup
    ADD CONSTRAINT feeschedgroup_pkey PRIMARY KEY ("FeeSchedGroupNum");


--
-- Name: fhircontactpoint fhircontactpoint_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.fhircontactpoint
    ADD CONSTRAINT fhircontactpoint_pkey PRIMARY KEY ("FHIRContactPointNum");


--
-- Name: fhirsubscription fhirsubscription_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.fhirsubscription
    ADD CONSTRAINT fhirsubscription_pkey PRIMARY KEY ("FHIRSubscriptionNum");


--
-- Name: files files_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.files
    ADD CONSTRAINT files_pkey PRIMARY KEY ("DocNum");


--
-- Name: formpat formpat_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.formpat
    ADD CONSTRAINT formpat_pkey PRIMARY KEY ("FormPatNum");


--
-- Name: gradingscale gradingscale_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.gradingscale
    ADD CONSTRAINT gradingscale_pkey PRIMARY KEY ("GradingScaleNum");


--
-- Name: gradingscaleitem gradingscaleitem_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.gradingscaleitem
    ADD CONSTRAINT gradingscaleitem_pkey PRIMARY KEY ("GradingScaleItemNum");


--
-- Name: grouppermission grouppermission_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.grouppermission
    ADD CONSTRAINT grouppermission_pkey PRIMARY KEY ("GroupPermNum");


--
-- Name: guardian guardian_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.guardian
    ADD CONSTRAINT guardian_pkey PRIMARY KEY ("GuardianNum");


--
-- Name: hcpcs hcpcs_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.hcpcs
    ADD CONSTRAINT hcpcs_pkey PRIMARY KEY ("HcpcsNum");


--
-- Name: hieclinic hieclinic_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.hieclinic
    ADD CONSTRAINT hieclinic_pkey PRIMARY KEY ("HieClinicNum");


--
-- Name: hiequeue hiequeue_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.hiequeue
    ADD CONSTRAINT hiequeue_pkey PRIMARY KEY ("HieQueueNum");


--
-- Name: histappointment histappointment_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.histappointment
    ADD CONSTRAINT histappointment_pkey PRIMARY KEY ("HistApptNum");


--
-- Name: hl7msg hl7msg_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.hl7msg
    ADD CONSTRAINT hl7msg_pkey PRIMARY KEY ("HL7MsgNum");


--
-- Name: hl7procattach hl7procattach_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.hl7procattach
    ADD CONSTRAINT hl7procattach_pkey PRIMARY KEY ("HL7ProcAttachNum");


--
-- Name: icd10 icd10_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.icd10
    ADD CONSTRAINT icd10_pkey PRIMARY KEY ("Icd10Num");


--
-- Name: icd9 icd9_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.icd9
    ADD CONSTRAINT icd9_pkey PRIMARY KEY ("ICD9Num");


--
-- Name: imagedraw imagedraw_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.imagedraw
    ADD CONSTRAINT imagedraw_pkey PRIMARY KEY ("ImageDrawNum");


--
-- Name: imagingdevice imagingdevice_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.imagingdevice
    ADD CONSTRAINT imagingdevice_pkey PRIMARY KEY ("ImagingDeviceNum");


--
-- Name: insbluebook insbluebook_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.insbluebook
    ADD CONSTRAINT insbluebook_pkey PRIMARY KEY ("InsBlueBookNum");


--
-- Name: insbluebooklog insbluebooklog_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.insbluebooklog
    ADD CONSTRAINT insbluebooklog_pkey PRIMARY KEY ("InsBlueBookLogNum");


--
-- Name: insbluebookrule insbluebookrule_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.insbluebookrule
    ADD CONSTRAINT insbluebookrule_pkey PRIMARY KEY ("InsBlueBookRuleNum");


--
-- Name: inseditlog inseditlog_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.inseditlog
    ADD CONSTRAINT inseditlog_pkey PRIMARY KEY ("InsEditLogNum");


--
-- Name: inseditpatlog inseditpatlog_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.inseditpatlog
    ADD CONSTRAINT inseditpatlog_pkey PRIMARY KEY ("InsEditPatLogNum");


--
-- Name: insfilingcode insfilingcode_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.insfilingcode
    ADD CONSTRAINT insfilingcode_pkey PRIMARY KEY ("InsFilingCodeNum");


--
-- Name: insplan insplan_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.insplan
    ADD CONSTRAINT insplan_pkey PRIMARY KEY ("PlanNum");


--
-- Name: insplanpreference insplanpreference_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.insplanpreference
    ADD CONSTRAINT insplanpreference_pkey PRIMARY KEY ("InsPlanPrefNum");


--
-- Name: inssub inssub_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.inssub
    ADD CONSTRAINT inssub_pkey PRIMARY KEY ("InsSubNum");


--
-- Name: installmentplan installmentplan_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.installmentplan
    ADD CONSTRAINT installmentplan_pkey PRIMARY KEY ("InstallmentPlanNum");


--
-- Name: instructor instructor_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.instructor
    ADD CONSTRAINT instructor_pkey PRIMARY KEY ("InstructorNum");


--
-- Name: insverify insverify_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.insverify
    ADD CONSTRAINT insverify_pkey PRIMARY KEY ("InsVerifyNum");


--
-- Name: insverifyhist insverifyhist_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.insverifyhist
    ADD CONSTRAINT insverifyhist_pkey PRIMARY KEY ("InsVerifyHistNum");


--
-- Name: intervention intervention_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.intervention
    ADD CONSTRAINT intervention_pkey PRIMARY KEY ("InterventionNum");


--
-- Name: journalentry journalentry_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.journalentry
    ADD CONSTRAINT journalentry_pkey PRIMARY KEY ("JournalEntryNum");


--
-- Name: labcase labcase_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.labcase
    ADD CONSTRAINT labcase_pkey PRIMARY KEY ("LabCaseNum");


--
-- Name: laboratory laboratory_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.laboratory
    ADD CONSTRAINT laboratory_pkey PRIMARY KEY ("LaboratoryNum");


--
-- Name: labpanel labpanel_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.labpanel
    ADD CONSTRAINT labpanel_pkey PRIMARY KEY ("LabPanelNum");


--
-- Name: labresult labresult_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.labresult
    ADD CONSTRAINT labresult_pkey PRIMARY KEY ("LabResultNum");


--
-- Name: labturnaround labturnaround_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.labturnaround
    ADD CONSTRAINT labturnaround_pkey PRIMARY KEY ("LabTurnaroundNum");


--
-- Name: language language_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.language
    ADD CONSTRAINT language_pkey PRIMARY KEY ("LanguageNum");


--
-- Name: languageforeign languageforeign_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.languageforeign
    ADD CONSTRAINT languageforeign_pkey PRIMARY KEY ("LanguageForeignNum");


--
-- Name: languagepat languagepat_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.languagepat
    ADD CONSTRAINT languagepat_pkey PRIMARY KEY ("LanguagePatNum");


--
-- Name: letter letter_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.letter
    ADD CONSTRAINT letter_pkey PRIMARY KEY ("LetterNum");


--
-- Name: lettermerge lettermerge_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.lettermerge
    ADD CONSTRAINT lettermerge_pkey PRIMARY KEY ("LetterMergeNum");


--
-- Name: lettermergefield lettermergefield_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.lettermergefield
    ADD CONSTRAINT lettermergefield_pkey PRIMARY KEY ("FieldNum");


--
-- Name: limitedbetafeature limitedbetafeature_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.limitedbetafeature
    ADD CONSTRAINT limitedbetafeature_pkey PRIMARY KEY ("LimitedBetaFeatureNum");


--
-- Name: loginattempt loginattempt_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.loginattempt
    ADD CONSTRAINT loginattempt_pkey PRIMARY KEY ("LoginAttemptNum");


--
-- Name: loinc loinc_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.loinc
    ADD CONSTRAINT loinc_pkey PRIMARY KEY ("LoincNum");


--
-- Name: maparea maparea_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.maparea
    ADD CONSTRAINT maparea_pkey PRIMARY KEY ("MapAreaNum");


--
-- Name: medicalorder medicalorder_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.medicalorder
    ADD CONSTRAINT medicalorder_pkey PRIMARY KEY ("MedicalOrderNum");


--
-- Name: medication medication_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.medication
    ADD CONSTRAINT medication_pkey PRIMARY KEY ("MedicationNum");


--
-- Name: medicationpat medicationpat_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.medicationpat
    ADD CONSTRAINT medicationpat_pkey PRIMARY KEY ("MedicationPatNum");


--
-- Name: medlab medlab_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.medlab
    ADD CONSTRAINT medlab_pkey PRIMARY KEY ("MedLabNum");


--
-- Name: medlabfacattach medlabfacattach_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.medlabfacattach
    ADD CONSTRAINT medlabfacattach_pkey PRIMARY KEY ("MedLabFacAttachNum");


--
-- Name: medlabfacility medlabfacility_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.medlabfacility
    ADD CONSTRAINT medlabfacility_pkey PRIMARY KEY ("MedLabFacilityNum");


--
-- Name: medlabresult medlabresult_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.medlabresult
    ADD CONSTRAINT medlabresult_pkey PRIMARY KEY ("MedLabResultNum");


--
-- Name: medlabspecimen medlabspecimen_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.medlabspecimen
    ADD CONSTRAINT medlabspecimen_pkey PRIMARY KEY ("MedLabSpecimenNum");


--
-- Name: mobileappdevice mobileappdevice_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.mobileappdevice
    ADD CONSTRAINT mobileappdevice_pkey PRIMARY KEY ("MobileAppDeviceNum");


--
-- Name: mobilebrandingprofile mobilebrandingprofile_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.mobilebrandingprofile
    ADD CONSTRAINT mobilebrandingprofile_pkey PRIMARY KEY ("MobileBrandingProfileNum");


--
-- Name: mobiledatabyte mobiledatabyte_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.mobiledatabyte
    ADD CONSTRAINT mobiledatabyte_pkey PRIMARY KEY ("MobileDataByteNum");


--
-- Name: mobilenotification mobilenotification_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.mobilenotification
    ADD CONSTRAINT mobilenotification_pkey PRIMARY KEY ("MobileNotificationNum");


--
-- Name: mount mount_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.mount
    ADD CONSTRAINT mount_pkey PRIMARY KEY ("MountNum");


--
-- Name: mountdef mountdef_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.mountdef
    ADD CONSTRAINT mountdef_pkey PRIMARY KEY ("MountDefNum");


--
-- Name: mountitem mountitem_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.mountitem
    ADD CONSTRAINT mountitem_pkey PRIMARY KEY ("MountItemNum");


--
-- Name: mountitemdef mountitemdef_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.mountitemdef
    ADD CONSTRAINT mountitemdef_pkey PRIMARY KEY ("MountItemDefNum");


--
-- Name: msgtopaysent msgtopaysent_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.msgtopaysent
    ADD CONSTRAINT msgtopaysent_pkey PRIMARY KEY ("MsgToPaySentNum");


--
-- Name: oidexternal oidexternal_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.oidexternal
    ADD CONSTRAINT oidexternal_pkey PRIMARY KEY ("OIDExternalNum");


--
-- Name: oidinternal oidinternal_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.oidinternal
    ADD CONSTRAINT oidinternal_pkey PRIMARY KEY ("OIDInternalNum");


--
-- Name: operatory operatory_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.operatory
    ADD CONSTRAINT operatory_pkey PRIMARY KEY ("OperatoryNum");


--
-- Name: orionproc orionproc_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.orionproc
    ADD CONSTRAINT orionproc_pkey PRIMARY KEY ("OrionProcNum");


--
-- Name: orthocase orthocase_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.orthocase
    ADD CONSTRAINT orthocase_pkey PRIMARY KEY ("OrthoCaseNum");


--
-- Name: orthochart orthochart_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.orthochart
    ADD CONSTRAINT orthochart_pkey PRIMARY KEY ("OrthoChartNum");


--
-- Name: orthochartlog orthochartlog_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.orthochartlog
    ADD CONSTRAINT orthochartlog_pkey PRIMARY KEY ("OrthoChartLogNum");


--
-- Name: orthochartrow orthochartrow_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.orthochartrow
    ADD CONSTRAINT orthochartrow_pkey PRIMARY KEY ("OrthoChartRowNum");


--
-- Name: orthocharttab orthocharttab_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.orthocharttab
    ADD CONSTRAINT orthocharttab_pkey PRIMARY KEY ("OrthoChartTabNum");


--
-- Name: orthocharttablink orthocharttablink_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.orthocharttablink
    ADD CONSTRAINT orthocharttablink_pkey PRIMARY KEY ("OrthoChartTabLinkNum");


--
-- Name: orthohardware orthohardware_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.orthohardware
    ADD CONSTRAINT orthohardware_pkey PRIMARY KEY ("OrthoHardwareNum");


--
-- Name: orthohardwarespec orthohardwarespec_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.orthohardwarespec
    ADD CONSTRAINT orthohardwarespec_pkey PRIMARY KEY ("OrthoHardwareSpecNum");


--
-- Name: orthoplanlink orthoplanlink_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.orthoplanlink
    ADD CONSTRAINT orthoplanlink_pkey PRIMARY KEY ("OrthoPlanLinkNum");


--
-- Name: orthoproclink orthoproclink_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.orthoproclink
    ADD CONSTRAINT orthoproclink_pkey PRIMARY KEY ("OrthoProcLinkNum");


--
-- Name: orthorx orthorx_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.orthorx
    ADD CONSTRAINT orthorx_pkey PRIMARY KEY ("OrthoRxNum");


--
-- Name: orthoschedule orthoschedule_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.orthoschedule
    ADD CONSTRAINT orthoschedule_pkey PRIMARY KEY ("OrthoScheduleNum");


--
-- Name: patfield patfield_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.patfield
    ADD CONSTRAINT patfield_pkey PRIMARY KEY ("PatFieldNum");


--
-- Name: patfielddef patfielddef_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.patfielddef
    ADD CONSTRAINT patfielddef_pkey PRIMARY KEY ("PatFieldDefNum");


--
-- Name: patfieldpickitem patfieldpickitem_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.patfieldpickitem
    ADD CONSTRAINT patfieldpickitem_pkey PRIMARY KEY ("PatFieldPickItemNum");


--
-- Name: patient patient_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.patient
    ADD CONSTRAINT patient_pkey PRIMARY KEY ("PatNum");


--
-- Name: patientlink patientlink_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.patientlink
    ADD CONSTRAINT patientlink_pkey PRIMARY KEY ("PatientLinkNum");


--
-- Name: patientnote patientnote_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.patientnote
    ADD CONSTRAINT patientnote_pkey PRIMARY KEY ("PatNum");


--
-- Name: patientportalinvite patientportalinvite_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.patientportalinvite
    ADD CONSTRAINT patientportalinvite_pkey PRIMARY KEY ("PatientPortalInviteNum");


--
-- Name: patientrace patientrace_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.patientrace
    ADD CONSTRAINT patientrace_pkey PRIMARY KEY ("PatientRaceNum");


--
-- Name: patplan patplan_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.patplan
    ADD CONSTRAINT patplan_pkey PRIMARY KEY ("PatPlanNum");


--
-- Name: patrestriction patrestriction_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.patrestriction
    ADD CONSTRAINT patrestriction_pkey PRIMARY KEY ("PatRestrictionNum");


--
-- Name: payconnectresponseweb payconnectresponseweb_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.payconnectresponseweb
    ADD CONSTRAINT payconnectresponseweb_pkey PRIMARY KEY ("PayConnectResponseWebNum");


--
-- Name: payment payment_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.payment
    ADD CONSTRAINT payment_pkey PRIMARY KEY ("PayNum");


--
-- Name: payperiod payperiod_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.payperiod
    ADD CONSTRAINT payperiod_pkey PRIMARY KEY ("PayPeriodNum");


--
-- Name: payplan payplan_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.payplan
    ADD CONSTRAINT payplan_pkey PRIMARY KEY ("PayPlanNum");


--
-- Name: payplancharge payplancharge_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.payplancharge
    ADD CONSTRAINT payplancharge_pkey PRIMARY KEY ("PayPlanChargeNum");


--
-- Name: payplanlink payplanlink_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.payplanlink
    ADD CONSTRAINT payplanlink_pkey PRIMARY KEY ("PayPlanLinkNum");


--
-- Name: payplantemplate payplantemplate_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.payplantemplate
    ADD CONSTRAINT payplantemplate_pkey PRIMARY KEY ("PayPlanTemplateNum");


--
-- Name: paysplit paysplit_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.paysplit
    ADD CONSTRAINT paysplit_pkey PRIMARY KEY ("SplitNum");


--
-- Name: payterminal payterminal_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.payterminal
    ADD CONSTRAINT payterminal_pkey PRIMARY KEY ("PayTerminalNum");


--
-- Name: pearlrequest pearlrequest_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.pearlrequest
    ADD CONSTRAINT pearlrequest_pkey PRIMARY KEY ("PearlRequestNum");


--
-- Name: perioexam perioexam_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.perioexam
    ADD CONSTRAINT perioexam_pkey PRIMARY KEY ("PerioExamNum");


--
-- Name: periomeasure periomeasure_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.periomeasure
    ADD CONSTRAINT periomeasure_pkey PRIMARY KEY ("PerioMeasureNum");


--
-- Name: pharmacy pharmacy_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.pharmacy
    ADD CONSTRAINT pharmacy_pkey PRIMARY KEY ("PharmacyNum");


--
-- Name: pharmclinic pharmclinic_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.pharmclinic
    ADD CONSTRAINT pharmclinic_pkey PRIMARY KEY ("PharmClinicNum");


--
-- Name: phonenumber phonenumber_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.phonenumber
    ADD CONSTRAINT phonenumber_pkey PRIMARY KEY ("PhoneNumberNum");


--
-- Name: popup popup_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.popup
    ADD CONSTRAINT popup_pkey PRIMARY KEY ("PopupNum");


--
-- Name: preference preference_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.preference
    ADD CONSTRAINT preference_pkey PRIMARY KEY ("PrefNum");


--
-- Name: printer printer_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.printer
    ADD CONSTRAINT printer_pkey PRIMARY KEY ("PrinterNum");


--
-- Name: procapptcolor procapptcolor_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.procapptcolor
    ADD CONSTRAINT procapptcolor_pkey PRIMARY KEY ("ProcApptColorNum");


--
-- Name: procbutton procbutton_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.procbutton
    ADD CONSTRAINT procbutton_pkey PRIMARY KEY ("ProcButtonNum");


--
-- Name: procbuttonitem procbuttonitem_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.procbuttonitem
    ADD CONSTRAINT procbuttonitem_pkey PRIMARY KEY ("ProcButtonItemNum");


--
-- Name: procbuttonquick procbuttonquick_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.procbuttonquick
    ADD CONSTRAINT procbuttonquick_pkey PRIMARY KEY ("ProcButtonQuickNum");


--
-- Name: proccodenote proccodenote_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.proccodenote
    ADD CONSTRAINT proccodenote_pkey PRIMARY KEY ("ProcCodeNoteNum");


--
-- Name: procedurecode procedurecode_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.procedurecode
    ADD CONSTRAINT procedurecode_pkey PRIMARY KEY ("CodeNum");


--
-- Name: procedurelog procedurelog_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.procedurelog
    ADD CONSTRAINT procedurelog_pkey PRIMARY KEY ("ProcNum");


--
-- Name: procgroupitem procgroupitem_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.procgroupitem
    ADD CONSTRAINT procgroupitem_pkey PRIMARY KEY ("ProcGroupItemNum");


--
-- Name: procmultivisit procmultivisit_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.procmultivisit
    ADD CONSTRAINT procmultivisit_pkey PRIMARY KEY ("ProcMultiVisitNum");


--
-- Name: procnote procnote_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.procnote
    ADD CONSTRAINT procnote_pkey PRIMARY KEY ("ProcNoteNum");


--
-- Name: proctp proctp_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.proctp
    ADD CONSTRAINT proctp_pkey PRIMARY KEY ("ProcTPNum");


--
-- Name: program program_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.program
    ADD CONSTRAINT program_pkey PRIMARY KEY ("ProgramNum");


--
-- Name: programproperty programproperty_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.programproperty
    ADD CONSTRAINT programproperty_pkey PRIMARY KEY ("ProgramPropertyNum");


--
-- Name: promotion promotion_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.promotion
    ADD CONSTRAINT promotion_pkey PRIMARY KEY ("PromotionNum");


--
-- Name: promotionlog promotionlog_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.promotionlog
    ADD CONSTRAINT promotionlog_pkey PRIMARY KEY ("PromotionLogNum");


--
-- Name: provider provider_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.provider
    ADD CONSTRAINT provider_pkey PRIMARY KEY ("ProvNum");


--
-- Name: providerclinic providerclinic_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.providerclinic
    ADD CONSTRAINT providerclinic_pkey PRIMARY KEY ("ProviderClinicNum");


--
-- Name: providercliniclink providercliniclink_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.providercliniclink
    ADD CONSTRAINT providercliniclink_pkey PRIMARY KEY ("ProviderClinicLinkNum");


--
-- Name: providererx providererx_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.providererx
    ADD CONSTRAINT providererx_pkey PRIMARY KEY ("ProviderErxNum");


--
-- Name: providerident providerident_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.providerident
    ADD CONSTRAINT providerident_pkey PRIMARY KEY ("ProviderIdentNum");


--
-- Name: question question_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.question
    ADD CONSTRAINT question_pkey PRIMARY KEY ("QuestionNum");


--
-- Name: quickpastecat quickpastecat_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.quickpastecat
    ADD CONSTRAINT quickpastecat_pkey PRIMARY KEY ("QuickPasteCatNum");


--
-- Name: quickpastenote quickpastenote_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.quickpastenote
    ADD CONSTRAINT quickpastenote_pkey PRIMARY KEY ("QuickPasteNoteNum");


--
-- Name: reactivation reactivation_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.reactivation
    ADD CONSTRAINT reactivation_pkey PRIMARY KEY ("ReactivationNum");


--
-- Name: recall recall_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.recall
    ADD CONSTRAINT recall_pkey PRIMARY KEY ("RecallNum");


--
-- Name: recalltrigger recalltrigger_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.recalltrigger
    ADD CONSTRAINT recalltrigger_pkey PRIMARY KEY ("RecallTriggerNum");


--
-- Name: recalltype recalltype_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.recalltype
    ADD CONSTRAINT recalltype_pkey PRIMARY KEY ("RecallTypeNum");


--
-- Name: reconcile reconcile_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.reconcile
    ADD CONSTRAINT reconcile_pkey PRIMARY KEY ("ReconcileNum");


--
-- Name: recurringcharge recurringcharge_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.recurringcharge
    ADD CONSTRAINT recurringcharge_pkey PRIMARY KEY ("RecurringChargeNum");


--
-- Name: refattach refattach_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.refattach
    ADD CONSTRAINT refattach_pkey PRIMARY KEY ("RefAttachNum");


--
-- Name: referral referral_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.referral
    ADD CONSTRAINT referral_pkey PRIMARY KEY ("ReferralNum");


--
-- Name: referralcliniclink referralcliniclink_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.referralcliniclink
    ADD CONSTRAINT referralcliniclink_pkey PRIMARY KEY ("ReferralClinicLinkNum");


--
-- Name: registrationkey registrationkey_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.registrationkey
    ADD CONSTRAINT registrationkey_pkey PRIMARY KEY ("RegistrationKeyNum");


--
-- Name: reminderrule reminderrule_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.reminderrule
    ADD CONSTRAINT reminderrule_pkey PRIMARY KEY ("ReminderRuleNum");


--
-- Name: repeatcharge repeatcharge_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.repeatcharge
    ADD CONSTRAINT repeatcharge_pkey PRIMARY KEY ("RepeatChargeNum");


--
-- Name: replicationserver replicationserver_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.replicationserver
    ADD CONSTRAINT replicationserver_pkey PRIMARY KEY ("ReplicationServerNum");


--
-- Name: reqneeded reqneeded_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.reqneeded
    ADD CONSTRAINT reqneeded_pkey PRIMARY KEY ("ReqNeededNum");


--
-- Name: reqstudent reqstudent_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.reqstudent
    ADD CONSTRAINT reqstudent_pkey PRIMARY KEY ("ReqStudentNum");


--
-- Name: requiredfield requiredfield_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.requiredfield
    ADD CONSTRAINT requiredfield_pkey PRIMARY KEY ("RequiredFieldNum");


--
-- Name: requiredfieldcondition requiredfieldcondition_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.requiredfieldcondition
    ADD CONSTRAINT requiredfieldcondition_pkey PRIMARY KEY ("RequiredFieldConditionNum");


--
-- Name: reseller reseller_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.reseller
    ADD CONSTRAINT reseller_pkey PRIMARY KEY ("ResellerNum");


--
-- Name: resellerservice resellerservice_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.resellerservice
    ADD CONSTRAINT resellerservice_pkey PRIMARY KEY ("ResellerServiceNum");


--
-- Name: rxalert rxalert_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.rxalert
    ADD CONSTRAINT rxalert_pkey PRIMARY KEY ("RxAlertNum");


--
-- Name: rxdef rxdef_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.rxdef
    ADD CONSTRAINT rxdef_pkey PRIMARY KEY ("RxDefNum");


--
-- Name: rxnorm rxnorm_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.rxnorm
    ADD CONSTRAINT rxnorm_pkey PRIMARY KEY ("RxNormNum");


--
-- Name: rxpat rxpat_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.rxpat
    ADD CONSTRAINT rxpat_pkey PRIMARY KEY ("RxNum");


--
-- Name: schedule schedule_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.schedule
    ADD CONSTRAINT schedule_pkey PRIMARY KEY ("ScheduleNum");


--
-- Name: scheduledprocess scheduledprocess_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.scheduledprocess
    ADD CONSTRAINT scheduledprocess_pkey PRIMARY KEY ("ScheduledProcessNum");


--
-- Name: scheduleop scheduleop_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.scheduleop
    ADD CONSTRAINT scheduleop_pkey PRIMARY KEY ("ScheduleOpNum");


--
-- Name: schoolclass schoolclass_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.schoolclass
    ADD CONSTRAINT schoolclass_pkey PRIMARY KEY ("SchoolClassNum");


--
-- Name: schoolcourse schoolcourse_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.schoolcourse
    ADD CONSTRAINT schoolcourse_pkey PRIMARY KEY ("SchoolCourseNum");


--
-- Name: screen screen_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.screen
    ADD CONSTRAINT screen_pkey PRIMARY KEY ("ScreenNum");


--
-- Name: screengroup screengroup_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.screengroup
    ADD CONSTRAINT screengroup_pkey PRIMARY KEY ("ScreenGroupNum");


--
-- Name: screenpat screenpat_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.screenpat
    ADD CONSTRAINT screenpat_pkey PRIMARY KEY ("ScreenPatNum");


--
-- Name: securitylog securitylog_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.securitylog
    ADD CONSTRAINT securitylog_pkey PRIMARY KEY ("SecurityLogNum");


--
-- Name: securityloghash securityloghash_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.securityloghash
    ADD CONSTRAINT securityloghash_pkey PRIMARY KEY ("SecurityLogHashNum");


--
-- Name: sessiontoken sessiontoken_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.sessiontoken
    ADD CONSTRAINT sessiontoken_pkey PRIMARY KEY ("SessionTokenNum");


--
-- Name: sheet sheet_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.sheet
    ADD CONSTRAINT sheet_pkey PRIMARY KEY ("SheetNum");


--
-- Name: sheetdef sheetdef_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.sheetdef
    ADD CONSTRAINT sheetdef_pkey PRIMARY KEY ("SheetDefNum");


--
-- Name: sheetfield sheetfield_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.sheetfield
    ADD CONSTRAINT sheetfield_pkey PRIMARY KEY ("SheetFieldNum");


--
-- Name: sheetfielddef sheetfielddef_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.sheetfielddef
    ADD CONSTRAINT sheetfielddef_pkey PRIMARY KEY ("SheetFieldDefNum");


--
-- Name: sigbutdef sigbutdef_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.sigbutdef
    ADD CONSTRAINT sigbutdef_pkey PRIMARY KEY ("SigButDefNum");


--
-- Name: sigelementdef sigelementdef_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.sigelementdef
    ADD CONSTRAINT sigelementdef_pkey PRIMARY KEY ("SigElementDefNum");


--
-- Name: sigmessage sigmessage_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.sigmessage
    ADD CONSTRAINT sigmessage_pkey PRIMARY KEY ("SigMessageNum");


--
-- Name: signalod signalod_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.signalod
    ADD CONSTRAINT signalod_pkey PRIMARY KEY ("SignalNum");


--
-- Name: site site_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.site
    ADD CONSTRAINT site_pkey PRIMARY KEY ("SiteNum");


--
-- Name: smsblockphone smsblockphone_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.smsblockphone
    ADD CONSTRAINT smsblockphone_pkey PRIMARY KEY ("SmsBlockPhoneNum");


--
-- Name: smsfrommobile smsfrommobile_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.smsfrommobile
    ADD CONSTRAINT smsfrommobile_pkey PRIMARY KEY ("SmsFromMobileNum");


--
-- Name: smsphone smsphone_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.smsphone
    ADD CONSTRAINT smsphone_pkey PRIMARY KEY ("SmsPhoneNum");


--
-- Name: smstomobile smstomobile_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.smstomobile
    ADD CONSTRAINT smstomobile_pkey PRIMARY KEY ("SmsToMobileNum");


--
-- Name: snomed snomed_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.snomed
    ADD CONSTRAINT snomed_pkey PRIMARY KEY ("SnomedNum");


--
-- Name: sop sop_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.sop
    ADD CONSTRAINT sop_pkey PRIMARY KEY ("SopNum");


--
-- Name: stateabbr stateabbr_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.stateabbr
    ADD CONSTRAINT stateabbr_pkey PRIMARY KEY ("StateAbbrNum");


--
-- Name: statement statement_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.statement
    ADD CONSTRAINT statement_pkey PRIMARY KEY ("StatementNum");


--
-- Name: statementprod statementprod_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.statementprod
    ADD CONSTRAINT statementprod_pkey PRIMARY KEY ("StatementProdNum");


--
-- Name: stmtlink stmtlink_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.stmtlink
    ADD CONSTRAINT stmtlink_pkey PRIMARY KEY ("StmtLinkNum");


--
-- Name: substitutionlink substitutionlink_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.substitutionlink
    ADD CONSTRAINT substitutionlink_pkey PRIMARY KEY ("SubstitutionLinkNum");


--
-- Name: supplier supplier_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.supplier
    ADD CONSTRAINT supplier_pkey PRIMARY KEY ("SupplierNum");


--
-- Name: supply supply_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.supply
    ADD CONSTRAINT supply_pkey PRIMARY KEY ("SupplyNum");


--
-- Name: supplyneeded supplyneeded_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.supplyneeded
    ADD CONSTRAINT supplyneeded_pkey PRIMARY KEY ("SupplyNeededNum");


--
-- Name: supplyorder supplyorder_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.supplyorder
    ADD CONSTRAINT supplyorder_pkey PRIMARY KEY ("SupplyOrderNum");


--
-- Name: supplyorderitem supplyorderitem_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.supplyorderitem
    ADD CONSTRAINT supplyorderitem_pkey PRIMARY KEY ("SupplyOrderItemNum");


--
-- Name: task task_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.task
    ADD CONSTRAINT task_pkey PRIMARY KEY ("TaskNum");


--
-- Name: taskancestor taskancestor_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.taskancestor
    ADD CONSTRAINT taskancestor_pkey PRIMARY KEY ("TaskAncestorNum");


--
-- Name: taskattachment taskattachment_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.taskattachment
    ADD CONSTRAINT taskattachment_pkey PRIMARY KEY ("TaskAttachmentNum");


--
-- Name: taskhist taskhist_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.taskhist
    ADD CONSTRAINT taskhist_pkey PRIMARY KEY ("TaskHistNum");


--
-- Name: tasklist tasklist_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.tasklist
    ADD CONSTRAINT tasklist_pkey PRIMARY KEY ("TaskListNum");


--
-- Name: tasknote tasknote_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.tasknote
    ADD CONSTRAINT tasknote_pkey PRIMARY KEY ("TaskNoteNum");


--
-- Name: tasksubscription tasksubscription_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.tasksubscription
    ADD CONSTRAINT tasksubscription_pkey PRIMARY KEY ("TaskSubscriptionNum");


--
-- Name: taskunread taskunread_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.taskunread
    ADD CONSTRAINT taskunread_pkey PRIMARY KEY ("TaskUnreadNum");


--
-- Name: terminalactive terminalactive_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.terminalactive
    ADD CONSTRAINT terminalactive_pkey PRIMARY KEY ("TerminalActiveNum");


--
-- Name: timeadjust timeadjust_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.timeadjust
    ADD CONSTRAINT timeadjust_pkey PRIMARY KEY ("TimeAdjustNum");


--
-- Name: timecardrule timecardrule_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.timecardrule
    ADD CONSTRAINT timecardrule_pkey PRIMARY KEY ("TimeCardRuleNum");


--
-- Name: toolbutitem toolbutitem_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.toolbutitem
    ADD CONSTRAINT toolbutitem_pkey PRIMARY KEY ("ToolButItemNum");


--
-- Name: toothgridcell toothgridcell_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.toothgridcell
    ADD CONSTRAINT toothgridcell_pkey PRIMARY KEY ("ToothGridCellNum");


--
-- Name: toothgridcol toothgridcol_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.toothgridcol
    ADD CONSTRAINT toothgridcol_pkey PRIMARY KEY ("ToothGridColNum");


--
-- Name: toothinitial toothinitial_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.toothinitial
    ADD CONSTRAINT toothinitial_pkey PRIMARY KEY ("ToothInitialNum");


--
-- Name: transaction transaction_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.transaction
    ADD CONSTRAINT transaction_pkey PRIMARY KEY ("TransactionNum");


--
-- Name: transactioninvoice transactioninvoice_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.transactioninvoice
    ADD CONSTRAINT transactioninvoice_pkey PRIMARY KEY ("TransactionInvoiceNum");


--
-- Name: treatplan treatplan_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.treatplan
    ADD CONSTRAINT treatplan_pkey PRIMARY KEY ("TreatPlanNum");


--
-- Name: treatplanattach treatplanattach_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.treatplanattach
    ADD CONSTRAINT treatplanattach_pkey PRIMARY KEY ("TreatPlanAttachNum");


--
-- Name: treatplanparam treatplanparam_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.treatplanparam
    ADD CONSTRAINT treatplanparam_pkey PRIMARY KEY ("TreatPlanParamNum");


--
-- Name: tsitranslog tsitranslog_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.tsitranslog
    ADD CONSTRAINT tsitranslog_pkey PRIMARY KEY ("TsiTransLogNum");


--
-- Name: ucum ucum_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.ucum
    ADD CONSTRAINT ucum_pkey PRIMARY KEY ("UcumNum");


--
-- Name: updatehistory updatehistory_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.updatehistory
    ADD CONSTRAINT updatehistory_pkey PRIMARY KEY ("UpdateHistoryNum");


--
-- Name: userclinic userclinic_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.userclinic
    ADD CONSTRAINT userclinic_pkey PRIMARY KEY ("UserClinicNum");


--
-- Name: usergroup usergroup_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.usergroup
    ADD CONSTRAINT usergroup_pkey PRIMARY KEY ("UserGroupNum");


--
-- Name: usergroupattach usergroupattach_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.usergroupattach
    ADD CONSTRAINT usergroupattach_pkey PRIMARY KEY ("UserGroupAttachNum");


--
-- Name: userod userod_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.userod
    ADD CONSTRAINT userod_pkey PRIMARY KEY ("UserNum");


--
-- Name: userodapptview userodapptview_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.userodapptview
    ADD CONSTRAINT userodapptview_pkey PRIMARY KEY ("UserodApptViewNum");


--
-- Name: userodpref userodpref_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.userodpref
    ADD CONSTRAINT userodpref_pkey PRIMARY KEY ("UserOdPrefNum");


--
-- Name: userquery userquery_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.userquery
    ADD CONSTRAINT userquery_pkey PRIMARY KEY ("QueryNum");


--
-- Name: userweb userweb_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.userweb
    ADD CONSTRAINT userweb_pkey PRIMARY KEY ("UserWebNum");


--
-- Name: utm utm_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.utm
    ADD CONSTRAINT utm_pkey PRIMARY KEY ("UtmNum");


--
-- Name: vaccineobs vaccineobs_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.vaccineobs
    ADD CONSTRAINT vaccineobs_pkey PRIMARY KEY ("VaccineObsNum");


--
-- Name: vaccinepat vaccinepat_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.vaccinepat
    ADD CONSTRAINT vaccinepat_pkey PRIMARY KEY ("VaccinePatNum");


--
-- Name: vitalsign vitalsign_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.vitalsign
    ADD CONSTRAINT vitalsign_pkey PRIMARY KEY ("VitalsignNum");


--
-- Name: webschedcarrierrule webschedcarrierrule_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.webschedcarrierrule
    ADD CONSTRAINT webschedcarrierrule_pkey PRIMARY KEY ("WebSchedCarrierRuleNum");


--
-- Name: webschedrecall webschedrecall_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.webschedrecall
    ADD CONSTRAINT webschedrecall_pkey PRIMARY KEY ("WebSchedRecallNum");


--
-- Name: wikilistheaderwidth wikilistheaderwidth_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.wikilistheaderwidth
    ADD CONSTRAINT wikilistheaderwidth_pkey PRIMARY KEY ("WikiListHeaderWidthNum");


--
-- Name: wikilisthist wikilisthist_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.wikilisthist
    ADD CONSTRAINT wikilisthist_pkey PRIMARY KEY ("WikiListHistNum");


--
-- Name: wikipage wikipage_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.wikipage
    ADD CONSTRAINT wikipage_pkey PRIMARY KEY ("WikiPageNum");


--
-- Name: wikipagehist wikipagehist_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.wikipagehist
    ADD CONSTRAINT wikipagehist_pkey PRIMARY KEY ("WikiPageNum");


--
-- Name: xchargetransaction xchargetransaction_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.xchargetransaction
    ADD CONSTRAINT xchargetransaction_pkey PRIMARY KEY ("XChargeTransactionNum");


--
-- Name: xwebresponse xwebresponse_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.xwebresponse
    ADD CONSTRAINT xwebresponse_pkey PRIMARY KEY ("XWebResponseNum");


--
-- Name: zipcode zipcode_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.zipcode
    ADD CONSTRAINT zipcode_pkey PRIMARY KEY ("ZipCodeNum");


--
-- Name: idx_etl_load_status_load_status; Type: INDEX; Schema: raw; Owner: -
--

CREATE INDEX idx_etl_load_status_load_status ON raw.etl_load_status USING btree (load_status);


--
-- Name: idx_etl_load_status_primary_value; Type: INDEX; Schema: raw; Owner: -
--

CREATE INDEX idx_etl_load_status_primary_value ON raw.etl_load_status USING btree (last_primary_value);


--
-- Name: idx_etl_load_status_table_name; Type: INDEX; Schema: raw; Owner: -
--

CREATE INDEX idx_etl_load_status_table_name ON raw.etl_load_status USING btree (table_name);


--
-- Name: idx_etl_transform_status_primary_value; Type: INDEX; Schema: raw; Owner: -
--

CREATE INDEX idx_etl_transform_status_primary_value ON raw.etl_transform_status USING btree (last_primary_value);


--
-- Name: idx_etl_transform_status_table_name; Type: INDEX; Schema: raw; Owner: -
--

CREATE INDEX idx_etl_transform_status_table_name ON raw.etl_transform_status USING btree (table_name);


--
-- Name: idx_etl_transform_status_transform_status; Type: INDEX; Schema: raw; Owner: -
--

CREATE INDEX idx_etl_transform_status_transform_status ON raw.etl_transform_status USING btree (transform_status);


--
-- Name: idx_pipeline_metrics_pipeline_id; Type: INDEX; Schema: raw; Owner: -
--

CREATE INDEX idx_pipeline_metrics_pipeline_id ON raw.etl_pipeline_metrics USING btree (pipeline_id);


--
-- Name: idx_pipeline_metrics_start_time; Type: INDEX; Schema: raw; Owner: -
--

CREATE INDEX idx_pipeline_metrics_start_time ON raw.etl_pipeline_metrics USING btree (start_time);


--
-- Name: idx_table_metrics_pipeline_id; Type: INDEX; Schema: raw; Owner: -
--

CREATE INDEX idx_table_metrics_pipeline_id ON raw.etl_table_metrics USING btree (pipeline_id);


--
-- Name: idx_table_metrics_table_name; Type: INDEX; Schema: raw; Owner: -
--

CREATE INDEX idx_table_metrics_table_name ON raw.etl_table_metrics USING btree (table_name);


--
-- PostgreSQL database dump complete
--


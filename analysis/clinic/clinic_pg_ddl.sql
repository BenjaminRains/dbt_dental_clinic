-- public.clinic definition

-- Drop table

-- DROP TABLE public.clinic;

CREATE TABLE public.clinic (
	"ClinicNum" serial4 NOT NULL,
	"Description" varchar(255) NULL,
	"Address" varchar(255) NULL,
	"Address2" varchar(255) NULL,
	"City" varchar(255) NULL,
	"State" varchar(255) NULL,
	"Zip" varchar(255) NULL,
	"Phone" varchar(255) NULL,
	"BankNumber" varchar(255) NULL,
	"DefaultPlaceService" int2 NULL,
	"InsBillingProv" int8 NULL,
	"Fax" varchar(50) NULL,
	"EmailAddressNum" int8 NULL,
	"DefaultProv" int8 NULL,
	"SmsContractDate" timestamp NULL,
	"SmsMonthlyLimit" float8 NULL,
	"IsMedicalOnly" int2 NULL,
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
	"UseBillAddrOnClaims" int2 NULL,
	"Region" int8 NULL,
	"ItemOrder" int4 NULL,
	"IsInsVerifyExcluded" int2 NULL,
	"Abbr" varchar(255) NULL,
	"MedLabAccountNum" varchar(16) NULL,
	"IsConfirmEnabled" int2 NULL,
	"IsConfirmDefault" int2 NULL,
	"IsNewPatApptExcluded" int2 NULL,
	"IsHidden" int2 NULL,
	"ExternalID" int8 NULL,
	"SchedNote" varchar(255) NULL,
	"HasProcOnRx" int2 NULL,
	"TimeZone" varchar(75) NULL,
	"EmailAliasOverride" varchar(255) NULL,
	CONSTRAINT clinic_pkey PRIMARY KEY ("ClinicNum")
);
CREATE INDEX "clinic_DefaultProv" ON public.clinic USING btree ("DefaultProv");
CREATE INDEX "clinic_EmailAddressNum" ON public.clinic USING btree ("EmailAddressNum");
CREATE INDEX "clinic_ExternalID" ON public.clinic USING btree ("ExternalID");
CREATE INDEX "clinic_InsBillingProv" ON public.clinic USING btree ("InsBillingProv");
CREATE INDEX "clinic_Region" ON public.clinic USING btree ("Region");
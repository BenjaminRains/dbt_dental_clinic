-- public.securitylog definition

-- Drop table

-- DROP TABLE public.securitylog;

CREATE TABLE public.securitylog (
	"SecurityLogNum" serial4 NOT NULL,
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
CREATE INDEX "securitylog_DefNum" ON public.securitylog USING btree ("DefNum");
CREATE INDEX "securitylog_DefNumError" ON public.securitylog USING btree ("DefNumError");
CREATE INDEX "securitylog_FKey" ON public.securitylog USING btree ("FKey");
CREATE INDEX "securitylog_LogDateTime" ON public.securitylog USING brin ("LogDateTime");
CREATE INDEX "securitylog_PatNum" ON public.securitylog USING btree ("PatNum");
CREATE INDEX "securitylog_PermType" ON public.securitylog USING btree ("PermType");
CREATE INDEX "securitylog_UserNum" ON public.securitylog USING btree ("UserNum");
-- Table: public.patient

-- DROP TABLE IF EXISTS public.patient;

CREATE TABLE IF NOT EXISTS public.patient
(
    "PatNum" integer NOT NULL DEFAULT nextval('"patient_PatNum_seq"'::regclass),
    "LName" character varying(100) COLLATE pg_catalog."default",
    "FName" character varying(100) COLLATE pg_catalog."default",
    "MiddleI" character varying(100) COLLATE pg_catalog."default",
    "Preferred" character varying(100) COLLATE pg_catalog."default",
    "PatStatus" smallint DEFAULT 0,
    "Gender" smallint DEFAULT 0,
    "Position" smallint DEFAULT 0,
    "Birthdate" date,
    "SSN" character varying(100) COLLATE pg_catalog."default",
    "Address" character varying(100) COLLATE pg_catalog."default",
    "Address2" character varying(100) COLLATE pg_catalog."default",
    "City" character varying(100) COLLATE pg_catalog."default",
    "State" character varying(100) COLLATE pg_catalog."default",
    "Zip" character varying(100) COLLATE pg_catalog."default",
    "HmPhone" character varying(30) COLLATE pg_catalog."default",
    "WkPhone" character varying(30) COLLATE pg_catalog."default",
    "WirelessPhone" character varying(30) COLLATE pg_catalog."default",
    "Guarantor" bigint,
    "CreditType" character varying(1) COLLATE pg_catalog."default",
    "Email" character varying(100) COLLATE pg_catalog."default",
    "Salutation" character varying(100) COLLATE pg_catalog."default",
    "EstBalance" double precision DEFAULT 0,
    "PriProv" bigint,
    "SecProv" bigint,
    "FeeSched" bigint,
    "BillingType" bigint,
    "ImageFolder" character varying(100) COLLATE pg_catalog."default",
    "AddrNote" text COLLATE pg_catalog."default",
    "FamFinUrgNote" text COLLATE pg_catalog."default",
    "MedUrgNote" character varying(255) COLLATE pg_catalog."default",
    "ApptModNote" character varying(255) COLLATE pg_catalog."default",
    "StudentStatus" character varying(1) COLLATE pg_catalog."default",
    "SchoolName" character varying(255) COLLATE pg_catalog."default",
    "ChartNumber" character varying(100) COLLATE pg_catalog."default",
    "MedicaidID" character varying(20) COLLATE pg_catalog."default",
    "Bal_0_30" double precision DEFAULT 0,
    "Bal_31_60" double precision DEFAULT 0,
    "Bal_61_90" double precision DEFAULT 0,
    "BalOver90" double precision DEFAULT 0,
    "InsEst" double precision DEFAULT 0,
    "BalTotal" double precision DEFAULT 0,
    "EmployerNum" bigint,
    "EmploymentNote" character varying(255) COLLATE pg_catalog."default",
    "County" character varying(255) COLLATE pg_catalog."default",
    "GradeLevel" smallint DEFAULT 0,
    "Urgency" smallint DEFAULT 0,
    "DateFirstVisit" date,
    "ClinicNum" bigint,
    "HasIns" character varying(255) COLLATE pg_catalog."default",
    "TrophyFolder" character varying(255) COLLATE pg_catalog."default",
    "PlannedIsDone" smallint,
    "Premed" smallint,
    "Ward" character varying(255) COLLATE pg_catalog."default",
    "PreferConfirmMethod" smallint,
    "PreferContactMethod" smallint,
    "PreferRecallMethod" smallint,
    "SchedBeforeTime" time without time zone,
    "SchedAfterTime" time without time zone,
    "SchedDayOfWeek" smallint,
    "Language" character varying(100) COLLATE pg_catalog."default",
    "AdmitDate" date,
    "Title" character varying(15) COLLATE pg_catalog."default",
    "PayPlanDue" double precision,
    "SiteNum" bigint,
    "DateTStamp" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    "ResponsParty" bigint,
    "CanadianEligibilityCode" smallint,
    "AskToArriveEarly" integer,
    "PreferContactConfidential" smallint,
    "SuperFamily" bigint,
    "TxtMsgOk" smallint,
    "SmokingSnoMed" character varying(32) COLLATE pg_catalog."default",
    "Country" character varying(255) COLLATE pg_catalog."default",
    "DateTimeDeceased" timestamp without time zone,
    "BillingCycleDay" integer DEFAULT 1,
    "SecUserNumEntry" bigint,
    "SecDateEntry" date,
    "HasSuperBilling" smallint,
    "PatNumCloneFrom" bigint,
    "DiscountPlanNum" bigint,
    "HasSignedTil" smallint,
    "ShortCodeOptIn" smallint,
    "SecurityHash" character varying(255) COLLATE pg_catalog."default",
    CONSTRAINT patient_pkey PRIMARY KEY ("PatNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.patient
    OWNER to postgres;
-- Index: patient_BirthdateStatus

-- DROP INDEX IF EXISTS public."patient_BirthdateStatus";

CREATE INDEX IF NOT EXISTS "patient_BirthdateStatus"
    ON public.patient USING btree
    ("Birthdate" ASC NULLS LAST, "PatStatus" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: patient_ChartNumber

-- DROP INDEX IF EXISTS public."patient_ChartNumber";

CREATE INDEX IF NOT EXISTS "patient_ChartNumber"
    ON public.patient USING btree
    ("ChartNumber" COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: patient_ClinicPatStatus

-- DROP INDEX IF EXISTS public."patient_ClinicPatStatus";

CREATE INDEX IF NOT EXISTS "patient_ClinicPatStatus"
    ON public.patient USING btree
    ("ClinicNum" ASC NULLS LAST, "PatStatus" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: patient_DiscountPlanNum

-- DROP INDEX IF EXISTS public."patient_DiscountPlanNum";

CREATE INDEX IF NOT EXISTS "patient_DiscountPlanNum"
    ON public.patient USING btree
    ("DiscountPlanNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: patient_Email

-- DROP INDEX IF EXISTS public."patient_Email";

CREATE INDEX IF NOT EXISTS "patient_Email"
    ON public.patient USING btree
    ("Email" COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: patient_FeeSched

-- DROP INDEX IF EXISTS public."patient_FeeSched";

CREATE INDEX IF NOT EXISTS "patient_FeeSched"
    ON public.patient USING btree
    ("FeeSched" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: patient_HmPhone

-- DROP INDEX IF EXISTS public."patient_HmPhone";

CREATE INDEX IF NOT EXISTS "patient_HmPhone"
    ON public.patient USING btree
    ("HmPhone" COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: patient_PatNumCloneFrom

-- DROP INDEX IF EXISTS public."patient_PatNumCloneFrom";

CREATE INDEX IF NOT EXISTS "patient_PatNumCloneFrom"
    ON public.patient USING btree
    ("PatNumCloneFrom" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: patient_PatStatus

-- DROP INDEX IF EXISTS public."patient_PatStatus";

CREATE INDEX IF NOT EXISTS "patient_PatStatus"
    ON public.patient USING btree
    ("PatStatus" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: patient_PriProv

-- DROP INDEX IF EXISTS public."patient_PriProv";

CREATE INDEX IF NOT EXISTS "patient_PriProv"
    ON public.patient USING btree
    ("PriProv" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: patient_ResponsParty

-- DROP INDEX IF EXISTS public."patient_ResponsParty";

CREATE INDEX IF NOT EXISTS "patient_ResponsParty"
    ON public.patient USING btree
    ("ResponsParty" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: patient_SecDateEntry

-- DROP INDEX IF EXISTS public."patient_SecDateEntry";

CREATE INDEX IF NOT EXISTS "patient_SecDateEntry"
    ON public.patient USING btree
    ("SecDateEntry" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: patient_SecProv

-- DROP INDEX IF EXISTS public."patient_SecProv";

CREATE INDEX IF NOT EXISTS "patient_SecProv"
    ON public.patient USING btree
    ("SecProv" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: patient_SecUserNumEntry

-- DROP INDEX IF EXISTS public."patient_SecUserNumEntry";

CREATE INDEX IF NOT EXISTS "patient_SecUserNumEntry"
    ON public.patient USING btree
    ("SecUserNumEntry" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: patient_SiteNum

-- DROP INDEX IF EXISTS public."patient_SiteNum";

CREATE INDEX IF NOT EXISTS "patient_SiteNum"
    ON public.patient USING btree
    ("SiteNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: patient_SuperFamily

-- DROP INDEX IF EXISTS public."patient_SuperFamily";

CREATE INDEX IF NOT EXISTS "patient_SuperFamily"
    ON public.patient USING btree
    ("SuperFamily" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: patient_WirelessPhone

-- DROP INDEX IF EXISTS public."patient_WirelessPhone";

CREATE INDEX IF NOT EXISTS "patient_WirelessPhone"
    ON public.patient USING btree
    ("WirelessPhone" COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: patient_WkPhone

-- DROP INDEX IF EXISTS public."patient_WkPhone";

CREATE INDEX IF NOT EXISTS "patient_WkPhone"
    ON public.patient USING btree
    ("WkPhone" COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: patient_indexFName

-- DROP INDEX IF EXISTS public."patient_indexFName";

CREATE INDEX IF NOT EXISTS "patient_indexFName"
    ON public.patient USING btree
    ("FName" COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: patient_indexGuarantor

-- DROP INDEX IF EXISTS public."patient_indexGuarantor";

CREATE INDEX IF NOT EXISTS "patient_indexGuarantor"
    ON public.patient USING btree
    ("Guarantor" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: patient_indexLFName

-- DROP INDEX IF EXISTS public."patient_indexLFName";

CREATE INDEX IF NOT EXISTS "patient_indexLFName"
    ON public.patient USING btree
    ("LName" COLLATE pg_catalog."default" ASC NULLS LAST, "FName" COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: patient_indexLName

-- DROP INDEX IF EXISTS public."patient_indexLName";

CREATE INDEX IF NOT EXISTS "patient_indexLName"
    ON public.patient USING btree
    ("LName" COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Table: public.procedurelog

-- DROP TABLE IF EXISTS public.procedurelog;

CREATE TABLE IF NOT EXISTS public.procedurelog
(
    "ProcNum" integer NOT NULL DEFAULT nextval('"procedurelog_ProcNum_seq"'::regclass),
    "PatNum" bigint,
    "AptNum" bigint,
    "OldCode" character varying(15) COLLATE pg_catalog."default",
    "ProcDate" date,
    "ProcFee" double precision DEFAULT 0,
    "Surf" character varying(10) COLLATE pg_catalog."default",
    "ToothNum" character varying(2) COLLATE pg_catalog."default",
    "ToothRange" character varying(100) COLLATE pg_catalog."default",
    "Priority" bigint,
    "ProcStatus" smallint DEFAULT 0,
    "ProvNum" bigint,
    "Dx" bigint,
    "PlannedAptNum" bigint,
    "PlaceService" smallint DEFAULT 0,
    "Prosthesis" character varying(1) COLLATE pg_catalog."default",
    "DateOriginalProsth" date,
    "ClaimNote" character varying(80) COLLATE pg_catalog."default",
    "DateEntryC" date,
    "ClinicNum" bigint,
    "MedicalCode" character varying(15) COLLATE pg_catalog."default",
    "DiagnosticCode" character varying(255) COLLATE pg_catalog."default",
    "IsPrincDiag" smallint,
    "ProcNumLab" bigint,
    "BillingTypeOne" bigint,
    "BillingTypeTwo" bigint,
    "CodeNum" bigint,
    "CodeMod1" character varying(2) COLLATE pg_catalog."default",
    "CodeMod2" character varying(2) COLLATE pg_catalog."default",
    "CodeMod3" character varying(2) COLLATE pg_catalog."default",
    "CodeMod4" character varying(2) COLLATE pg_catalog."default",
    "RevCode" character varying(45) COLLATE pg_catalog."default",
    "UnitQty" integer,
    "BaseUnits" integer,
    "StartTime" integer,
    "StopTime" integer,
    "DateTP" date,
    "SiteNum" bigint,
    "HideGraphics" smallint,
    "CanadianTypeCodes" character varying(20) COLLATE pg_catalog."default",
    "ProcTime" time without time zone,
    "ProcTimeEnd" time without time zone,
    "DateTStamp" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    "Prognosis" bigint,
    "DrugUnit" smallint,
    "DrugQty" real,
    "UnitQtyType" smallint,
    "StatementNum" bigint,
    "IsLocked" smallint,
    "BillingNote" character varying(255) COLLATE pg_catalog."default",
    "RepeatChargeNum" bigint,
    "SnomedBodySite" character varying(255) COLLATE pg_catalog."default",
    "DiagnosticCode2" character varying(255) COLLATE pg_catalog."default",
    "DiagnosticCode3" character varying(255) COLLATE pg_catalog."default",
    "DiagnosticCode4" character varying(255) COLLATE pg_catalog."default",
    "ProvOrderOverride" bigint,
    "Discount" double precision,
    "IsDateProsthEst" smallint,
    "IcdVersion" smallint,
    "IsCpoe" smallint,
    "SecUserNumEntry" bigint,
    "SecDateEntry" timestamp without time zone,
    "DateComplete" date,
    "OrderingReferralNum" bigint,
    "TaxAmt" double precision,
    "Urgency" smallint,
    "DiscountPlanAmt" double precision,
    CONSTRAINT procedurelog_pkey PRIMARY KEY ("ProcNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.procedurelog
    OWNER to postgres;
-- Index: procedurelog_BillingTypeOne

-- DROP INDEX IF EXISTS public."procedurelog_BillingTypeOne";

CREATE INDEX IF NOT EXISTS "procedurelog_BillingTypeOne"
    ON public.procedurelog USING btree
    ("BillingTypeOne" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: procedurelog_BillingTypeTwo

-- DROP INDEX IF EXISTS public."procedurelog_BillingTypeTwo";

CREATE INDEX IF NOT EXISTS "procedurelog_BillingTypeTwo"
    ON public.procedurelog USING btree
    ("BillingTypeTwo" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: procedurelog_ClinicNum

-- DROP INDEX IF EXISTS public."procedurelog_ClinicNum";

CREATE INDEX IF NOT EXISTS "procedurelog_ClinicNum"
    ON public.procedurelog USING btree
    ("ClinicNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: procedurelog_CodeNum

-- DROP INDEX IF EXISTS public."procedurelog_CodeNum";

CREATE INDEX IF NOT EXISTS "procedurelog_CodeNum"
    ON public.procedurelog USING btree
    ("CodeNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: procedurelog_DateClinicStatus

-- DROP INDEX IF EXISTS public."procedurelog_DateClinicStatus";

CREATE INDEX IF NOT EXISTS "procedurelog_DateClinicStatus"
    ON public.procedurelog USING btree
    ("ProcDate" ASC NULLS LAST, "ClinicNum" ASC NULLS LAST, "ProcStatus" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: procedurelog_DateComplete

-- DROP INDEX IF EXISTS public."procedurelog_DateComplete";

CREATE INDEX IF NOT EXISTS "procedurelog_DateComplete"
    ON public.procedurelog USING btree
    ("DateComplete" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: procedurelog_DateTStampPN

-- DROP INDEX IF EXISTS public."procedurelog_DateTStampPN";

CREATE INDEX IF NOT EXISTS "procedurelog_DateTStampPN"
    ON public.procedurelog USING btree
    ("DateTStamp" ASC NULLS LAST, "PatNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: procedurelog_OrderingReferralNum

-- DROP INDEX IF EXISTS public."procedurelog_OrderingReferralNum";

CREATE INDEX IF NOT EXISTS "procedurelog_OrderingReferralNum"
    ON public.procedurelog USING btree
    ("OrderingReferralNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: procedurelog_PatStatusCodeDate

-- DROP INDEX IF EXISTS public."procedurelog_PatStatusCodeDate";

CREATE INDEX IF NOT EXISTS "procedurelog_PatStatusCodeDate"
    ON public.procedurelog USING btree
    ("PatNum" ASC NULLS LAST, "ProcStatus" ASC NULLS LAST, "CodeNum" ASC NULLS LAST, "ProcDate" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: procedurelog_PlannedAptNum

-- DROP INDEX IF EXISTS public."procedurelog_PlannedAptNum";

CREATE INDEX IF NOT EXISTS "procedurelog_PlannedAptNum"
    ON public.procedurelog USING btree
    ("PlannedAptNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: procedurelog_Priority

-- DROP INDEX IF EXISTS public."procedurelog_Priority";

CREATE INDEX IF NOT EXISTS "procedurelog_Priority"
    ON public.procedurelog USING btree
    ("Priority" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: procedurelog_Prognosis

-- DROP INDEX IF EXISTS public."procedurelog_Prognosis";

CREATE INDEX IF NOT EXISTS "procedurelog_Prognosis"
    ON public.procedurelog USING btree
    ("Prognosis" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: procedurelog_ProvOrderOverride

-- DROP INDEX IF EXISTS public."procedurelog_ProvOrderOverride";

CREATE INDEX IF NOT EXISTS "procedurelog_ProvOrderOverride"
    ON public.procedurelog USING btree
    ("ProvOrderOverride" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: procedurelog_RadiologyProcs

-- DROP INDEX IF EXISTS public."procedurelog_RadiologyProcs";

CREATE INDEX IF NOT EXISTS "procedurelog_RadiologyProcs"
    ON public.procedurelog USING btree
    ("AptNum" ASC NULLS LAST, "CodeNum" ASC NULLS LAST, "ProcStatus" ASC NULLS LAST, "IsCpoe" ASC NULLS LAST, "ProvNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: procedurelog_RepeatChargeNum

-- DROP INDEX IF EXISTS public."procedurelog_RepeatChargeNum";

CREATE INDEX IF NOT EXISTS "procedurelog_RepeatChargeNum"
    ON public.procedurelog USING btree
    ("RepeatChargeNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: procedurelog_SecUserNumEntry

-- DROP INDEX IF EXISTS public."procedurelog_SecUserNumEntry";

CREATE INDEX IF NOT EXISTS "procedurelog_SecUserNumEntry"
    ON public.procedurelog USING btree
    ("SecUserNumEntry" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: procedurelog_StatementNum

-- DROP INDEX IF EXISTS public."procedurelog_StatementNum";

CREATE INDEX IF NOT EXISTS "procedurelog_StatementNum"
    ON public.procedurelog USING btree
    ("StatementNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: procedurelog_indexAgingCovering

-- DROP INDEX IF EXISTS public."procedurelog_indexAgingCovering";

CREATE INDEX IF NOT EXISTS "procedurelog_indexAgingCovering"
    ON public.procedurelog USING btree
    ("PatNum" ASC NULLS LAST, "ProcStatus" ASC NULLS LAST, "ProcFee" ASC NULLS LAST, "UnitQty" ASC NULLS LAST, "BaseUnits" ASC NULLS LAST, "ProcDate" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: procedurelog_indexPNPD

-- DROP INDEX IF EXISTS public."procedurelog_indexPNPD";

CREATE INDEX IF NOT EXISTS "procedurelog_indexPNPD"
    ON public.procedurelog USING btree
    ("ProvNum" ASC NULLS LAST, "ProcDate" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: procedurelog_indexPNPSCN

-- DROP INDEX IF EXISTS public."procedurelog_indexPNPSCN";

CREATE INDEX IF NOT EXISTS "procedurelog_indexPNPSCN"
    ON public.procedurelog USING btree
    ("PatNum" ASC NULLS LAST, "ProcStatus" ASC NULLS LAST, "ClinicNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: procedurelog_procedurelog_ProcNumLab

-- DROP INDEX IF EXISTS public."procedurelog_procedurelog_ProcNumLab";

CREATE INDEX IF NOT EXISTS "procedurelog_procedurelog_ProcNumLab"
    ON public.procedurelog USING btree
    ("ProcNumLab" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Table: public.insplan

-- DROP TABLE IF EXISTS public.insplan;

CREATE TABLE IF NOT EXISTS public.insplan
(
    "PlanNum" integer NOT NULL DEFAULT nextval('"insplan_PlanNum_seq"'::regclass),
    "GroupName" character varying(50) COLLATE pg_catalog."default",
    "GroupNum" character varying(25) COLLATE pg_catalog."default",
    "PlanNote" text COLLATE pg_catalog."default",
    "FeeSched" bigint,
    "PlanType" character varying(1) COLLATE pg_catalog."default",
    "ClaimFormNum" bigint,
    "UseAltCode" smallint DEFAULT 0,
    "ClaimsUseUCR" smallint DEFAULT 0,
    "CopayFeeSched" bigint,
    "EmployerNum" bigint,
    "CarrierNum" bigint,
    "AllowedFeeSched" bigint,
    "TrojanID" character varying(100) COLLATE pg_catalog."default",
    "DivisionNo" character varying(255) COLLATE pg_catalog."default",
    "IsMedical" smallint,
    "FilingCode" bigint,
    "DentaideCardSequence" smallint,
    "ShowBaseUnits" boolean,
    "CodeSubstNone" boolean,
    "IsHidden" smallint,
    "MonthRenew" smallint,
    "FilingCodeSubtype" bigint,
    "CanadianPlanFlag" character varying(5) COLLATE pg_catalog."default",
    "CanadianDiagnosticCode" character varying(255) COLLATE pg_catalog."default",
    "CanadianInstitutionCode" character varying(255) COLLATE pg_catalog."default",
    "RxBIN" character varying(255) COLLATE pg_catalog."default",
    "CobRule" smallint,
    "SopCode" character varying(255) COLLATE pg_catalog."default",
    "SecUserNumEntry" bigint,
    "SecDateEntry" date,
    "SecDateTEdit" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    "HideFromVerifyList" smallint,
    "OrthoType" smallint,
    "OrthoAutoProcFreq" smallint,
    "OrthoAutoProcCodeNumOverride" bigint,
    "OrthoAutoFeeBilled" double precision,
    "OrthoAutoClaimDaysWait" integer,
    "BillingType" bigint,
    "HasPpoSubstWriteoffs" smallint,
    "ExclusionFeeRule" smallint,
    "ManualFeeSchedNum" bigint DEFAULT 0,
    "IsBlueBookEnabled" smallint DEFAULT 1,
    "InsPlansZeroWriteOffsOnAnnualMaxOverride" smallint,
    "InsPlansZeroWriteOffsOnFreqOrAgingOverride" smallint,
    "PerVisitPatAmount" double precision,
    "PerVisitInsAmount" double precision,
    CONSTRAINT insplan_pkey PRIMARY KEY ("PlanNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.insplan
    OWNER to postgres;
-- Index: insplan_AllowedFeeSched

-- DROP INDEX IF EXISTS public."insplan_AllowedFeeSched";

CREATE INDEX IF NOT EXISTS "insplan_AllowedFeeSched"
    ON public.insplan USING btree
    ("AllowedFeeSched" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: insplan_BillingType

-- DROP INDEX IF EXISTS public."insplan_BillingType";

CREATE INDEX IF NOT EXISTS "insplan_BillingType"
    ON public.insplan USING btree
    ("BillingType" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: insplan_CarrierNum

-- DROP INDEX IF EXISTS public."insplan_CarrierNum";

CREATE INDEX IF NOT EXISTS "insplan_CarrierNum"
    ON public.insplan USING btree
    ("CarrierNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: insplan_CarrierNumPlanNum

-- DROP INDEX IF EXISTS public."insplan_CarrierNumPlanNum";

CREATE INDEX IF NOT EXISTS "insplan_CarrierNumPlanNum"
    ON public.insplan USING btree
    ("CarrierNum" ASC NULLS LAST, "PlanNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: insplan_CopayFeeSched

-- DROP INDEX IF EXISTS public."insplan_CopayFeeSched";

CREATE INDEX IF NOT EXISTS "insplan_CopayFeeSched"
    ON public.insplan USING btree
    ("CopayFeeSched" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: insplan_FeeSched

-- DROP INDEX IF EXISTS public."insplan_FeeSched";

CREATE INDEX IF NOT EXISTS "insplan_FeeSched"
    ON public.insplan USING btree
    ("FeeSched" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: insplan_FilingCodeSubtype

-- DROP INDEX IF EXISTS public."insplan_FilingCodeSubtype";

CREATE INDEX IF NOT EXISTS "insplan_FilingCodeSubtype"
    ON public.insplan USING btree
    ("FilingCodeSubtype" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: insplan_ManualFeeSchedNum

-- DROP INDEX IF EXISTS public."insplan_ManualFeeSchedNum";

CREATE INDEX IF NOT EXISTS "insplan_ManualFeeSchedNum"
    ON public.insplan USING btree
    ("ManualFeeSchedNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: insplan_OrthoAutoProcCodeNumOverride

-- DROP INDEX IF EXISTS public."insplan_OrthoAutoProcCodeNumOverride";

CREATE INDEX IF NOT EXISTS "insplan_OrthoAutoProcCodeNumOverride"
    ON public.insplan USING btree
    ("OrthoAutoProcCodeNumOverride" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: insplan_SecUserNumEntry

-- DROP INDEX IF EXISTS public."insplan_SecUserNumEntry";

CREATE INDEX IF NOT EXISTS "insplan_SecUserNumEntry"
    ON public.insplan USING btree
    ("SecUserNumEntry" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: insplan_TrojanID

-- DROP INDEX IF EXISTS public."insplan_TrojanID";

CREATE INDEX IF NOT EXISTS "insplan_TrojanID"
    ON public.insplan USING btree
    ("TrojanID" COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;
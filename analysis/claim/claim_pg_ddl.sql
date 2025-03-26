-- Table: public.claim

-- DROP TABLE IF EXISTS public.claim;

CREATE TABLE IF NOT EXISTS public.claim
(
    "ClaimNum" integer NOT NULL DEFAULT nextval('"claim_ClaimNum_seq"'::regclass),
    "PatNum" bigint,
    "DateService" date,
    "DateSent" date,
    "ClaimStatus" character varying(1) COLLATE pg_catalog."default",
    "DateReceived" date,
    "PlanNum" bigint,
    "ProvTreat" bigint,
    "ClaimFee" double precision DEFAULT 0,
    "InsPayEst" double precision DEFAULT 0,
    "InsPayAmt" double precision DEFAULT 0,
    "DedApplied" double precision DEFAULT 0,
    "PreAuthString" character varying(40) COLLATE pg_catalog."default",
    "IsProsthesis" character varying(1) COLLATE pg_catalog."default",
    "PriorDate" date,
    "ReasonUnderPaid" character varying(255) COLLATE pg_catalog."default",
    "ClaimNote" character varying(400) COLLATE pg_catalog."default",
    "ClaimType" character varying(255) COLLATE pg_catalog."default",
    "ProvBill" bigint,
    "ReferringProv" bigint,
    "RefNumString" character varying(40) COLLATE pg_catalog."default",
    "PlaceService" smallint DEFAULT 0,
    "AccidentRelated" character varying(1) COLLATE pg_catalog."default",
    "AccidentDate" date,
    "AccidentST" character varying(2) COLLATE pg_catalog."default",
    "EmployRelated" smallint DEFAULT 0,
    "IsOrtho" smallint DEFAULT 0,
    "OrthoRemainM" smallint DEFAULT 0,
    "OrthoDate" date,
    "PatRelat" smallint DEFAULT 0,
    "PlanNum2" bigint,
    "PatRelat2" smallint DEFAULT 0,
    "WriteOff" double precision DEFAULT 0,
    "Radiographs" smallint DEFAULT 0,
    "ClinicNum" bigint,
    "ClaimForm" bigint,
    "AttachedImages" integer,
    "AttachedModels" integer,
    "AttachedFlags" character varying(255) COLLATE pg_catalog."default",
    "AttachmentID" character varying(255) COLLATE pg_catalog."default",
    "CanadianMaterialsForwarded" character varying(10) COLLATE pg_catalog."default",
    "CanadianReferralProviderNum" character varying(20) COLLATE pg_catalog."default",
    "CanadianReferralReason" smallint,
    "CanadianIsInitialLower" character varying(5) COLLATE pg_catalog."default",
    "CanadianDateInitialLower" date,
    "CanadianMandProsthMaterial" smallint,
    "CanadianIsInitialUpper" character varying(5) COLLATE pg_catalog."default",
    "CanadianDateInitialUpper" date,
    "CanadianMaxProsthMaterial" smallint,
    "InsSubNum" bigint,
    "InsSubNum2" bigint,
    "CanadaTransRefNum" character varying(255) COLLATE pg_catalog."default",
    "CanadaEstTreatStartDate" date,
    "CanadaInitialPayment" double precision,
    "CanadaPaymentMode" smallint,
    "CanadaTreatDuration" smallint,
    "CanadaNumAnticipatedPayments" smallint,
    "CanadaAnticipatedPayAmount" double precision,
    "PriorAuthorizationNumber" character varying(255) COLLATE pg_catalog."default",
    "SpecialProgramCode" smallint,
    "UniformBillType" character varying(255) COLLATE pg_catalog."default",
    "MedType" smallint,
    "AdmissionTypeCode" character varying(255) COLLATE pg_catalog."default",
    "AdmissionSourceCode" character varying(255) COLLATE pg_catalog."default",
    "PatientStatusCode" character varying(255) COLLATE pg_catalog."default",
    "CustomTracking" bigint,
    "DateResent" date,
    "CorrectionType" smallint,
    "ClaimIdentifier" character varying(255) COLLATE pg_catalog."default",
    "OrigRefNum" character varying(255) COLLATE pg_catalog."default",
    "ProvOrderOverride" bigint,
    "OrthoTotalM" smallint,
    "ShareOfCost" double precision,
    "SecUserNumEntry" bigint,
    "SecDateEntry" date,
    "SecDateTEdit" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    "OrderingReferralNum" bigint,
    "DateSentOrig" date,
    "DateIllnessInjuryPreg" date,
    "DateIllnessInjuryPregQualifier" smallint,
    "DateOther" date,
    "DateOtherQualifier" smallint,
    "IsOutsideLab" smallint,
    "SecurityHash" character varying(255) COLLATE pg_catalog."default",
    "Narrative" text COLLATE pg_catalog."default",
    CONSTRAINT claim_pkey PRIMARY KEY ("ClaimNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.claim
    OWNER to postgres;
-- Index: claim_ClinicNum

-- DROP INDEX IF EXISTS public."claim_ClinicNum";

CREATE INDEX IF NOT EXISTS "claim_ClinicNum"
    ON public.claim USING btree
    ("ClinicNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: claim_CustomTracking

-- DROP INDEX IF EXISTS public."claim_CustomTracking";

CREATE INDEX IF NOT EXISTS "claim_CustomTracking"
    ON public.claim USING btree
    ("CustomTracking" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: claim_InsSubNum

-- DROP INDEX IF EXISTS public."claim_InsSubNum";

CREATE INDEX IF NOT EXISTS "claim_InsSubNum"
    ON public.claim USING btree
    ("InsSubNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: claim_InsSubNum2

-- DROP INDEX IF EXISTS public."claim_InsSubNum2";

CREATE INDEX IF NOT EXISTS "claim_InsSubNum2"
    ON public.claim USING btree
    ("InsSubNum2" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: claim_OrderingReferralNum

-- DROP INDEX IF EXISTS public."claim_OrderingReferralNum";

CREATE INDEX IF NOT EXISTS "claim_OrderingReferralNum"
    ON public.claim USING btree
    ("OrderingReferralNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: claim_PatStatusTypeDate

-- DROP INDEX IF EXISTS public."claim_PatStatusTypeDate";

CREATE INDEX IF NOT EXISTS "claim_PatStatusTypeDate"
    ON public.claim USING btree
    ("PatNum" ASC NULLS LAST, "ClaimStatus" COLLATE pg_catalog."default" ASC NULLS LAST, "ClaimType" COLLATE pg_catalog."default" ASC NULLS LAST, "DateSent" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: claim_ProvBill

-- DROP INDEX IF EXISTS public."claim_ProvBill";

CREATE INDEX IF NOT EXISTS "claim_ProvBill"
    ON public.claim USING btree
    ("ProvBill" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: claim_ProvOrderOverride

-- DROP INDEX IF EXISTS public."claim_ProvOrderOverride";

CREATE INDEX IF NOT EXISTS "claim_ProvOrderOverride"
    ON public.claim USING btree
    ("ProvOrderOverride" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: claim_ProvTreat

-- DROP INDEX IF EXISTS public."claim_ProvTreat";

CREATE INDEX IF NOT EXISTS "claim_ProvTreat"
    ON public.claim USING btree
    ("ProvTreat" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: claim_SecUserNumEntry

-- DROP INDEX IF EXISTS public."claim_SecUserNumEntry";

CREATE INDEX IF NOT EXISTS "claim_SecUserNumEntry"
    ON public.claim USING btree
    ("SecUserNumEntry" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: claim_idx_ml_claim_lookup

-- DROP INDEX IF EXISTS public.claim_idx_ml_claim_lookup;

CREATE INDEX IF NOT EXISTS claim_idx_ml_claim_lookup
    ON public.claim USING btree
    ("ClaimNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: claim_indexOutClaimCovering

-- DROP INDEX IF EXISTS public."claim_indexOutClaimCovering";

CREATE INDEX IF NOT EXISTS "claim_indexOutClaimCovering"
    ON public.claim USING btree
    ("PlanNum" ASC NULLS LAST, "ClaimStatus" COLLATE pg_catalog."default" ASC NULLS LAST, "ClaimType" COLLATE pg_catalog."default" ASC NULLS LAST, "PatNum" ASC NULLS LAST, "ClaimNum" ASC NULLS LAST, "DateService" ASC NULLS LAST, "ProvTreat" ASC NULLS LAST, "ClaimFee" ASC NULLS LAST, "ClinicNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: claim_indexPlanNum

-- DROP INDEX IF EXISTS public."claim_indexPlanNum";

CREATE INDEX IF NOT EXISTS "claim_indexPlanNum"
    ON public.claim USING btree
    ("PlanNum" ASC NULLS LAST)
    TABLESPACE pg_default;
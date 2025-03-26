-- Table: public.claimproc

-- DROP TABLE IF EXISTS public.claimproc;

CREATE TABLE IF NOT EXISTS public.claimproc
(
    "ClaimProcNum" integer NOT NULL DEFAULT nextval('"claimproc_ClaimProcNum_seq"'::regclass),
    "ProcNum" bigint,
    "ClaimNum" bigint,
    "PatNum" bigint,
    "ProvNum" bigint,
    "FeeBilled" double precision DEFAULT 0,
    "InsPayEst" double precision DEFAULT 0,
    "DedApplied" double precision DEFAULT 0,
    "Status" smallint DEFAULT 0,
    "InsPayAmt" double precision DEFAULT 0,
    "Remarks" character varying(255) COLLATE pg_catalog."default",
    "ClaimPaymentNum" bigint,
    "PlanNum" bigint,
    "DateCP" date,
    "WriteOff" double precision DEFAULT 0,
    "CodeSent" character varying(15) COLLATE pg_catalog."default",
    "AllowedOverride" double precision,
    "Percentage" smallint DEFAULT '-1'::integer,
    "PercentOverride" smallint DEFAULT '-1'::integer,
    "CopayAmt" double precision DEFAULT '-1'::integer,
    "NoBillIns" smallint DEFAULT 0,
    "PaidOtherIns" double precision DEFAULT '-1'::integer,
    "BaseEst" double precision DEFAULT 0,
    "CopayOverride" double precision DEFAULT '-1'::integer,
    "ProcDate" date,
    "DateEntry" date,
    "LineNumber" smallint,
    "DedEst" double precision,
    "DedEstOverride" double precision,
    "InsEstTotal" double precision,
    "InsEstTotalOverride" double precision,
    "PaidOtherInsOverride" double precision,
    "EstimateNote" character varying(255) COLLATE pg_catalog."default",
    "WriteOffEst" double precision,
    "WriteOffEstOverride" double precision,
    "ClinicNum" bigint,
    "InsSubNum" bigint,
    "PaymentRow" integer,
    "PayPlanNum" bigint,
    "ClaimPaymentTracking" bigint,
    "SecUserNumEntry" bigint,
    "SecDateEntry" date,
    "SecDateTEdit" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    "DateSuppReceived" date,
    "DateInsFinalized" date,
    "IsTransfer" smallint,
    "ClaimAdjReasonCodes" character varying(255) COLLATE pg_catalog."default",
    "IsOverpay" smallint,
    "SecurityHash" character varying(255) COLLATE pg_catalog."default",
    CONSTRAINT claimproc_pkey PRIMARY KEY ("ClaimProcNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.claimproc
    OWNER to postgres;
-- Index: claimproc_ClaimPaymentTracking

-- DROP INDEX IF EXISTS public."claimproc_ClaimPaymentTracking";

CREATE INDEX IF NOT EXISTS "claimproc_ClaimPaymentTracking"
    ON public.claimproc USING btree
    ("ClaimPaymentTracking" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: claimproc_ClinicNum

-- DROP INDEX IF EXISTS public."claimproc_ClinicNum";

CREATE INDEX IF NOT EXISTS "claimproc_ClinicNum"
    ON public.claimproc USING btree
    ("ClinicNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: claimproc_DateCP

-- DROP INDEX IF EXISTS public."claimproc_DateCP";

CREATE INDEX IF NOT EXISTS "claimproc_DateCP"
    ON public.claimproc USING btree
    ("DateCP" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: claimproc_DateSuppReceived

-- DROP INDEX IF EXISTS public."claimproc_DateSuppReceived";

CREATE INDEX IF NOT EXISTS "claimproc_DateSuppReceived"
    ON public.claimproc USING btree
    ("DateSuppReceived" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: claimproc_InsSubNum

-- DROP INDEX IF EXISTS public."claimproc_InsSubNum";

CREATE INDEX IF NOT EXISTS "claimproc_InsSubNum"
    ON public.claimproc USING btree
    ("InsSubNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: claimproc_PayPlanNum

-- DROP INDEX IF EXISTS public."claimproc_PayPlanNum";

CREATE INDEX IF NOT EXISTS "claimproc_PayPlanNum"
    ON public.claimproc USING btree
    ("PayPlanNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: claimproc_SecDateTEditPN

-- DROP INDEX IF EXISTS public."claimproc_SecDateTEditPN";

CREATE INDEX IF NOT EXISTS "claimproc_SecDateTEditPN"
    ON public.claimproc USING btree
    ("SecDateTEdit" ASC NULLS LAST, "PatNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: claimproc_SecUserNumEntry

-- DROP INDEX IF EXISTS public."claimproc_SecUserNumEntry";

CREATE INDEX IF NOT EXISTS "claimproc_SecUserNumEntry"
    ON public.claimproc USING btree
    ("SecUserNumEntry" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: claimproc_Status

-- DROP INDEX IF EXISTS public."claimproc_Status";

CREATE INDEX IF NOT EXISTS "claimproc_Status"
    ON public.claimproc USING btree
    ("Status" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: claimproc_idx_ml_claimproc_core

-- DROP INDEX IF EXISTS public.claimproc_idx_ml_claimproc_core;

CREATE INDEX IF NOT EXISTS claimproc_idx_ml_claimproc_core
    ON public.claimproc USING btree
    ("ProcNum" ASC NULLS LAST, "InsPayAmt" ASC NULLS LAST, "InsPayEst" ASC NULLS LAST, "Status" ASC NULLS LAST, "ClaimNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: claimproc_idx_ml_claimproc_procnum

-- DROP INDEX IF EXISTS public.claimproc_idx_ml_claimproc_procnum;

CREATE INDEX IF NOT EXISTS claimproc_idx_ml_claimproc_procnum
    ON public.claimproc USING btree
    ("ProcNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: claimproc_idx_ml_claimproc_status

-- DROP INDEX IF EXISTS public.claimproc_idx_ml_claimproc_status;

CREATE INDEX IF NOT EXISTS claimproc_idx_ml_claimproc_status
    ON public.claimproc USING btree
    ("Status" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: claimproc_indexAcctCov

-- DROP INDEX IF EXISTS public."claimproc_indexAcctCov";

CREATE INDEX IF NOT EXISTS "claimproc_indexAcctCov"
    ON public.claimproc USING btree
    ("ProcNum" ASC NULLS LAST, "PlanNum" ASC NULLS LAST, "Status" ASC NULLS LAST, "InsPayAmt" ASC NULLS LAST, "InsPayEst" ASC NULLS LAST, "WriteOff" ASC NULLS LAST, "NoBillIns" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: claimproc_indexAgingCovering

-- DROP INDEX IF EXISTS public."claimproc_indexAgingCovering";

CREATE INDEX IF NOT EXISTS "claimproc_indexAgingCovering"
    ON public.claimproc USING btree
    ("Status" ASC NULLS LAST, "PatNum" ASC NULLS LAST, "DateCP" ASC NULLS LAST, "PayPlanNum" ASC NULLS LAST, "InsPayAmt" ASC NULLS LAST, "WriteOff" ASC NULLS LAST, "InsPayEst" ASC NULLS LAST, "ProcDate" ASC NULLS LAST, "ProcNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: claimproc_indexCPNSIPA

-- DROP INDEX IF EXISTS public."claimproc_indexCPNSIPA";

CREATE INDEX IF NOT EXISTS "claimproc_indexCPNSIPA"
    ON public.claimproc USING btree
    ("ClaimPaymentNum" ASC NULLS LAST, "Status" ASC NULLS LAST, "InsPayAmt" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: claimproc_indexClaimNum

-- DROP INDEX IF EXISTS public."claimproc_indexClaimNum";

CREATE INDEX IF NOT EXISTS "claimproc_indexClaimNum"
    ON public.claimproc USING btree
    ("ClaimNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: claimproc_indexClaimPaymentNum

-- DROP INDEX IF EXISTS public."claimproc_indexClaimPaymentNum";

CREATE INDEX IF NOT EXISTS "claimproc_indexClaimPaymentNum"
    ON public.claimproc USING btree
    ("ClaimPaymentNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: claimproc_indexOutClaimCovering

-- DROP INDEX IF EXISTS public."claimproc_indexOutClaimCovering";

CREATE INDEX IF NOT EXISTS "claimproc_indexOutClaimCovering"
    ON public.claimproc USING btree
    ("ClaimNum" ASC NULLS LAST, "ClaimPaymentNum" ASC NULLS LAST, "InsPayAmt" ASC NULLS LAST, "DateCP" ASC NULLS LAST, "IsTransfer" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: claimproc_indexPNDCP

-- DROP INDEX IF EXISTS public."claimproc_indexPNDCP";

CREATE INDEX IF NOT EXISTS "claimproc_indexPNDCP"
    ON public.claimproc USING btree
    ("ProvNum" ASC NULLS LAST, "DateCP" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: claimproc_indexPNPD

-- DROP INDEX IF EXISTS public."claimproc_indexPNPD";

CREATE INDEX IF NOT EXISTS "claimproc_indexPNPD"
    ON public.claimproc USING btree
    ("ProvNum" ASC NULLS LAST, "ProcDate" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: claimproc_indexPatNum

-- DROP INDEX IF EXISTS public."claimproc_indexPatNum";

CREATE INDEX IF NOT EXISTS "claimproc_indexPatNum"
    ON public.claimproc USING btree
    ("PatNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: claimproc_indexPlanNum

-- DROP INDEX IF EXISTS public."claimproc_indexPlanNum";

CREATE INDEX IF NOT EXISTS "claimproc_indexPlanNum"
    ON public.claimproc USING btree
    ("PlanNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: claimproc_indexProcNum

-- DROP INDEX IF EXISTS public."claimproc_indexProcNum";

CREATE INDEX IF NOT EXISTS "claimproc_indexProcNum"
    ON public.claimproc USING btree
    ("ProcNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: claimproc_indexProvNum

-- DROP INDEX IF EXISTS public."claimproc_indexProvNum";

CREATE INDEX IF NOT EXISTS "claimproc_indexProvNum"
    ON public.claimproc USING btree
    ("ProvNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: claimproc_indexTxFinder

-- DROP INDEX IF EXISTS public."claimproc_indexTxFinder";

CREATE INDEX IF NOT EXISTS "claimproc_indexTxFinder"
    ON public.claimproc USING btree
    ("InsSubNum" ASC NULLS LAST, "ProcNum" ASC NULLS LAST, "Status" ASC NULLS LAST, "ProcDate" ASC NULLS LAST, "PatNum" ASC NULLS LAST, "InsPayAmt" ASC NULLS LAST, "InsPayEst" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Table: public.paysplit

-- DROP TABLE IF EXISTS public.paysplit;

CREATE TABLE IF NOT EXISTS public.paysplit
(
    "SplitNum" integer NOT NULL DEFAULT nextval('"paysplit_SplitNum_seq"'::regclass),
    "SplitAmt" double precision DEFAULT 0,
    "PatNum" bigint,
    "ProcDate" date,
    "PayNum" bigint,
    "IsDiscount" smallint DEFAULT 0,
    "DiscountType" smallint DEFAULT 0,
    "ProvNum" bigint,
    "PayPlanNum" bigint,
    "DatePay" date,
    "ProcNum" bigint,
    "DateEntry" date,
    "UnearnedType" bigint,
    "ClinicNum" bigint,
    "SecUserNumEntry" bigint,
    "SecDateTEdit" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    "FSplitNum" bigint,
    "AdjNum" bigint,
    "PayPlanChargeNum" bigint,
    "PayPlanDebitType" smallint,
    "SecurityHash" character varying(255) COLLATE pg_catalog."default",
    CONSTRAINT paysplit_pkey PRIMARY KEY ("SplitNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.paysplit
    OWNER to postgres;
-- Index: paysplit_AdjNum

-- DROP INDEX IF EXISTS public."paysplit_AdjNum";

CREATE INDEX IF NOT EXISTS "paysplit_AdjNum"
    ON public.paysplit USING btree
    ("AdjNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: paysplit_ClinicNum

-- DROP INDEX IF EXISTS public."paysplit_ClinicNum";

CREATE INDEX IF NOT EXISTS "paysplit_ClinicNum"
    ON public.paysplit USING btree
    ("ClinicNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: paysplit_DatePay

-- DROP INDEX IF EXISTS public."paysplit_DatePay";

CREATE INDEX IF NOT EXISTS "paysplit_DatePay"
    ON public.paysplit USING brin
    ("DatePay")
    TABLESPACE pg_default;
-- Index: paysplit_PayNum

-- DROP INDEX IF EXISTS public."paysplit_PayNum";

CREATE INDEX IF NOT EXISTS "paysplit_PayNum"
    ON public.paysplit USING btree
    ("PayNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: paysplit_PayPlanChargeNum

-- DROP INDEX IF EXISTS public."paysplit_PayPlanChargeNum";

CREATE INDEX IF NOT EXISTS "paysplit_PayPlanChargeNum"
    ON public.paysplit USING btree
    ("PayPlanChargeNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: paysplit_PayPlanNum

-- DROP INDEX IF EXISTS public."paysplit_PayPlanNum";

CREATE INDEX IF NOT EXISTS "paysplit_PayPlanNum"
    ON public.paysplit USING btree
    ("PayPlanNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: paysplit_PrepaymentNum

-- DROP INDEX IF EXISTS public."paysplit_PrepaymentNum";

CREATE INDEX IF NOT EXISTS "paysplit_PrepaymentNum"
    ON public.paysplit USING btree
    ("FSplitNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: paysplit_ProcNum

-- DROP INDEX IF EXISTS public."paysplit_ProcNum";

CREATE INDEX IF NOT EXISTS "paysplit_ProcNum"
    ON public.paysplit USING btree
    ("ProcNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: paysplit_ProvNum

-- DROP INDEX IF EXISTS public."paysplit_ProvNum";

CREATE INDEX IF NOT EXISTS "paysplit_ProvNum"
    ON public.paysplit USING btree
    ("ProvNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: paysplit_SecDateTEditPN

-- DROP INDEX IF EXISTS public."paysplit_SecDateTEditPN";

CREATE INDEX IF NOT EXISTS "paysplit_SecDateTEditPN"
    ON public.paysplit USING btree
    ("SecDateTEdit" ASC NULLS LAST, "PatNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: paysplit_SecUserNumEntry

-- DROP INDEX IF EXISTS public."paysplit_SecUserNumEntry";

CREATE INDEX IF NOT EXISTS "paysplit_SecUserNumEntry"
    ON public.paysplit USING btree
    ("SecUserNumEntry" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: paysplit_idx_ml_paysplit_payment

-- DROP INDEX IF EXISTS public.paysplit_idx_ml_paysplit_payment;

CREATE INDEX IF NOT EXISTS paysplit_idx_ml_paysplit_payment
    ON public.paysplit USING btree
    ("ProcNum" ASC NULLS LAST, "PayNum" ASC NULLS LAST, "SplitAmt" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: paysplit_idx_ml_paysplit_paynum

-- DROP INDEX IF EXISTS public.paysplit_idx_ml_paysplit_paynum;

CREATE INDEX IF NOT EXISTS paysplit_idx_ml_paysplit_paynum
    ON public.paysplit USING btree
    ("PayNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: paysplit_idx_ml_paysplit_proc_pay

-- DROP INDEX IF EXISTS public.paysplit_idx_ml_paysplit_proc_pay;

CREATE INDEX IF NOT EXISTS paysplit_idx_ml_paysplit_proc_pay
    ON public.paysplit USING btree
    ("ProcNum" ASC NULLS LAST, "PayNum" ASC NULLS LAST, "SplitAmt" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: paysplit_idx_ml_paysplit_procnum

-- DROP INDEX IF EXISTS public.paysplit_idx_ml_paysplit_procnum;

CREATE INDEX IF NOT EXISTS paysplit_idx_ml_paysplit_procnum
    ON public.paysplit USING btree
    ("ProcNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: paysplit_indexPNAmt

-- DROP INDEX IF EXISTS public."paysplit_indexPNAmt";

CREATE INDEX IF NOT EXISTS "paysplit_indexPNAmt"
    ON public.paysplit USING btree
    ("ProcNum" ASC NULLS LAST, "SplitAmt" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: paysplit_indexPatNum

-- DROP INDEX IF EXISTS public."paysplit_indexPatNum";

CREATE INDEX IF NOT EXISTS "paysplit_indexPatNum"
    ON public.paysplit USING btree
    ("PatNum" ASC NULLS LAST)
    TABLESPACE pg_default;
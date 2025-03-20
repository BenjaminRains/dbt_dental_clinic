-- Table: public.payment

-- DROP TABLE IF EXISTS public.payment;

CREATE TABLE IF NOT EXISTS public.payment
(
    "PayNum" integer NOT NULL DEFAULT nextval('"payment_PayNum_seq"'::regclass),
    "PayType" bigint,
    "PayDate" date,
    "PayAmt" double precision DEFAULT 0,
    "CheckNum" character varying(25) COLLATE pg_catalog."default",
    "BankBranch" character varying(25) COLLATE pg_catalog."default",
    "PayNote" text COLLATE pg_catalog."default",
    "IsSplit" smallint DEFAULT 0,
    "PatNum" bigint,
    "ClinicNum" bigint,
    "DateEntry" date,
    "DepositNum" bigint,
    "Receipt" text COLLATE pg_catalog."default",
    "IsRecurringCC" smallint,
    "SecUserNumEntry" bigint,
    "SecDateTEdit" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    "PaymentSource" smallint,
    "ProcessStatus" smallint,
    "RecurringChargeDate" date,
    "ExternalId" character varying(255) COLLATE pg_catalog."default",
    "PaymentStatus" smallint,
    "IsCcCompleted" smallint,
    "MerchantFee" double precision,
    CONSTRAINT payment_pkey PRIMARY KEY ("PayNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.payment
    OWNER to postgres;
-- Index: payment_ClinicNum

-- DROP INDEX IF EXISTS public."payment_ClinicNum";

CREATE INDEX IF NOT EXISTS "payment_ClinicNum"
    ON public.payment USING btree
    ("ClinicNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: payment_DepositNum

-- DROP INDEX IF EXISTS public."payment_DepositNum";

CREATE INDEX IF NOT EXISTS "payment_DepositNum"
    ON public.payment USING btree
    ("DepositNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: payment_PayType

-- DROP INDEX IF EXISTS public."payment_PayType";

CREATE INDEX IF NOT EXISTS "payment_PayType"
    ON public.payment USING btree
    ("PayType" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: payment_ProcessStatus

-- DROP INDEX IF EXISTS public."payment_ProcessStatus";

CREATE INDEX IF NOT EXISTS "payment_ProcessStatus"
    ON public.payment USING btree
    ("ProcessStatus" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: payment_SecUserNumEntry

-- DROP INDEX IF EXISTS public."payment_SecUserNumEntry";

CREATE INDEX IF NOT EXISTS "payment_SecUserNumEntry"
    ON public.payment USING btree
    ("SecUserNumEntry" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: payment_idx_ml_payment_core

-- DROP INDEX IF EXISTS public.payment_idx_ml_payment_core;

CREATE INDEX IF NOT EXISTS payment_idx_ml_payment_core
    ON public.payment USING btree
    ("PayNum" ASC NULLS LAST, "PayDate" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: payment_idx_ml_payment_window

-- DROP INDEX IF EXISTS public.payment_idx_ml_payment_window;

CREATE INDEX IF NOT EXISTS payment_idx_ml_payment_window
    ON public.payment USING btree
    ("PayDate" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: payment_indexPatNum

-- DROP INDEX IF EXISTS public."payment_indexPatNum";

CREATE INDEX IF NOT EXISTS "payment_indexPatNum"
    ON public.payment USING btree
    ("PatNum" ASC NULLS LAST)
    TABLESPACE pg_default;
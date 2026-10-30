-- Table: public.claimpayment

-- DROP TABLE IF EXISTS public.claimpayment;

CREATE TABLE IF NOT EXISTS public.claimpayment
(
    "ClaimPaymentNum" integer NOT NULL DEFAULT nextval('"claimpayment_ClaimPaymentNum_seq"'::regclass),
    "CheckDate" date,
    "CheckAmt" double precision DEFAULT 0,
    "CheckNum" character varying(25) COLLATE pg_catalog."default",
    "BankBranch" character varying(25) COLLATE pg_catalog."default",
    "Note" character varying(255) COLLATE pg_catalog."default",
    "ClinicNum" bigint,
    "DepositNum" bigint,
    "CarrierName" character varying(255) COLLATE pg_catalog."default",
    "DateIssued" date,
    "IsPartial" smallint,
    "PayType" bigint,
    "SecUserNumEntry" bigint,
    "SecDateEntry" date,
    "SecDateTEdit" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    "PayGroup" bigint,
    CONSTRAINT claimpayment_pkey PRIMARY KEY ("ClaimPaymentNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.claimpayment
    OWNER to postgres;
-- Index: claimpayment_CheckDate

-- DROP INDEX IF EXISTS public."claimpayment_CheckDate";

CREATE INDEX IF NOT EXISTS "claimpayment_CheckDate"
    ON public.claimpayment USING btree
    ("CheckDate" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: claimpayment_ClinicNum

-- DROP INDEX IF EXISTS public."claimpayment_ClinicNum";

CREATE INDEX IF NOT EXISTS "claimpayment_ClinicNum"
    ON public.claimpayment USING btree
    ("ClinicNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: claimpayment_DepositNum

-- DROP INDEX IF EXISTS public."claimpayment_DepositNum";

CREATE INDEX IF NOT EXISTS "claimpayment_DepositNum"
    ON public.claimpayment USING btree
    ("DepositNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: claimpayment_PayGroup

-- DROP INDEX IF EXISTS public."claimpayment_PayGroup";

CREATE INDEX IF NOT EXISTS "claimpayment_PayGroup"
    ON public.claimpayment USING btree
    ("PayGroup" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: claimpayment_PayType

-- DROP INDEX IF EXISTS public."claimpayment_PayType";

CREATE INDEX IF NOT EXISTS "claimpayment_PayType"
    ON public.claimpayment USING btree
    ("PayType" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: claimpayment_SecUserNumEntry

-- DROP INDEX IF EXISTS public."claimpayment_SecUserNumEntry";

CREATE INDEX IF NOT EXISTS "claimpayment_SecUserNumEntry"
    ON public.claimpayment USING btree
    ("SecUserNumEntry" ASC NULLS LAST)
    TABLESPACE pg_default;
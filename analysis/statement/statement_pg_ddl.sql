-- Table: public.statement

-- DROP TABLE IF EXISTS public.statement;

CREATE TABLE IF NOT EXISTS public.statement
(
    "StatementNum" integer NOT NULL DEFAULT nextval('"statement_StatementNum_seq"'::regclass),
    "PatNum" bigint,
    "DateSent" date,
    "DateRangeFrom" date,
    "DateRangeTo" date,
    "Note" text COLLATE pg_catalog."default",
    "NoteBold" text COLLATE pg_catalog."default",
    "Mode_" smallint,
    "HidePayment" boolean,
    "SinglePatient" boolean,
    "Intermingled" boolean,
    "IsSent" boolean,
    "DocNum" bigint,
    "DateTStamp" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    "IsReceipt" smallint,
    "IsInvoice" smallint,
    "IsInvoiceCopy" smallint,
    "EmailSubject" character varying(255) COLLATE pg_catalog."default",
    "EmailBody" text COLLATE pg_catalog."default",
    "SuperFamily" bigint,
    "IsBalValid" smallint,
    "InsEst" double precision,
    "BalTotal" double precision,
    "StatementType" character varying(50) COLLATE pg_catalog."default",
    "ShortGUID" character varying(30) COLLATE pg_catalog."default",
    "StatementShortURL" character varying(50) COLLATE pg_catalog."default",
    "StatementURL" character varying(255) COLLATE pg_catalog."default",
    "SmsSendStatus" smallint,
    "LimitedCustomFamily" smallint DEFAULT 0,
    CONSTRAINT statement_pkey PRIMARY KEY ("StatementNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.statement
    OWNER to postgres;
-- Index: statement_DocNum

-- DROP INDEX IF EXISTS public."statement_DocNum";

CREATE INDEX IF NOT EXISTS "statement_DocNum"
    ON public.statement USING btree
    ("DocNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: statement_IsSent

-- DROP INDEX IF EXISTS public."statement_IsSent";

CREATE INDEX IF NOT EXISTS "statement_IsSent"
    ON public.statement USING btree
    ("IsSent" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: statement_PatNum

-- DROP INDEX IF EXISTS public."statement_PatNum";

CREATE INDEX IF NOT EXISTS "statement_PatNum"
    ON public.statement USING btree
    ("PatNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: statement_ShortGUID

-- DROP INDEX IF EXISTS public."statement_ShortGUID";

CREATE INDEX IF NOT EXISTS "statement_ShortGUID"
    ON public.statement USING btree
    ("ShortGUID" COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: statement_SuperFamModeDateSent

-- DROP INDEX IF EXISTS public."statement_SuperFamModeDateSent";

CREATE INDEX IF NOT EXISTS "statement_SuperFamModeDateSent"
    ON public.statement USING btree
    ("SuperFamily" ASC NULLS LAST, "Mode_" ASC NULLS LAST, "DateSent" ASC NULLS LAST)
    TABLESPACE pg_default;
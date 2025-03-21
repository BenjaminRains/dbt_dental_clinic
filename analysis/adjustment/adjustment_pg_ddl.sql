-- Table: public.adjustment

-- DROP TABLE IF EXISTS public.adjustment;

CREATE TABLE IF NOT EXISTS public.adjustment
(
    "AdjNum" integer NOT NULL DEFAULT nextval('"adjustment_AdjNum_seq"'::regclass),
    "AdjDate" date,
    "AdjAmt" double precision DEFAULT 0,
    "PatNum" bigint,
    "AdjType" bigint,
    "ProvNum" bigint,
    "AdjNote" text COLLATE pg_catalog."default",
    "ProcDate" date,
    "ProcNum" bigint,
    "DateEntry" date,
    "ClinicNum" bigint,
    "StatementNum" bigint,
    "SecUserNumEntry" bigint,
    "SecDateTEdit" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    "TaxTransID" bigint,
    CONSTRAINT adjustment_pkey PRIMARY KEY ("AdjNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.adjustment
    OWNER to postgres;
-- Index: adjustment_AdjDatePN

-- DROP INDEX IF EXISTS public."adjustment_AdjDatePN";

CREATE INDEX IF NOT EXISTS "adjustment_AdjDatePN"
    ON public.adjustment USING btree
    ("AdjDate" ASC NULLS LAST, "PatNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: adjustment_ClinicNum

-- DROP INDEX IF EXISTS public."adjustment_ClinicNum";

CREATE INDEX IF NOT EXISTS "adjustment_ClinicNum"
    ON public.adjustment USING btree
    ("ClinicNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: adjustment_ProcNum

-- DROP INDEX IF EXISTS public."adjustment_ProcNum";

CREATE INDEX IF NOT EXISTS "adjustment_ProcNum"
    ON public.adjustment USING btree
    ("ProcNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: adjustment_SecDateTEditPN

-- DROP INDEX IF EXISTS public."adjustment_SecDateTEditPN";

CREATE INDEX IF NOT EXISTS "adjustment_SecDateTEditPN"
    ON public.adjustment USING btree
    ("SecDateTEdit" ASC NULLS LAST, "PatNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: adjustment_SecUserNumEntry

-- DROP INDEX IF EXISTS public."adjustment_SecUserNumEntry";

CREATE INDEX IF NOT EXISTS "adjustment_SecUserNumEntry"
    ON public.adjustment USING btree
    ("SecUserNumEntry" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: adjustment_StatementNum

-- DROP INDEX IF EXISTS public."adjustment_StatementNum";

CREATE INDEX IF NOT EXISTS "adjustment_StatementNum"
    ON public.adjustment USING btree
    ("StatementNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: adjustment_TaxTransID

-- DROP INDEX IF EXISTS public."adjustment_TaxTransID";

CREATE INDEX IF NOT EXISTS "adjustment_TaxTransID"
    ON public.adjustment USING btree
    ("TaxTransID" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: adjustment_indexPNAmt

-- DROP INDEX IF EXISTS public."adjustment_indexPNAmt";

CREATE INDEX IF NOT EXISTS "adjustment_indexPNAmt"
    ON public.adjustment USING btree
    ("ProcNum" ASC NULLS LAST, "AdjAmt" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: adjustment_indexPatNum

-- DROP INDEX IF EXISTS public."adjustment_indexPatNum";

CREATE INDEX IF NOT EXISTS "adjustment_indexPatNum"
    ON public.adjustment USING btree
    ("PatNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: adjustment_indexProvNum

-- DROP INDEX IF EXISTS public."adjustment_indexProvNum";

CREATE INDEX IF NOT EXISTS "adjustment_indexProvNum"
    ON public.adjustment USING btree
    ("ProvNum" ASC NULLS LAST)
    TABLESPACE pg_default;
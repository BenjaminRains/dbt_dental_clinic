-- Table: public.procmultivisit

-- DROP TABLE IF EXISTS public.procmultivisit;

CREATE TABLE IF NOT EXISTS public.procmultivisit
(
    "ProcMultiVisitNum" integer NOT NULL DEFAULT nextval('"procmultivisit_ProcMultiVisitNum_seq"'::regclass),
    "GroupProcMultiVisitNum" bigint,
    "ProcNum" bigint,
    "ProcStatus" smallint,
    "IsInProcess" smallint,
    "SecDateTEntry" timestamp without time zone,
    "SecDateTEdit" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    "PatNum" bigint,
    CONSTRAINT procmultivisit_pkey PRIMARY KEY ("ProcMultiVisitNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.procmultivisit
    OWNER to postgres;
-- Index: procmultivisit_GroupProcMultiVisitNum

-- DROP INDEX IF EXISTS public."procmultivisit_GroupProcMultiVisitNum";

CREATE INDEX IF NOT EXISTS "procmultivisit_GroupProcMultiVisitNum"
    ON public.procmultivisit USING btree
    ("GroupProcMultiVisitNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: procmultivisit_IsInProcess

-- DROP INDEX IF EXISTS public."procmultivisit_IsInProcess";

CREATE INDEX IF NOT EXISTS "procmultivisit_IsInProcess"
    ON public.procmultivisit USING btree
    ("IsInProcess" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: procmultivisit_PatNum

-- DROP INDEX IF EXISTS public."procmultivisit_PatNum";

CREATE INDEX IF NOT EXISTS "procmultivisit_PatNum"
    ON public.procmultivisit USING btree
    ("PatNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: procmultivisit_ProcNum

-- DROP INDEX IF EXISTS public."procmultivisit_ProcNum";

CREATE INDEX IF NOT EXISTS "procmultivisit_ProcNum"
    ON public.procmultivisit USING btree
    ("ProcNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: procmultivisit_SecDateTEdit

-- DROP INDEX IF EXISTS public."procmultivisit_SecDateTEdit";

CREATE INDEX IF NOT EXISTS "procmultivisit_SecDateTEdit"
    ON public.procmultivisit USING btree
    ("SecDateTEdit" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: procmultivisit_SecDateTEntry

-- DROP INDEX IF EXISTS public."procmultivisit_SecDateTEntry";

CREATE INDEX IF NOT EXISTS "procmultivisit_SecDateTEntry"
    ON public.procmultivisit USING btree
    ("SecDateTEntry" ASC NULLS LAST)
    TABLESPACE pg_default;
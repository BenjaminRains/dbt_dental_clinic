-- Table: public.patplan

-- DROP TABLE IF EXISTS public.patplan;

CREATE TABLE IF NOT EXISTS public.patplan
(
    "PatPlanNum" integer NOT NULL DEFAULT nextval('"patplan_PatPlanNum_seq"'::regclass),
    "PatNum" bigint,
    "Ordinal" smallint,
    "IsPending" smallint,
    "Relationship" smallint,
    "PatID" character varying(100) COLLATE pg_catalog."default",
    "InsSubNum" bigint,
    "OrthoAutoFeeBilledOverride" double precision DEFAULT '-1'::integer,
    "OrthoAutoNextClaimDate" date,
    "SecDateTEntry" timestamp without time zone,
    "SecDateTEdit" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT patplan_pkey PRIMARY KEY ("PatPlanNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.patplan
    OWNER to postgres;
-- Index: patplan_InsSubNum

-- DROP INDEX IF EXISTS public."patplan_InsSubNum";

CREATE INDEX IF NOT EXISTS "patplan_InsSubNum"
    ON public.patplan USING btree
    ("InsSubNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: patplan_SecDateTEdit

-- DROP INDEX IF EXISTS public."patplan_SecDateTEdit";

CREATE INDEX IF NOT EXISTS "patplan_SecDateTEdit"
    ON public.patplan USING btree
    ("SecDateTEdit" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: patplan_SecDateTEntry

-- DROP INDEX IF EXISTS public."patplan_SecDateTEntry";

CREATE INDEX IF NOT EXISTS "patplan_SecDateTEntry"
    ON public.patplan USING btree
    ("SecDateTEntry" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: patplan_indexPatNum

-- DROP INDEX IF EXISTS public."patplan_indexPatNum";

CREATE INDEX IF NOT EXISTS "patplan_indexPatNum"
    ON public.patplan USING btree
    ("PatNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Table: public.feesched

-- DROP TABLE IF EXISTS public.feesched;

CREATE TABLE IF NOT EXISTS public.feesched
(
    "FeeSchedNum" integer NOT NULL DEFAULT nextval('"feesched_FeeSchedNum_seq"'::regclass),
    "Description" character varying(255) COLLATE pg_catalog."default",
    "FeeSchedType" integer,
    "ItemOrder" integer,
    "IsHidden" boolean,
    "IsGlobal" smallint,
    "SecUserNumEntry" bigint,
    "SecDateEntry" date,
    "SecDateTEdit" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT feesched_pkey PRIMARY KEY ("FeeSchedNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.feesched
    OWNER to postgres;
-- Index: feesched_SecUserNumEntry

-- DROP INDEX IF EXISTS public."feesched_SecUserNumEntry";

CREATE INDEX IF NOT EXISTS "feesched_SecUserNumEntry"
    ON public.feesched USING btree
    ("SecUserNumEntry" ASC NULLS LAST)
    TABLESPACE pg_default;
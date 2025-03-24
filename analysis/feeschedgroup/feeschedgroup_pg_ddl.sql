-- Table: public.feeschedgroup

-- DROP TABLE IF EXISTS public.feeschedgroup;

CREATE TABLE IF NOT EXISTS public.feeschedgroup
(
    "FeeSchedGroupNum" integer NOT NULL DEFAULT nextval('"feeschedgroup_FeeSchedGroupNum_seq"'::regclass),
    "Description" character varying(255) COLLATE pg_catalog."default",
    "FeeSchedNum" bigint,
    "ClinicNums" character varying(255) COLLATE pg_catalog."default",
    CONSTRAINT feeschedgroup_pkey PRIMARY KEY ("FeeSchedGroupNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.feeschedgroup
    OWNER to postgres;
-- Index: feeschedgroup_FeeSchedNum

-- DROP INDEX IF EXISTS public."feeschedgroup_FeeSchedNum";

CREATE INDEX IF NOT EXISTS "feeschedgroup_FeeSchedNum"
    ON public.feeschedgroup USING btree
    ("FeeSchedNum" ASC NULLS LAST)
    TABLESPACE pg_default;
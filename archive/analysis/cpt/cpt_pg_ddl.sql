-- Table: public.cpt

-- DROP TABLE IF EXISTS public.cpt;

CREATE TABLE IF NOT EXISTS public.cpt
(
    "CptNum" integer NOT NULL DEFAULT nextval('"cpt_CptNum_seq"'::regclass),
    "CptCode" character varying(255) COLLATE pg_catalog."default",
    "Description" character varying(4000) COLLATE pg_catalog."default",
    "VersionIDs" character varying(255) COLLATE pg_catalog."default",
    CONSTRAINT cpt_pkey PRIMARY KEY ("CptNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.cpt
    OWNER to postgres;
-- Index: cpt_CptCode

-- DROP INDEX IF EXISTS public."cpt_CptCode";

CREATE INDEX IF NOT EXISTS "cpt_CptCode"
    ON public.cpt USING btree
    ("CptCode" COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Table: public.rxdef

-- DROP TABLE IF EXISTS public.rxdef;

CREATE TABLE IF NOT EXISTS public.rxdef
(
    "RxDefNum" integer NOT NULL DEFAULT nextval('"rxdef_RxDefNum_seq"'::regclass),
    "Drug" character varying(255) COLLATE pg_catalog."default",
    "Sig" character varying(255) COLLATE pg_catalog."default",
    "Disp" character varying(255) COLLATE pg_catalog."default",
    "Refills" character varying(30) COLLATE pg_catalog."default",
    "Notes" character varying(255) COLLATE pg_catalog."default",
    "IsControlled" smallint,
    "RxCui" bigint,
    "IsProcRequired" smallint,
    "PatientInstruction" text COLLATE pg_catalog."default",
    CONSTRAINT rxdef_pkey PRIMARY KEY ("RxDefNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.rxdef
    OWNER to postgres;
-- Index: rxdef_RxCui

-- DROP INDEX IF EXISTS public."rxdef_RxCui";

CREATE INDEX IF NOT EXISTS "rxdef_RxCui"
    ON public.rxdef USING btree
    ("RxCui" ASC NULLS LAST)
    TABLESPACE pg_default;
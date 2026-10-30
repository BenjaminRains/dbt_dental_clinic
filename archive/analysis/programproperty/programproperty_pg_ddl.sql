-- Table: public.programproperty

-- DROP TABLE IF EXISTS public.programproperty;

CREATE TABLE IF NOT EXISTS public.programproperty
(
    "ProgramPropertyNum" integer NOT NULL DEFAULT nextval('"programproperty_ProgramPropertyNum_seq"'::regclass),
    "ProgramNum" bigint,
    "PropertyDesc" character varying(255) COLLATE pg_catalog."default",
    "PropertyValue" text COLLATE pg_catalog."default",
    "ComputerName" character varying(255) COLLATE pg_catalog."default",
    "ClinicNum" bigint,
    "IsMasked" smallint,
    "IsHighSecurity" smallint,
    CONSTRAINT programproperty_pkey PRIMARY KEY ("ProgramPropertyNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.programproperty
    OWNER to postgres;
-- Index: programproperty_ClinicNum

-- DROP INDEX IF EXISTS public."programproperty_ClinicNum";

CREATE INDEX IF NOT EXISTS "programproperty_ClinicNum"
    ON public.programproperty USING btree
    ("ClinicNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Table: public.diseasedef

-- DROP TABLE IF EXISTS public.diseasedef;

CREATE TABLE IF NOT EXISTS public.diseasedef
(
    "DiseaseDefNum" integer NOT NULL DEFAULT nextval('"diseasedef_DiseaseDefNum_seq"'::regclass),
    "DiseaseName" character varying(255) COLLATE pg_catalog."default",
    "ItemOrder" smallint,
    "IsHidden" smallint,
    "DateTStamp" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    "ICD9Code" character varying(255) COLLATE pg_catalog."default",
    "SnomedCode" character varying(255) COLLATE pg_catalog."default",
    "Icd10Code" character varying(255) COLLATE pg_catalog."default",
    CONSTRAINT diseasedef_pkey PRIMARY KEY ("DiseaseDefNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.diseasedef
    OWNER to postgres;
-- Index: diseasedef_ICD9Code

-- DROP INDEX IF EXISTS public."diseasedef_ICD9Code";

CREATE INDEX IF NOT EXISTS "diseasedef_ICD9Code"
    ON public.diseasedef USING btree
    ("ICD9Code" COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: diseasedef_Icd10Code

-- DROP INDEX IF EXISTS public."diseasedef_Icd10Code";

CREATE INDEX IF NOT EXISTS "diseasedef_Icd10Code"
    ON public.diseasedef USING btree
    ("Icd10Code" COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: diseasedef_SnomedCode

-- DROP INDEX IF EXISTS public."diseasedef_SnomedCode";

CREATE INDEX IF NOT EXISTS "diseasedef_SnomedCode"
    ON public.diseasedef USING btree
    ("SnomedCode" COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;
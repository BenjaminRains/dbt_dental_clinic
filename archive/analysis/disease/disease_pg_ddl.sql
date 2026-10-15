-- Table: public.disease

-- DROP TABLE IF EXISTS public.disease;

CREATE TABLE IF NOT EXISTS public.disease
(
    "DiseaseNum" integer NOT NULL DEFAULT nextval('"disease_DiseaseNum_seq"'::regclass),
    "PatNum" bigint,
    "DiseaseDefNum" bigint,
    "PatNote" text COLLATE pg_catalog."default",
    "DateTStamp" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    "ProbStatus" smallint,
    "DateStart" date,
    "DateStop" date,
    "SnomedProblemType" character varying(255) COLLATE pg_catalog."default",
    "FunctionStatus" smallint,
    CONSTRAINT disease_pkey PRIMARY KEY ("DiseaseNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.disease
    OWNER to postgres;
-- Index: disease_DiseaseDefNum

-- DROP INDEX IF EXISTS public."disease_DiseaseDefNum";

CREATE INDEX IF NOT EXISTS "disease_DiseaseDefNum"
    ON public.disease USING btree
    ("DiseaseDefNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: disease_indexPatNum

-- DROP INDEX IF EXISTS public."disease_indexPatNum";

CREATE INDEX IF NOT EXISTS "disease_indexPatNum"
    ON public.disease USING btree
    ("PatNum" ASC NULLS LAST)
    TABLESPACE pg_default;
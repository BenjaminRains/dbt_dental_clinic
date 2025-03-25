-- Table: public.perioexam

-- DROP TABLE IF EXISTS public.perioexam;

CREATE TABLE IF NOT EXISTS public.perioexam
(
    "PerioExamNum" integer NOT NULL DEFAULT nextval('"perioexam_PerioExamNum_seq"'::regclass),
    "PatNum" bigint,
    "ExamDate" date,
    "ProvNum" bigint,
    "DateTMeasureEdit" timestamp without time zone,
    "Note" text COLLATE pg_catalog."default",
    CONSTRAINT perioexam_pkey PRIMARY KEY ("PerioExamNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.perioexam
    OWNER to postgres;
-- Index: perioexam_PatNum

-- DROP INDEX IF EXISTS public."perioexam_PatNum";

CREATE INDEX IF NOT EXISTS "perioexam_PatNum"
    ON public.perioexam USING btree
    ("PatNum" ASC NULLS LAST)
    TABLESPACE pg_default;
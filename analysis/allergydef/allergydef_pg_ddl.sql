-- Table: public.allergydef

-- DROP TABLE IF EXISTS public.allergydef;

CREATE TABLE IF NOT EXISTS public.allergydef
(
    "AllergyDefNum" integer NOT NULL DEFAULT nextval('"allergydef_AllergyDefNum_seq"'::regclass),
    "Description" character varying(255) COLLATE pg_catalog."default",
    "IsHidden" smallint,
    "DateTStamp" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    "SnomedType" smallint,
    "MedicationNum" bigint,
    "UniiCode" character varying(255) COLLATE pg_catalog."default",
    CONSTRAINT allergydef_pkey PRIMARY KEY ("AllergyDefNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.allergydef
    OWNER to postgres;
-- Index: allergydef_MedicationNum

-- DROP INDEX IF EXISTS public."allergydef_MedicationNum";

CREATE INDEX IF NOT EXISTS "allergydef_MedicationNum"
    ON public.allergydef USING btree
    ("MedicationNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Table: public.allergy

-- DROP TABLE IF EXISTS public.allergy;

CREATE TABLE IF NOT EXISTS public.allergy
(
    "AllergyNum" integer NOT NULL DEFAULT nextval('"allergy_AllergyNum_seq"'::regclass),
    "AllergyDefNum" bigint,
    "PatNum" bigint,
    "Reaction" character varying(255) COLLATE pg_catalog."default",
    "StatusIsActive" smallint,
    "DateTStamp" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    "DateAdverseReaction" date,
    "SnomedReaction" character varying(255) COLLATE pg_catalog."default",
    CONSTRAINT allergy_pkey PRIMARY KEY ("AllergyNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.allergy
    OWNER to postgres;
-- Index: allergy_AllergyDefNum

-- DROP INDEX IF EXISTS public."allergy_AllergyDefNum";

CREATE INDEX IF NOT EXISTS "allergy_AllergyDefNum"
    ON public.allergy USING btree
    ("AllergyDefNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: allergy_PatNum

-- DROP INDEX IF EXISTS public."allergy_PatNum";

CREATE INDEX IF NOT EXISTS "allergy_PatNum"
    ON public.allergy USING btree
    ("PatNum" ASC NULLS LAST)
    TABLESPACE pg_default;
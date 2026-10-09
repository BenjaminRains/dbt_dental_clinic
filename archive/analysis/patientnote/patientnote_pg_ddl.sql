-- Table: public.patientnote

-- DROP TABLE IF EXISTS public.patientnote;

CREATE TABLE IF NOT EXISTS public.patientnote
(
    "PatNum" bigint NOT NULL,
    "FamFinancial" text COLLATE pg_catalog."default",
    "ApptPhone" text COLLATE pg_catalog."default",
    "Medical" text COLLATE pg_catalog."default",
    "Service" text COLLATE pg_catalog."default",
    "MedicalComp" text COLLATE pg_catalog."default",
    "Treatment" text COLLATE pg_catalog."default",
    "ICEName" character varying(255) COLLATE pg_catalog."default",
    "ICEPhone" character varying(30) COLLATE pg_catalog."default",
    "OrthoMonthsTreatOverride" integer DEFAULT '-1'::integer,
    "DateOrthoPlacementOverride" date,
    "SecDateTEntry" timestamp without time zone,
    "SecDateTEdit" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    "Consent" smallint,
    "UserNumOrthoLocked" bigint,
    "Pronoun" smallint,
    CONSTRAINT patientnote_pkey PRIMARY KEY ("PatNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.patientnote
    OWNER to postgres;
-- Index: patientnote_SecDateTEdit

-- DROP INDEX IF EXISTS public."patientnote_SecDateTEdit";

CREATE INDEX IF NOT EXISTS "patientnote_SecDateTEdit"
    ON public.patientnote USING btree
    ("SecDateTEdit" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: patientnote_SecDateTEntry

-- DROP INDEX IF EXISTS public."patientnote_SecDateTEntry";

CREATE INDEX IF NOT EXISTS "patientnote_SecDateTEntry"
    ON public.patientnote USING btree
    ("SecDateTEntry" ASC NULLS LAST)
    TABLESPACE pg_default;
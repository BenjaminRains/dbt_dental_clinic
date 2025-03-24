-- Table: public.patientlink

-- DROP TABLE IF EXISTS public.patientlink;

CREATE TABLE IF NOT EXISTS public.patientlink
(
    "PatientLinkNum" integer NOT NULL DEFAULT nextval('"patientlink_PatientLinkNum_seq"'::regclass),
    "PatNumFrom" bigint,
    "PatNumTo" bigint,
    "LinkType" smallint,
    "DateTimeLink" timestamp without time zone,
    CONSTRAINT patientlink_pkey PRIMARY KEY ("PatientLinkNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.patientlink
    OWNER to postgres;
-- Index: patientlink_PatNumFrom

-- DROP INDEX IF EXISTS public."patientlink_PatNumFrom";

CREATE INDEX IF NOT EXISTS "patientlink_PatNumFrom"
    ON public.patientlink USING btree
    ("PatNumFrom" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: patientlink_PatNumTo

-- DROP INDEX IF EXISTS public."patientlink_PatNumTo";

CREATE INDEX IF NOT EXISTS "patientlink_PatNumTo"
    ON public.patientlink USING btree
    ("PatNumTo" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Table: public.labcase

-- DROP TABLE IF EXISTS public.labcase;

CREATE TABLE IF NOT EXISTS public.labcase
(
    "LabCaseNum" integer NOT NULL DEFAULT nextval('"labcase_LabCaseNum_seq"'::regclass),
    "PatNum" bigint,
    "LaboratoryNum" bigint,
    "AptNum" bigint,
    "PlannedAptNum" bigint,
    "DateTimeDue" timestamp without time zone,
    "DateTimeCreated" timestamp without time zone,
    "DateTimeSent" timestamp without time zone,
    "DateTimeRecd" timestamp without time zone,
    "DateTimeChecked" timestamp without time zone,
    "ProvNum" bigint,
    "Instructions" text COLLATE pg_catalog."default",
    "LabFee" double precision,
    "DateTStamp" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    "InvoiceNum" character varying(255) COLLATE pg_catalog."default",
    CONSTRAINT labcase_pkey PRIMARY KEY ("LabCaseNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.labcase
    OWNER to postgres;
-- Index: labcase_indexAptNum

-- DROP INDEX IF EXISTS public."labcase_indexAptNum";

CREATE INDEX IF NOT EXISTS "labcase_indexAptNum"
    ON public.labcase USING btree
    ("AptNum" ASC NULLS LAST)
    TABLESPACE pg_default;
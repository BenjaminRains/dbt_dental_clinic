-- Table: public.rxpat

-- DROP TABLE IF EXISTS public.rxpat;

CREATE TABLE IF NOT EXISTS public.rxpat
(
    "RxNum" integer NOT NULL DEFAULT nextval('"rxpat_RxNum_seq"'::regclass),
    "PatNum" bigint,
    "RxDate" date,
    "Drug" character varying(255) COLLATE pg_catalog."default",
    "Sig" character varying(255) COLLATE pg_catalog."default",
    "Disp" character varying(255) COLLATE pg_catalog."default",
    "Refills" character varying(30) COLLATE pg_catalog."default",
    "ProvNum" bigint,
    "Notes" character varying(255) COLLATE pg_catalog."default",
    "PharmacyNum" bigint,
    "IsControlled" smallint,
    "DateTStamp" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    "SendStatus" smallint,
    "RxCui" bigint,
    "DosageCode" character varying(255) COLLATE pg_catalog."default",
    "ErxGuid" character varying(40) COLLATE pg_catalog."default",
    "IsErxOld" smallint,
    "ErxPharmacyInfo" character varying(255) COLLATE pg_catalog."default",
    "IsProcRequired" smallint,
    "ProcNum" bigint,
    "DaysOfSupply" double precision,
    "PatientInstruction" text COLLATE pg_catalog."default",
    "ClinicNum" bigint,
    "UserNum" bigint,
    "RxType" smallint,
    CONSTRAINT rxpat_pkey PRIMARY KEY ("RxNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.rxpat
    OWNER to postgres;
-- Index: rxpat_ClinicNum

-- DROP INDEX IF EXISTS public."rxpat_ClinicNum";

CREATE INDEX IF NOT EXISTS "rxpat_ClinicNum"
    ON public.rxpat USING btree
    ("ClinicNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: rxpat_PatNumRxType

-- DROP INDEX IF EXISTS public."rxpat_PatNumRxType";

CREATE INDEX IF NOT EXISTS "rxpat_PatNumRxType"
    ON public.rxpat USING btree
    ("PatNum" ASC NULLS LAST, "RxType" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: rxpat_ProcNum

-- DROP INDEX IF EXISTS public."rxpat_ProcNum";

CREATE INDEX IF NOT EXISTS "rxpat_ProcNum"
    ON public.rxpat USING btree
    ("ProcNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: rxpat_ProvNum

-- DROP INDEX IF EXISTS public."rxpat_ProvNum";

CREATE INDEX IF NOT EXISTS "rxpat_ProvNum"
    ON public.rxpat USING btree
    ("ProvNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: rxpat_RxCui

-- DROP INDEX IF EXISTS public."rxpat_RxCui";

CREATE INDEX IF NOT EXISTS "rxpat_RxCui"
    ON public.rxpat USING btree
    ("RxCui" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: rxpat_UserNum

-- DROP INDEX IF EXISTS public."rxpat_UserNum";

CREATE INDEX IF NOT EXISTS "rxpat_UserNum"
    ON public.rxpat USING btree
    ("UserNum" ASC NULLS LAST)
    TABLESPACE pg_default;
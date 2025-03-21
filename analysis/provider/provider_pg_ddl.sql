-- Table: public.provider

-- DROP TABLE IF EXISTS public.provider;

CREATE TABLE IF NOT EXISTS public.provider
(
    "ProvNum" integer NOT NULL DEFAULT nextval('"provider_ProvNum_seq"'::regclass),
    "Abbr" character varying(255) COLLATE pg_catalog."default",
    "ItemOrder" smallint DEFAULT 0,
    "LName" character varying(100) COLLATE pg_catalog."default",
    "FName" character varying(100) COLLATE pg_catalog."default",
    "MI" character varying(100) COLLATE pg_catalog."default",
    "Suffix" character varying(100) COLLATE pg_catalog."default",
    "FeeSched" bigint,
    "Specialty" bigint,
    "SSN" character varying(12) COLLATE pg_catalog."default",
    "StateLicense" character varying(15) COLLATE pg_catalog."default",
    "DEANum" character varying(15) COLLATE pg_catalog."default",
    "IsSecondary" smallint DEFAULT 0,
    "ProvColor" integer DEFAULT 0,
    "IsHidden" smallint DEFAULT 0,
    "UsingTIN" smallint DEFAULT 0,
    "BlueCrossID" character varying(25) COLLATE pg_catalog."default",
    "SigOnFile" smallint DEFAULT 1,
    "MedicaidID" character varying(20) COLLATE pg_catalog."default",
    "OutlineColor" integer DEFAULT 0,
    "SchoolClassNum" bigint,
    "NationalProvID" character varying(255) COLLATE pg_catalog."default",
    "CanadianOfficeNum" character varying(100) COLLATE pg_catalog."default",
    "DateTStamp" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    "AnesthProvType" bigint,
    "TaxonomyCodeOverride" character varying(255) COLLATE pg_catalog."default",
    "IsCDAnet" smallint,
    "EcwID" character varying(255) COLLATE pg_catalog."default",
    "StateRxID" character varying(255) COLLATE pg_catalog."default",
    "IsNotPerson" smallint,
    "StateWhereLicensed" character varying(50) COLLATE pg_catalog."default",
    "EmailAddressNum" bigint,
    "IsInstructor" smallint,
    "EhrMuStage" integer,
    "ProvNumBillingOverride" bigint,
    "CustomID" character varying(255) COLLATE pg_catalog."default",
    "ProvStatus" smallint,
    "IsHiddenReport" smallint,
    "IsErxEnabled" smallint,
    "Birthdate" date,
    "SchedNote" character varying(255) COLLATE pg_catalog."default",
    "WebSchedDescript" character varying(500) COLLATE pg_catalog."default",
    "WebSchedImageLocation" character varying(255) COLLATE pg_catalog."default",
    "HourlyProdGoalAmt" double precision,
    "DateTerm" date,
    "PreferredName" character varying(100) COLLATE pg_catalog."default",
    CONSTRAINT provider_pkey PRIMARY KEY ("ProvNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.provider
    OWNER to postgres;
-- Index: provider_EmailAddressNum

-- DROP INDEX IF EXISTS public."provider_EmailAddressNum";

CREATE INDEX IF NOT EXISTS "provider_EmailAddressNum"
    ON public.provider USING btree
    ("EmailAddressNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: provider_FeeSched

-- DROP INDEX IF EXISTS public."provider_FeeSched";

CREATE INDEX IF NOT EXISTS "provider_FeeSched"
    ON public.provider USING btree
    ("FeeSched" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: provider_IDX_TEMPPROVIDER_ABBR

-- DROP INDEX IF EXISTS public."provider_IDX_TEMPPROVIDER_ABBR";

CREATE INDEX IF NOT EXISTS "provider_IDX_TEMPPROVIDER_ABBR"
    ON public.provider USING btree
    ("Abbr" COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: provider_ProvNumBillingOverride

-- DROP INDEX IF EXISTS public."provider_ProvNumBillingOverride";

CREATE INDEX IF NOT EXISTS "provider_ProvNumBillingOverride"
    ON public.provider USING btree
    ("ProvNumBillingOverride" ASC NULLS LAST)
    TABLESPACE pg_default;
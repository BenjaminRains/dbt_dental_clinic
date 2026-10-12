-- Table: public.carrier

-- DROP TABLE IF EXISTS public.carrier;

CREATE TABLE IF NOT EXISTS public.carrier
(
    "CarrierNum" integer NOT NULL DEFAULT nextval('"carrier_CarrierNum_seq"'::regclass),
    "CarrierName" character varying(255) COLLATE pg_catalog."default",
    "Address" character varying(255) COLLATE pg_catalog."default",
    "Address2" character varying(255) COLLATE pg_catalog."default",
    "City" character varying(255) COLLATE pg_catalog."default",
    "State" character varying(255) COLLATE pg_catalog."default",
    "Zip" character varying(255) COLLATE pg_catalog."default",
    "Phone" character varying(255) COLLATE pg_catalog."default",
    "ElectID" character varying(255) COLLATE pg_catalog."default",
    "NoSendElect" smallint DEFAULT 0,
    "IsCDA" smallint,
    "CDAnetVersion" character varying(100) COLLATE pg_catalog."default",
    "CanadianNetworkNum" bigint,
    "IsHidden" smallint,
    "CanadianEncryptionMethod" smallint,
    "CanadianSupportedTypes" integer,
    "SecUserNumEntry" bigint,
    "SecDateEntry" date,
    "SecDateTEdit" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    "TIN" character varying(255) COLLATE pg_catalog."default",
    "CarrierGroupName" bigint,
    "ApptTextBackColor" integer,
    "IsCoinsuranceInverted" smallint,
    "TrustedEtransFlags" smallint,
    "CobInsPaidBehaviorOverride" smallint,
    "EraAutomationOverride" smallint,
    "OrthoInsPayConsolidate" smallint,
    CONSTRAINT carrier_pkey PRIMARY KEY ("CarrierNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.carrier
    OWNER to postgres;
-- Index: carrier_CanadianNetworkNum

-- DROP INDEX IF EXISTS public."carrier_CanadianNetworkNum";

CREATE INDEX IF NOT EXISTS "carrier_CanadianNetworkNum"
    ON public.carrier USING btree
    ("CanadianNetworkNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: carrier_CarrierGroupName

-- DROP INDEX IF EXISTS public."carrier_CarrierGroupName";

CREATE INDEX IF NOT EXISTS "carrier_CarrierGroupName"
    ON public.carrier USING btree
    ("CarrierGroupName" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: carrier_CarrierNumName

-- DROP INDEX IF EXISTS public."carrier_CarrierNumName";

CREATE INDEX IF NOT EXISTS "carrier_CarrierNumName"
    ON public.carrier USING btree
    ("CarrierNum" ASC NULLS LAST, "CarrierName" COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: carrier_SecUserNumEntry

-- DROP INDEX IF EXISTS public."carrier_SecUserNumEntry";

CREATE INDEX IF NOT EXISTS "carrier_SecUserNumEntry"
    ON public.carrier USING btree
    ("SecUserNumEntry" ASC NULLS LAST)
    TABLESPACE pg_default;
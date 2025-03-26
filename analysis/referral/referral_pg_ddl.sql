-- Table: public.referral

-- DROP TABLE IF EXISTS public.referral;

CREATE TABLE IF NOT EXISTS public.referral
(
    "ReferralNum" integer NOT NULL DEFAULT nextval('"referral_ReferralNum_seq"'::regclass),
    "LName" character varying(100) COLLATE pg_catalog."default",
    "FName" character varying(100) COLLATE pg_catalog."default",
    "MName" character varying(100) COLLATE pg_catalog."default",
    "SSN" character varying(9) COLLATE pg_catalog."default",
    "UsingTIN" smallint DEFAULT 0,
    "Specialty" bigint,
    "ST" character varying(2) COLLATE pg_catalog."default",
    "Telephone" character varying(10) COLLATE pg_catalog."default",
    "Address" character varying(100) COLLATE pg_catalog."default",
    "Address2" character varying(100) COLLATE pg_catalog."default",
    "City" character varying(100) COLLATE pg_catalog."default",
    "Zip" character varying(10) COLLATE pg_catalog."default",
    "Note" text COLLATE pg_catalog."default",
    "Phone2" character varying(30) COLLATE pg_catalog."default",
    "IsHidden" smallint DEFAULT 0,
    "NotPerson" smallint DEFAULT 0,
    "Title" character varying(255) COLLATE pg_catalog."default",
    "EMail" character varying(255) COLLATE pg_catalog."default",
    "PatNum" bigint,
    "NationalProvID" character varying(255) COLLATE pg_catalog."default",
    "Slip" bigint,
    "IsDoctor" smallint,
    "IsTrustedDirect" smallint,
    "DateTStamp" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    "IsPreferred" smallint,
    "BusinessName" character varying(255) COLLATE pg_catalog."default",
    "DisplayNote" character varying(4000) COLLATE pg_catalog."default",
    CONSTRAINT referral_pkey PRIMARY KEY ("ReferralNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.referral
    OWNER to postgres;
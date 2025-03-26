-- Table: public.pharmacy

-- DROP TABLE IF EXISTS public.pharmacy;

CREATE TABLE IF NOT EXISTS public.pharmacy
(
    "PharmacyNum" integer NOT NULL DEFAULT nextval('"pharmacy_PharmacyNum_seq"'::regclass),
    "PharmID" character varying(255) COLLATE pg_catalog."default",
    "StoreName" character varying(255) COLLATE pg_catalog."default",
    "Phone" character varying(255) COLLATE pg_catalog."default",
    "Fax" character varying(255) COLLATE pg_catalog."default",
    "Address" character varying(255) COLLATE pg_catalog."default",
    "Address2" character varying(255) COLLATE pg_catalog."default",
    "City" character varying(255) COLLATE pg_catalog."default",
    "State" character varying(255) COLLATE pg_catalog."default",
    "Zip" character varying(255) COLLATE pg_catalog."default",
    "Note" text COLLATE pg_catalog."default",
    "DateTStamp" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pharmacy_pkey PRIMARY KEY ("PharmacyNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.pharmacy
    OWNER to postgres;
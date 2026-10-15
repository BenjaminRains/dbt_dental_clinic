-- Table: public.zipcode

-- DROP TABLE IF EXISTS public.zipcode;

CREATE TABLE IF NOT EXISTS public.zipcode
(
    "ZipCodeNum" integer NOT NULL DEFAULT nextval('"zipcode_ZipCodeNum_seq"'::regclass),
    "ZipCodeDigits" character varying(20) COLLATE pg_catalog."default",
    "City" character varying(100) COLLATE pg_catalog."default",
    "State" character varying(20) COLLATE pg_catalog."default",
    "IsFrequent" smallint DEFAULT 0,
    CONSTRAINT zipcode_pkey PRIMARY KEY ("ZipCodeNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.zipcode
    OWNER to postgres;
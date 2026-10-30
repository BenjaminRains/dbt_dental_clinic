-- Table: public.rxnorm

-- DROP TABLE IF EXISTS public.rxnorm;

CREATE TABLE IF NOT EXISTS public.rxnorm
(
    "RxNormNum" integer NOT NULL DEFAULT nextval('"rxnorm_RxNormNum_seq"'::regclass),
    "RxCui" character varying(255) COLLATE pg_catalog."default",
    "MmslCode" character varying(255) COLLATE pg_catalog."default",
    "Description" text COLLATE pg_catalog."default",
    CONSTRAINT rxnorm_pkey PRIMARY KEY ("RxNormNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.rxnorm
    OWNER to postgres;
-- Index: rxnorm_RxCui

-- DROP INDEX IF EXISTS public."rxnorm_RxCui";

CREATE INDEX IF NOT EXISTS "rxnorm_RxCui"
    ON public.rxnorm USING btree
    ("RxCui" COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;
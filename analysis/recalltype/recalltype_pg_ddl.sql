-- Table: public.recalltype

-- DROP TABLE IF EXISTS public.recalltype;

CREATE TABLE IF NOT EXISTS public.recalltype
(
    "RecallTypeNum" integer NOT NULL DEFAULT nextval('"recalltype_RecallTypeNum_seq"'::regclass),
    "Description" character varying(255) COLLATE pg_catalog."default",
    "DefaultInterval" integer,
    "TimePattern" character varying(255) COLLATE pg_catalog."default",
    "Procedures" character varying(255) COLLATE pg_catalog."default",
    "AppendToSpecial" smallint,
    CONSTRAINT recalltype_pkey PRIMARY KEY ("RecallTypeNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.recalltype
    OWNER to postgres;
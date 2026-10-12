-- Table: public.autocode

-- DROP TABLE IF EXISTS public.autocode;

CREATE TABLE IF NOT EXISTS public.autocode
(
    "AutoCodeNum" integer NOT NULL DEFAULT nextval('"autocode_AutoCodeNum_seq"'::regclass),
    "Description" character varying(255) COLLATE pg_catalog."default",
    "IsHidden" smallint DEFAULT 0,
    "LessIntrusive" smallint DEFAULT 0,
    CONSTRAINT autocode_pkey PRIMARY KEY ("AutoCodeNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.autocode
    OWNER to postgres;
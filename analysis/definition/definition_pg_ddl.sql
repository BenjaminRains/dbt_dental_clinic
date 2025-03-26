-- Table: public.definition

-- DROP TABLE IF EXISTS public.definition;

CREATE TABLE IF NOT EXISTS public.definition
(
    "DefNum" integer NOT NULL DEFAULT nextval('"definition_DefNum_seq"'::regclass),
    "Category" smallint DEFAULT 0,
    "ItemOrder" smallint DEFAULT 0,
    "ItemName" character varying(255) COLLATE pg_catalog."default",
    "ItemValue" character varying(255) COLLATE pg_catalog."default",
    "ItemColor" integer DEFAULT 0,
    "IsHidden" smallint DEFAULT 0,
    CONSTRAINT definition_pkey PRIMARY KEY ("DefNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.definition
    OWNER to postgres;
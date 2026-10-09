-- Table: public.codegroup

-- DROP TABLE IF EXISTS public.codegroup;

CREATE TABLE IF NOT EXISTS public.codegroup
(
    "CodeGroupNum" integer NOT NULL DEFAULT nextval('"codegroup_CodeGroupNum_seq"'::regclass),
    "GroupName" character varying(50) COLLATE pg_catalog."default",
    "ProcCodes" text COLLATE pg_catalog."default",
    "ItemOrder" integer,
    "CodeGroupFixed" smallint,
    "IsHidden" smallint,
    "ShowInAgeLimit" smallint,
    CONSTRAINT codegroup_pkey PRIMARY KEY ("CodeGroupNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.codegroup
    OWNER to postgres;
-- Table: public.program

-- DROP TABLE IF EXISTS public.program;

CREATE TABLE IF NOT EXISTS public.program
(
    "ProgramNum" integer NOT NULL DEFAULT nextval('"program_ProgramNum_seq"'::regclass),
    "ProgName" character varying(100) COLLATE pg_catalog."default",
    "ProgDesc" character varying(100) COLLATE pg_catalog."default",
    "Enabled" smallint DEFAULT 0,
    "Path" text COLLATE pg_catalog."default",
    "CommandLine" text COLLATE pg_catalog."default",
    "Note" text COLLATE pg_catalog."default",
    "PluginDllName" character varying(255) COLLATE pg_catalog."default",
    "ButtonImage" text COLLATE pg_catalog."default",
    "FileTemplate" text COLLATE pg_catalog."default",
    "FilePath" character varying(255) COLLATE pg_catalog."default",
    "IsDisabledByHq" smallint,
    "CustErr" character varying(255) COLLATE pg_catalog."default",
    CONSTRAINT program_pkey PRIMARY KEY ("ProgramNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.program
    OWNER to postgres;
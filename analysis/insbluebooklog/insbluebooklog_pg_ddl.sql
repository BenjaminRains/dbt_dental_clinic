-- Table: public.insbluebooklog

-- DROP TABLE IF EXISTS public.insbluebooklog;

CREATE TABLE IF NOT EXISTS public.insbluebooklog
(
    "InsBlueBookLogNum" integer NOT NULL DEFAULT nextval('"insbluebooklog_InsBlueBookLogNum_seq"'::regclass),
    "ClaimProcNum" bigint,
    "AllowedFee" double precision,
    "DateTEntry" timestamp without time zone,
    "Description" text COLLATE pg_catalog."default",
    CONSTRAINT insbluebooklog_pkey PRIMARY KEY ("InsBlueBookLogNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.insbluebooklog
    OWNER to postgres;
-- Index: insbluebooklog_ClaimProcNum

-- DROP INDEX IF EXISTS public."insbluebooklog_ClaimProcNum";

CREATE INDEX IF NOT EXISTS "insbluebooklog_ClaimProcNum"
    ON public.insbluebooklog USING btree
    ("ClaimProcNum" ASC NULLS LAST)
    TABLESPACE pg_default;
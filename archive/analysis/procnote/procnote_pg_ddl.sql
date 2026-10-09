-- Table: public.procnote

-- DROP TABLE IF EXISTS public.procnote;

CREATE TABLE IF NOT EXISTS public.procnote
(
    "ProcNoteNum" integer NOT NULL DEFAULT nextval('"procnote_ProcNoteNum_seq"'::regclass),
    "PatNum" bigint,
    "ProcNum" bigint,
    "EntryDateTime" timestamp without time zone,
    "UserNum" bigint,
    "Note" text COLLATE pg_catalog."default",
    "SigIsTopaz" smallint,
    "Signature" text COLLATE pg_catalog."default",
    CONSTRAINT procnote_pkey PRIMARY KEY ("ProcNoteNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.procnote
    OWNER to postgres;
-- Index: procnote_PatNum

-- DROP INDEX IF EXISTS public."procnote_PatNum";

CREATE INDEX IF NOT EXISTS "procnote_PatNum"
    ON public.procnote USING btree
    ("PatNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: procnote_ProcNum

-- DROP INDEX IF EXISTS public."procnote_ProcNum";

CREATE INDEX IF NOT EXISTS "procnote_ProcNum"
    ON public.procnote USING btree
    ("ProcNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: procnote_UserNum

-- DROP INDEX IF EXISTS public."procnote_UserNum";

CREATE INDEX IF NOT EXISTS "procnote_UserNum"
    ON public.procnote USING btree
    ("UserNum" ASC NULLS LAST)
    TABLESPACE pg_default;
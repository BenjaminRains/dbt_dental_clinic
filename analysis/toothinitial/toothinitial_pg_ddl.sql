-- Table: public.toothinitial

-- DROP TABLE IF EXISTS public.toothinitial;

CREATE TABLE IF NOT EXISTS public.toothinitial
(
    "ToothInitialNum" integer NOT NULL DEFAULT nextval('"toothinitial_ToothInitialNum_seq"'::regclass),
    "PatNum" bigint,
    "ToothNum" character varying(2) COLLATE pg_catalog."default",
    "InitialType" smallint,
    "Movement" real,
    "DrawingSegment" text COLLATE pg_catalog."default",
    "ColorDraw" integer,
    "SecDateTEntry" timestamp without time zone,
    "SecDateTEdit" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    "DrawText" character varying(255) COLLATE pg_catalog."default",
    CONSTRAINT toothinitial_pkey PRIMARY KEY ("ToothInitialNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.toothinitial
    OWNER to postgres;
-- Index: toothinitial_PatNum

-- DROP INDEX IF EXISTS public."toothinitial_PatNum";

CREATE INDEX IF NOT EXISTS "toothinitial_PatNum"
    ON public.toothinitial USING btree
    ("PatNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: toothinitial_SecDateTEdit

-- DROP INDEX IF EXISTS public."toothinitial_SecDateTEdit";

CREATE INDEX IF NOT EXISTS "toothinitial_SecDateTEdit"
    ON public.toothinitial USING btree
    ("SecDateTEdit" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: toothinitial_SecDateTEntry

-- DROP INDEX IF EXISTS public."toothinitial_SecDateTEntry";

CREATE INDEX IF NOT EXISTS "toothinitial_SecDateTEntry"
    ON public.toothinitial USING btree
    ("SecDateTEntry" ASC NULLS LAST)
    TABLESPACE pg_default;
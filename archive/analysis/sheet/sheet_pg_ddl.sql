-- Table: public.sheet

-- DROP TABLE IF EXISTS public.sheet;

CREATE TABLE IF NOT EXISTS public.sheet
(
    "SheetNum" integer NOT NULL DEFAULT nextval('"sheet_SheetNum_seq"'::regclass),
    "SheetType" integer,
    "PatNum" bigint,
    "DateTimeSheet" timestamp without time zone,
    "FontSize" real,
    "FontName" character varying(255) COLLATE pg_catalog."default",
    "Width" integer,
    "Height" integer,
    "IsLandscape" smallint,
    "InternalNote" text COLLATE pg_catalog."default",
    "Description" character varying(255) COLLATE pg_catalog."default",
    "ShowInTerminal" smallint,
    "IsWebForm" smallint,
    "IsMultiPage" smallint,
    "IsDeleted" smallint,
    "SheetDefNum" bigint,
    "DocNum" bigint,
    "ClinicNum" bigint,
    "DateTSheetEdited" timestamp without time zone,
    "HasMobileLayout" smallint,
    "RevID" integer,
    "WebFormSheetID" bigint,
    CONSTRAINT sheet_pkey PRIMARY KEY ("SheetNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.sheet
    OWNER to postgres;
-- Index: sheet_ClinicNum

-- DROP INDEX IF EXISTS public."sheet_ClinicNum";

CREATE INDEX IF NOT EXISTS "sheet_ClinicNum"
    ON public.sheet USING btree
    ("ClinicNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: sheet_DocNum

-- DROP INDEX IF EXISTS public."sheet_DocNum";

CREATE INDEX IF NOT EXISTS "sheet_DocNum"
    ON public.sheet USING btree
    ("DocNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: sheet_PatNum

-- DROP INDEX IF EXISTS public."sheet_PatNum";

CREATE INDEX IF NOT EXISTS "sheet_PatNum"
    ON public.sheet USING btree
    ("PatNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: sheet_SheetDefNum

-- DROP INDEX IF EXISTS public."sheet_SheetDefNum";

CREATE INDEX IF NOT EXISTS "sheet_SheetDefNum"
    ON public.sheet USING btree
    ("SheetDefNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: sheet_WebFormSheetID

-- DROP INDEX IF EXISTS public."sheet_WebFormSheetID";

CREATE INDEX IF NOT EXISTS "sheet_WebFormSheetID"
    ON public.sheet USING btree
    ("WebFormSheetID" ASC NULLS LAST)
    TABLESPACE pg_default;
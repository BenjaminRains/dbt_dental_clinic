-- Table: public.sheetdef

-- DROP TABLE IF EXISTS public.sheetdef;

CREATE TABLE IF NOT EXISTS public.sheetdef
(
    "SheetDefNum" integer NOT NULL DEFAULT nextval('"sheetdef_SheetDefNum_seq"'::regclass),
    "Description" character varying(255) COLLATE pg_catalog."default",
    "SheetType" integer,
    "FontSize" real,
    "FontName" character varying(255) COLLATE pg_catalog."default",
    "Width" integer,
    "Height" integer,
    "IsLandscape" smallint,
    "PageCount" integer,
    "IsMultiPage" smallint,
    "BypassGlobalLock" smallint,
    "HasMobileLayout" smallint,
    "DateTCreated" timestamp without time zone,
    "RevID" integer,
    "AutoCheckSaveImage" smallint DEFAULT 1,
    "AutoCheckSaveImageDocCategory" bigint,
    CONSTRAINT sheetdef_pkey PRIMARY KEY ("SheetDefNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.sheetdef
    OWNER to postgres;
-- Index: sheetdef_AutoCheckSaveImageDocCategory

-- DROP INDEX IF EXISTS public."sheetdef_AutoCheckSaveImageDocCategory";

CREATE INDEX IF NOT EXISTS "sheetdef_AutoCheckSaveImageDocCategory"
    ON public.sheetdef USING btree
    ("AutoCheckSaveImageDocCategory" ASC NULLS LAST)
    TABLESPACE pg_default;
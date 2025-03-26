-- Table: public.document

-- DROP TABLE IF EXISTS public.document;

CREATE TABLE IF NOT EXISTS public.document
(
    "DocNum" integer NOT NULL DEFAULT nextval('"document_DocNum_seq"'::regclass),
    "Description" character varying(255) COLLATE pg_catalog."default",
    "DateCreated" timestamp without time zone,
    "DocCategory" bigint,
    "PatNum" bigint,
    "FileName" character varying(255) COLLATE pg_catalog."default",
    "ImgType" smallint DEFAULT 0,
    "IsFlipped" smallint DEFAULT 0,
    "DegreesRotated" real,
    "ToothNumbers" character varying(255) COLLATE pg_catalog."default",
    "Note" text COLLATE pg_catalog."default",
    "SigIsTopaz" smallint,
    "Signature" text COLLATE pg_catalog."default",
    "CropX" integer,
    "CropY" integer,
    "CropW" integer,
    "CropH" integer,
    "WindowingMin" integer,
    "WindowingMax" integer,
    "MountItemNum" bigint,
    "DateTStamp" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    "RawBase64" text COLLATE pg_catalog."default",
    "Thumbnail" text COLLATE pg_catalog."default",
    "ExternalGUID" character varying(255) COLLATE pg_catalog."default",
    "ExternalSource" character varying(255) COLLATE pg_catalog."default",
    "ProvNum" bigint,
    "IsCropOld" smallint,
    "OcrResponseData" text COLLATE pg_catalog."default",
    "ImageCaptureType" smallint DEFAULT 0,
    "PrintHeading" smallint,
    CONSTRAINT document_pkey PRIMARY KEY ("DocNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.document
    OWNER to postgres;
-- Index: document_MountItemNum

-- DROP INDEX IF EXISTS public."document_MountItemNum";

CREATE INDEX IF NOT EXISTS "document_MountItemNum"
    ON public.document USING btree
    ("MountItemNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: document_PNDC

-- DROP INDEX IF EXISTS public."document_PNDC";

CREATE INDEX IF NOT EXISTS "document_PNDC"
    ON public.document USING btree
    ("PatNum" ASC NULLS LAST, "DocCategory" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: document_PatNum

-- DROP INDEX IF EXISTS public."document_PatNum";

CREATE INDEX IF NOT EXISTS "document_PatNum"
    ON public.document USING btree
    ("PatNum" ASC NULLS LAST)
    TABLESPACE pg_default;
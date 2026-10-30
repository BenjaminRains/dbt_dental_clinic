-- Table: public.userod

-- DROP TABLE IF EXISTS public.userod;

CREATE TABLE IF NOT EXISTS public.userod
(
    "UserNum" integer NOT NULL DEFAULT nextval('"userod_UserNum_seq"'::regclass),
    "UserName" character varying(255) COLLATE pg_catalog."default",
    "Password" character varying(255) COLLATE pg_catalog."default",
    "UserGroupNum" bigint,
    "EmployeeNum" bigint,
    "ClinicNum" bigint,
    "ProvNum" bigint,
    "IsHidden" boolean,
    "TaskListInBox" bigint,
    "AnesthProvType" integer DEFAULT 3,
    "DefaultHidePopups" smallint,
    "PasswordIsStrong" smallint,
    "ClinicIsRestricted" smallint,
    "InboxHidePopups" smallint,
    "UserNumCEMT" bigint,
    "DateTFail" timestamp without time zone,
    "FailedAttempts" smallint,
    "DomainUser" character varying(255) COLLATE pg_catalog."default",
    "IsPasswordResetRequired" smallint,
    "MobileWebPin" character varying(255) COLLATE pg_catalog."default",
    "MobileWebPinFailedAttempts" smallint,
    "DateTLastLogin" timestamp without time zone,
    "EClipboardClinicalPin" character varying(128) COLLATE pg_catalog."default",
    "BadgeId" character varying(255) COLLATE pg_catalog."default",
    CONSTRAINT userod_pkey PRIMARY KEY ("UserNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.userod
    OWNER to postgres;
-- Index: userod_ClinicNum

-- DROP INDEX IF EXISTS public."userod_ClinicNum";

CREATE INDEX IF NOT EXISTS "userod_ClinicNum"
    ON public.userod USING btree
    ("ClinicNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: userod_ProvNum

-- DROP INDEX IF EXISTS public."userod_ProvNum";

CREATE INDEX IF NOT EXISTS "userod_ProvNum"
    ON public.userod USING btree
    ("ProvNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: userod_UserGroupNum

-- DROP INDEX IF EXISTS public."userod_UserGroupNum";

CREATE INDEX IF NOT EXISTS "userod_UserGroupNum"
    ON public.userod USING btree
    ("UserGroupNum" ASC NULLS LAST)
    TABLESPACE pg_default;
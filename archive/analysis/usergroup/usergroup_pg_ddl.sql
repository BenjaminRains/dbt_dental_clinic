-- Table: public.usergroup

-- DROP TABLE IF EXISTS public.usergroup;

CREATE TABLE IF NOT EXISTS public.usergroup
(
    "UserGroupNum" integer NOT NULL DEFAULT nextval('"usergroup_UserGroupNum_seq"'::regclass),
    "Description" character varying(255) COLLATE pg_catalog."default",
    "UserGroupNumCEMT" bigint,
    CONSTRAINT usergroup_pkey PRIMARY KEY ("UserGroupNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.usergroup
    OWNER to postgres;
-- Index: usergroup_UserGroupNumCEMT

-- DROP INDEX IF EXISTS public."usergroup_UserGroupNumCEMT";

CREATE INDEX IF NOT EXISTS "usergroup_UserGroupNumCEMT"
    ON public.usergroup USING btree
    ("UserGroupNumCEMT" ASC NULLS LAST)
    TABLESPACE pg_default;
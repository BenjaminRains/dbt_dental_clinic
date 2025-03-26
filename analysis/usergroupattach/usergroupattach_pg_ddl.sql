-- Table: public.usergroupattach

-- DROP TABLE IF EXISTS public.usergroupattach;

CREATE TABLE IF NOT EXISTS public.usergroupattach
(
    "UserGroupAttachNum" integer NOT NULL DEFAULT nextval('"usergroupattach_UserGroupAttachNum_seq"'::regclass),
    "UserNum" bigint,
    "UserGroupNum" bigint,
    CONSTRAINT usergroupattach_pkey PRIMARY KEY ("UserGroupAttachNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.usergroupattach
    OWNER to postgres;
-- Index: usergroupattach_UserGroupNum

-- DROP INDEX IF EXISTS public."usergroupattach_UserGroupNum";

CREATE INDEX IF NOT EXISTS "usergroupattach_UserGroupNum"
    ON public.usergroupattach USING btree
    ("UserGroupNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: usergroupattach_UserNum

-- DROP INDEX IF EXISTS public."usergroupattach_UserNum";

CREATE INDEX IF NOT EXISTS "usergroupattach_UserNum"
    ON public.usergroupattach USING btree
    ("UserNum" ASC NULLS LAST)
    TABLESPACE pg_default;
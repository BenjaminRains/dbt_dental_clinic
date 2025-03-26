-- Table: public.claimtracking

-- DROP TABLE IF EXISTS public.claimtracking;

CREATE TABLE IF NOT EXISTS public.claimtracking
(
    "ClaimTrackingNum" integer NOT NULL DEFAULT nextval('"claimtracking_ClaimTrackingNum_seq"'::regclass),
    "ClaimNum" bigint,
    "TrackingType" character varying(255) COLLATE pg_catalog."default",
    "UserNum" bigint,
    "DateTimeEntry" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    "Note" text COLLATE pg_catalog."default",
    "TrackingDefNum" bigint,
    "TrackingErrorDefNum" bigint,
    CONSTRAINT claimtracking_pkey PRIMARY KEY ("ClaimTrackingNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.claimtracking
    OWNER to postgres;
-- Index: claimtracking_ClaimNum

-- DROP INDEX IF EXISTS public."claimtracking_ClaimNum";

CREATE INDEX IF NOT EXISTS "claimtracking_ClaimNum"
    ON public.claimtracking USING btree
    ("ClaimNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: claimtracking_TrackingDefNum

-- DROP INDEX IF EXISTS public."claimtracking_TrackingDefNum";

CREATE INDEX IF NOT EXISTS "claimtracking_TrackingDefNum"
    ON public.claimtracking USING btree
    ("TrackingDefNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: claimtracking_TrackingErrorDefNum

-- DROP INDEX IF EXISTS public."claimtracking_TrackingErrorDefNum";

CREATE INDEX IF NOT EXISTS "claimtracking_TrackingErrorDefNum"
    ON public.claimtracking USING btree
    ("TrackingErrorDefNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: claimtracking_UserNum

-- DROP INDEX IF EXISTS public."claimtracking_UserNum";

CREATE INDEX IF NOT EXISTS "claimtracking_UserNum"
    ON public.claimtracking USING btree
    ("UserNum" ASC NULLS LAST)
    TABLESPACE pg_default;
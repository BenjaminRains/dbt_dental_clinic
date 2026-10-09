-- public.entrylog definition

-- Drop table

-- DROP TABLE public.entrylog;

CREATE TABLE public.entrylog (
	"EntryLogNum" serial4 NOT NULL,
	"UserNum" int8 NULL,
	"FKeyType" int2 NULL,
	"FKey" int8 NULL,
	"LogSource" int2 NULL,
	"EntryDateTime" timestamp NULL,
	CONSTRAINT entrylog_pkey PRIMARY KEY ("EntryLogNum")
);
CREATE INDEX "entrylog_EntryDateTime" ON public.entrylog USING btree ("EntryDateTime");
CREATE INDEX "entrylog_FKey" ON public.entrylog USING btree ("FKey");
CREATE INDEX "entrylog_UserNum" ON public.entrylog USING btree ("UserNum");
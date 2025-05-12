-- public.tasknote definition

-- Drop table

-- DROP TABLE public.tasknote;

CREATE TABLE public.tasknote (
	"TaskNoteNum" serial4 NOT NULL,
	"TaskNum" int8 NULL,
	"UserNum" int8 NULL,
	"DateTimeNote" timestamp NULL,
	"Note" text NULL,
	CONSTRAINT tasknote_pkey PRIMARY KEY ("TaskNoteNum")
);
CREATE INDEX "tasknote_TaskNum" ON public.tasknote USING btree ("TaskNum");
CREATE INDEX "tasknote_UserNum" ON public.tasknote USING btree ("UserNum");
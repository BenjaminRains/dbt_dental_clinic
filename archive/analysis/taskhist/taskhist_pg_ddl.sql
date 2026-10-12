-- public.taskhist definition

-- Drop table

-- DROP TABLE public.taskhist;

CREATE TABLE public.taskhist (
	"TaskHistNum" serial4 NOT NULL,
	"UserNumHist" int8 NULL,
	"DateTStamp" timestamp NULL,
	"IsNoteChange" int2 NULL,
	"TaskNum" int8 NULL,
	"TaskListNum" int8 NULL,
	"DateTask" date NULL,
	"KeyNum" int8 NULL,
	"Descript" text NULL,
	"TaskStatus" int2 NULL,
	"IsRepeating" int2 NULL,
	"DateType" int2 NULL,
	"FromNum" int8 NULL,
	"ObjectType" int2 NULL,
	"DateTimeEntry" timestamp NULL,
	"UserNum" int8 NULL,
	"DateTimeFinished" timestamp NULL,
	"PriorityDefNum" int8 NULL,
	"ReminderGroupId" varchar(20) NULL,
	"ReminderType" int2 NULL,
	"ReminderFrequency" int4 NULL,
	"DateTimeOriginal" timestamp NULL,
	"SecDateTEdit" timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	"DescriptOverride" varchar(255) NULL,
	"IsReadOnly" bool DEFAULT false NULL,
	"TriageCategory" int8 NULL,
	CONSTRAINT taskhist_pkey PRIMARY KEY ("TaskHistNum")
);
CREATE INDEX "taskhist_DateTStamp" ON public.taskhist USING btree ("DateTStamp");
CREATE INDEX "taskhist_KeyNum" ON public.taskhist USING btree ("KeyNum");
CREATE INDEX "taskhist_SecDateTEdit" ON public.taskhist USING btree ("SecDateTEdit");
CREATE INDEX "taskhist_TaskNum" ON public.taskhist USING btree ("TaskNum");
CREATE INDEX "taskhist_TriageCategory" ON public.taskhist USING btree ("TriageCategory");
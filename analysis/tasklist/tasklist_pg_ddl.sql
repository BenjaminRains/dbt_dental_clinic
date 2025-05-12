-- public.tasklist definition

-- Drop table

-- DROP TABLE public.tasklist;

CREATE TABLE public.tasklist (
	"TaskListNum" serial4 NOT NULL,
	"Descript" varchar(255) NULL,
	"Parent" int8 NULL,
	"DateTL" date NULL,
	"IsRepeating" int2 NULL,
	"DateType" int2 NULL,
	"FromNum" int8 NULL,
	"ObjectType" int2 NULL,
	"DateTimeEntry" timestamp NULL,
	"GlobalTaskFilterType" int2 NULL,
	"TaskListStatus" int2 NULL,
	CONSTRAINT tasklist_pkey PRIMARY KEY ("TaskListNum")
);
CREATE INDEX "tasklist_indexParent" ON public.tasklist USING btree ("Parent");
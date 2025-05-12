-- public.taskunread definition

-- Drop table

-- DROP TABLE public.taskunread;

CREATE TABLE public.taskunread (
	"TaskUnreadNum" serial4 NOT NULL,
	"TaskNum" int8 NULL,
	"UserNum" int8 NULL,
	CONSTRAINT taskunread_pkey PRIMARY KEY ("TaskUnreadNum")
);
CREATE INDEX "taskunread_TaskNum" ON public.taskunread USING btree ("TaskNum");
CREATE INDEX "taskunread_UserNum" ON public.taskunread USING btree ("UserNum");
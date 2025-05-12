-- public.tasksubscription definition

-- Drop table

-- DROP TABLE public.tasksubscription;

CREATE TABLE public.tasksubscription (
	"TaskSubscriptionNum" serial4 NOT NULL,
	"UserNum" int8 NULL,
	"TaskListNum" int8 NULL,
	"TaskNum" int8 NULL,
	CONSTRAINT tasksubscription_pkey PRIMARY KEY ("TaskSubscriptionNum")
);
CREATE INDEX "tasksubscription_TaskListNum" ON public.tasksubscription USING btree ("TaskListNum");
CREATE INDEX "tasksubscription_TaskNum" ON public.tasksubscription USING btree ("TaskNum");
CREATE INDEX "tasksubscription_UserNum" ON public.tasksubscription USING btree ("UserNum");
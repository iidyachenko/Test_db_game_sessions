/*
При разработки использовал следующие предположения:

-Время окончания сессии расчитывается как максимальное время из таблицы ресурсов меньше чем начало следующей сессии для это проекта и пользователя
-Если во время сессии не было движений ресурсов то временем окончания ставил ее начала. 
-К сожалению времени на недели было мало не смог получше оптимизировать скрипт. Очень хотелось избавится от джоина по between и обойтись только оконными функциями.
На больших объемах данных при полной перезагрузки будут проблемы с производительностью.

*/



--Процедура для полной перезагрузки данных в таблице статистики сессий.

CREATE OR REPLACE PROCEDURE insert_session_statistic_full()
 LANGUAGE sql
AS $procedure$

truncate session_statistic;

with v_session as (
SELECT project, 
	user_id, 
	"timestamp", 
	gold, 
	silver, 
	coalesce(LEAD("timestamp",1) OVER (partition by project,user_id ORDER BY project,user_id),'9999-12-31 00:00:00') next_session

FROM "session"
)

INSERT INTO session_statistic
select distinct s.project, 
	s.user_id, 
	s."timestamp" as start_timestamp, 
	coalesce(max(r."timestamp") over w, s."timestamp") as finish_timestamp,
	s.gold as start_gold , 
	s.silver as finish_gold, 	
	coalesce(last_value (r.gold) over w, s.gold) as finish_gold,
	coalesce(last_value (r.silver) over w,s.silver) as finish_silver,
	sum(case when resource = 'gold' and movement = 'income' then r.amount else 0 end) over w as income_gold,
	sum(case when resource = 'gold' and movement = 'outcome' then -r.amount else 0 end) over w as outcome_gold,
	sum(case when resource = 'silver' and movement = 'income' then r.amount else 0 end) over w as income_silver,
	sum(case when resource = 'silver' and movement = 'outcome' then -r.amount else 0 end) over w as outcome_silver
from v_session s
left join resource r on s.project = r.project and s.user_id = r.user_id and r."timestamp" between s."timestamp" and next_session
WINDOW w AS (partition by s.project,s.user_id, s."timestamp")
order by s.project, 
		 s.user_id, 
	     s."timestamp"
;
$procedure$
;


--Процедура для дельта перезагрузки данных в таблице статистики сессий для сессий за определенный отрезок времени.

CREATE OR REPLACE PROCEDURE insert_session_statistic_delta(start_date timestamp with time zone, end_date timestamp with time zone)
 LANGUAGE sql
AS $procedure$

delete from session_statistic
where start_timestamp between start_date and end_date;

with v_session as (
SELECT project, 
	user_id, 
	"timestamp", 
	gold, 
	silver, 
	coalesce(LEAD("timestamp",1) OVER (partition by project,user_id ORDER BY project,user_id),'9999-12-31 00:00:00') next_session

FROM "session"
where "timestamp" between start_date and end_date
)

INSERT INTO session_statistic
select distinct s.project, 
	s.user_id, 
	s."timestamp" as start_timestamp, 
	coalesce(max(r."timestamp") over w, s."timestamp") as finish_timestamp,
	s.gold as start_gold , 
	s.silver as finish_gold, 	
	coalesce(last_value (r.gold) over w, s.gold) as finish_gold,
	coalesce(last_value (r.silver) over w,s.silver) as finish_silver,
	sum(case when resource = 'gold' and movement = 'income' then r.amount else 0 end) over w as income_gold,
	sum(case when resource = 'gold' and movement = 'outcome' then -r.amount else 0 end) over w as outcome_gold,
	sum(case when resource = 'silver' and movement = 'income' then r.amount else 0 end) over w as income_silver,
	sum(case when resource = 'silver' and movement = 'outcome' then -r.amount else 0 end) over w as outcome_silver
from v_session s
left join resource r on s.project = r.project and s.user_id = r.user_id and r."timestamp" between s."timestamp" and next_session
where r."timestamp" between start_date and end_date
WINDOW w AS (partition by s.project,s.user_id, s."timestamp")
order by s.project, 
		 s.user_id, 
	     s."timestamp"
;
$procedure$
;

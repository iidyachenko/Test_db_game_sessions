/*
	Селект запрос для тестирования и отладки процедуры с расширенным числом полей.
*/


--EXPLAIN 
with v_session as (
SELECT project, 
	user_id, 
	"timestamp", 
	gold, 
	silver, 
	coalesce(LEAD("timestamp",1) OVER (partition by project,user_id ORDER BY project,user_id),'9999-12-31 00:00:00') next_session

FROM "session"
)

select distinct s.project, 
	s.user_id, 
	s."timestamp", 
	s.gold, 
	s.silver, 
	next_session,
	coalesce(max(r."timestamp") over w,s."timestamp") as end_session,
	coalesce(last_value (r.gold) over w,s.gold) as finish_gold,
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



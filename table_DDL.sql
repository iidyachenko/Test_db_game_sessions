/* 
DDL скрипт для создания таблиц.  
*/

CREATE TABLE session (
    project varchar,
	user_id varchar,
	timestamp timestamptz,
	gold int,
	silver int,
	
    CONSTRAINT session_pk PRIMARY KEY (project, user_id, timestamp)
) ;

/* 
Так как судя по структуре таблицы session и movement будут использоваться для частой вставки данных,
Сильно увлекаться индексами не стоит, однако созданный на ключевых полях project и user_id индекс помог согласно плану заппроса ускорить его почти в 20 раз... 
Но конечно стоит тестировать на большом объеме данных. 
*/
CREATE INDEX session_project_idx ON session (project,user_id);

create type movement as enum ('income', 'outcome');
CREATE TABLE resource (
  	project varchar,
	user_id varchar,
	timestamp timestamptz,
	gold int,
	silver int,
	resource varchar,
	movement movement,
	amount int,
	
    CONSTRAINT resource_pk PRIMARY KEY (project, user_id, timestamp)
) ;

CREATE INDEX resource_project_idx ON resource (project,user_id);

CREATE TABLE session_statistic (
	project varchar,
	user_id varchar,
	start_timestamp timestamptz,
	finish_timestamp timestamptz,
	start_gold int,
	finish_gold int,
	start_silver int,
	finish_silver int,
	income_gold int,
	outcome_gold int,
	income_silver int,
	outcome_silver int,
	
    CONSTRAINT session_statistic_pk PRIMARY KEY (project, user_id, start_timestamp, finish_timestamp)
) ;
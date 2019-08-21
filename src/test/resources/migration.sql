create sequence logged_seq start with 0;

create table if not exists logged (
  log_id bigint default logged_seq.nextval primary key,
  ancestor_id bigint unique references logged(log_id),
  value_one bigint,
  value_two bigint
)
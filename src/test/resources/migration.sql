create table if not exists logged (
  log_id bigint primary key auto_increment,
  ancestor_id bigint unique references logged(log_id),
  value_one bigint,
  value_two bigint
)
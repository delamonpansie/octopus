#!/usr/bin/ruby

$: << File.dirname($0) + '/lib'
require 'run_env'

RunEnv.connect_eval do
  ping
  insert [1, 2, "abc", "def"]

  update_fields 1, [1, :or, 0xff]
  select 1

  update_fields 1, [1, :set, 127]
  select 1

  update_fields 1, [2, :splice, 2, 4, "aaaa"]
  select 1

  update_fields 1, [2, :splice, 2, 4, ""]
  select 1

  update_fields 1, [1, :insert, 11]
  select 1

  update_fields 1, [1, :delete, ""]
  select 1

  update_fields 1, [1, :delete, ""], [1, :delete, ""], [1, :delete, ""]
  select 1

  log_try { update_fields 1, [1, :delete, ""] }
  select 1

  insert [1]
  update_fields 1, [1, :insert, "aa"]
  select 1

  insert [10, "foo"]
  update_fields 10, [1, :splice, 0, 1, "b"]
  select 10

  insert [10, "foo"]
  update_fields 10, [1, :set, ""]
  update_fields 10, [1, :set, ""]
  select 10

  # test update of PK
  insert [2, "foo"]
  insert [3, "bar"]
  select 2, 3, 4
  update_fields 2, [0, :set, 4]
  select 2, 3, 4
  log_try { update_fields 4, [0, :set, 3] }
end

class UpdateEnv < RunEnv
  def config
    super + <<EOD
object_space[0].enabled = 1
object_space[0].index[0].type = "HASH"
object_space[0].index[0].unique = 1
object_space[0].index[0].key_field[0].fieldno = 0
object_space[0].index[0].key_field[0].type = "STR"
EOD
  end
end

UpdateEnv.new.with_server do |box|
  box.ping
  box.insert [1, 2, "abc", "def"]

  box.update_fields 1, [1, :or, 0xff]
  box.select 1

  box.update_fields 1, [1, :set, 127]
  box.select 1

  box.update_fields 1, [2, :splice, 2, 4, "aaaa"]
  box.select 1

  box.update_fields 1, [2, :splice, 2, 4, ""]
  box.select 1

  box.update_fields 1, [1, :insert, 11]
  box.select 1

  box.update_fields 1, [1, :delete, ""]
  box.select 1

  box.update_fields 1, [1, :delete, ""], [1, :delete, ""], [1, :delete, ""]
  box.select 1

  LogPpProxy.try { box.update_fields 1, [1, :delete, ""] }
  box.select 1

  box.insert [1]
  box.update_fields 1, [1, :insert, "aa"]
  box.select 1

  box.insert [10, "foo"]
  box.update_fields 10, [1, :splice, 0, 1, "b"]
  box.select 10

  box.insert [10, "foo"]
  box.update_fields 10, [1, :set, ""]
  box.update_fields 10, [1, :set, ""]
  box.select 10

end

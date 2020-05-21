#!/usr/bin/ruby

$: << File.dirname($0) + '/lib'
require 'run_env'

class Env < RunEnv
  def config
    super + <<EOD
object_space[0].enabled = 1
object_space[0].index[0] = { type = "HASH"
			     unique = 1
			     key_field[1] = { fieldno = 1, type = "STR" }
}
object_space[0].index[1] = { type = "TREE"
			     unique = 0
                             key_field[0] = { fieldno = 0, type = "STR" }
			     key_field[1] = { fieldno = 1, type = "NUM" }
}
object_space[0].index[2] = { type = "TREE"
			     unique = 0
                             key_field[0] = { fieldno = 0, type = "STR" }
			     key_field[1] = { fieldno = 1, type = "SNUM32" }
}
object_space[0].index[3] = { type = "TREE"
			     unique = 0
                             key_field[0] = { fieldno = 0, type = "STR" }
			     key_field[1] = { fieldno = 1, type = "UNUM32" }
}
object_space[0].index[4] = { type = "TREE"
			     unique = 0
                             key_field[0] = { fieldno = 0, type = "STR" }
			     key_field[1] = { fieldno = 1, type = "NUM32", sort_order = "DESC" }
}
object_space[0].index[5] = { type = "TREE"
			     unique = 0
                             key_field[0] = { fieldno = 0, type = "STR" }
			     key_field[1] = { fieldno = 1, type = "SNUM32", sort_order = "DESC" }
}
object_space[0].index[6] = { type = "TREE"
			     unique = 0
                             key_field[0] = { fieldno = 0, type = "STR" }
			     key_field[1] = { fieldno = 1, type = "UNUM32", sort_order = "DESC" }
}


object_space[1].enabled = 1
object_space[1].index[0] = { type = "HASH"
			     unique = 1
			     key_field[0] = { fieldno = 1, type = "STR" }
}
object_space[1].index[1] = { type = "TREE"
			     unique = 0
                             key_field[0] = { fieldno = 0, type = "STR" }
			     key_field[1] = { fieldno = 1, type = "NUM64" }
}
object_space[1].index[2] = { type = "TREE"
			     unique = 0
                             key_field[0] = { fieldno = 0, type = "STR" }
			     key_field[1] = { fieldno = 1, type = "SNUM64" }
}
object_space[1].index[3] = { type = "TREE"
			     unique = 0
                             key_field[0] = { fieldno = 0, type = "STR" }
			     key_field[1] = { fieldno = 1, type = "UNUM64" }
}
object_space[1].index[4] = { type = "TREE"
			     unique = 0
                             key_field[0] = { fieldno = 0, type = "STR" }
			     key_field[1] = { fieldno = 1, type = "NUM64", sort_order = "DESC" }
}
object_space[1].index[5] = { type = "TREE"
			     unique = 0
                             key_field[0] = { fieldno = 0, type = "STR" }
			     key_field[1] = { fieldno = 1, type = "SNUM64", sort_order = "DESC" }
}
object_space[1].index[6] = { type = "TREE"
			     unique = 0
                             key_field[0] = { fieldno = 0, type = "STR" }
			     key_field[1] = { fieldno = 1, type = "UNUM64", sort_order = "DESC" }
}


object_space[2].enabled = 1
object_space[2].index[0] = { type = "HASH"
			     unique = 1
			     key_field[0] = { fieldno = 0, type = "STR" }
}
object_space[2].index[1] = { type = "TREE"
			     unique = 1
                             key_field[0] = { fieldno = 0, type = "STR" }
			     key_field[1] = { fieldno = 1, type = "STR" }
}
object_space[2].index[2] = { type = "TREE"
			     unique = 1
                             key_field[0] = { fieldno = 0, type = "STR" }
			     key_field[1] = { fieldno = 1, type = "STR", sort_order = "DESC" }
}


EOD
  end
end

Env.connect_eval do
  ping

  self.object_space = 0
  (-10..10).each do |v|
    insert ["a", v]
  end
  sig = [nil, 'L', 'l', 'L', 'L', 'l', 'L']
  (1..6).each do |i|
    s = select(["a"], :index => i)
    puts s.map {|el| el[1].unpack(sig[i])}.join(", ")
    puts
  end

  self.object_space = 1
  (-10..10).each do |v|
    v = [v].pack('q')
    insert ["a", v]
  end
  sig = [nil, 'Q', 'q', 'Q', 'Q', 'q', 'Q']
  (1..6).each do |i|
    s = select(["a"], :index => i)
    puts s.map {|el| el[1].unpack(sig[i])}.join(", ")
    puts
  end


end

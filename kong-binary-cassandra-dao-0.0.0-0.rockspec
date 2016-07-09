package = "kong-binary-cassandra-dao"
version = "0.0.0-0"
supported_platforms = {"linux", "macosx"}
source = {
  url = "git://github.com/smanolache/kong-binary-cassandra-dao"
}
description = {
  summary = "A DAO for Kong that uses the datastax binary cassandra driver.",
  license = "MIT"
}
dependencies = {
  "kong ~> 0.8.3",
  "lua-cassandra-driver ~> 0.0.0",
}
build = {
  type = "builtin",
  modules = {
    ["kong.dao.cassandra_binary_db"] = "kong/dao/cassandra_binary_db.lua",
    ["kong.plugins.rate-limiting.dao.cassandra_binary"] = "kong/plugins/rate-limiting/dao/cassandra_binary.lua",
    ["kong.plugins.response-ratelimiting.dao.cassandra_binary"] = "kong/plugins/response-ratelimiting/dao/cassandra_binary.lua",
  }
}

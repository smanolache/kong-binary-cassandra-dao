local BaseDB = require "kong.dao.base_db"
local db = require "db.cassandra"
local utils = require "kong.tools.utils"
local timestamp = require "kong.tools.timestamp"
local uuid = require "lua_uuid"
local Errors = require "kong.dao.errors"

local CassBinaryDB = BaseDB:extend()

local singleton_cluster = nil
local singleton_session = nil

local KEYS_OF_INTEREST = {"prepare", "keyspace", "consistency", "auto_paging", "page_size"}
local function build_opts(base, override)
   if nil == override then
      return base
   end
   if nil == base then
      return override
   end
   r = {}
   for _, k in pairs(KEYS_OF_INTEREST) do
      if nil ~= override[k] then
   	 r[k] = override[k]
      elseif nil ~= base[k] then
	 r[k] = base[k]
      end
   end

   return r
end

local function cl(consistency)
   if "string" == type(consistency) then
      if "ANY" == consistency then
	 return db.CL.ANY
      end
      if "ONE" == consistency then
	 return db.CL.ONE
      end
      if "TWO" == consistency then
	 return db.CL.TWO
      end
      if "THREE" == consistency then
	 return db.CL.THREE
      end
      if "QUORUM" == consistency then
	 return db.CL.QUORUM
      end
      if "ALL" == consistency then
	 return db.CL.ALL
      end
      if "LOCAL_QUORUM" == consistency then
	 return db.CL.LOCAL_QUORUM
      end
      if "EACH_QUORUM" == consistency then
	 return db.CL.EACH_QUORUM
      end
      if "SERIAL" == consistency then
	 return db.CL.SERIAL
      end
      if "LOCAL_SERIAL" == consistency then
	 return db.CL.LOCAL_SERIAL
      end
      if "LOCAL_ONE" == consistency then
	 return db.CL.LOCAL_ONE
      end
      return db.CL.ONE
   end
   return consistency
end

local function init_cluster(opts)
   local cluster = db.cass_cluster_new()
   if nil == cluster then
      return nil, "cannot construct cassandra cluster representation"
   end

   local rc = cluster:set_port(opts.protocol_options.default_port)
   if 0 ~= rc then
      return nil, db.cass_error_desc(rc)
   end

   rc = cluster:set_protocol_version(opts.protocol_options.default_version)
   if 0 ~= rc then
      return nil, db.cass_error_desc(rc)
   end

   rc = cluster:set_contact_points(table.concat(opts.contact_points, ","))
   if 0 ~= rc then
      return nil, db.cass_error_desc(rc)
   end

   if opts.auth then
      cluster:set_credentials(opts.auth.username, opts.auth.password)
   end

   if opts.ssl_options.enabled then
      local ssl = db.cass_ssl_new()
      if nil == ssl then
	 return nil, "cannot build ssl object; probably out-of-memory"
      end

      rc = ssl:set_verify_flags(opts.ssl_options.verify)
      if 0 ~= rc then
	 return nil, db.cass_error_desc(rc)
      end

      cluster:set_ssl(ssl)
   end

   if nil ~= opts.core_connections_per_host then
      rc = cluster:set_core_connections_per_host(opts.core_connections_per_host)
      if 0 ~= rc then
	 return nil, db.cass_error_desc(rc)
      end
   end

   if nil ~= opts.max_connections_per_host then
      rc = cluster:set_max_connections_per_host(opts.max_connections_per_host)
      if 0 ~= rc then
	 return nil, db.cass_error_desc(rc)
      end
   end

   if nil ~= opts.num_threads_io then
      rc = cluster:set_num_threads_io(opts.num_threads_io)
      if 0 ~= rc then
	 return nil, db.cass_error_desc(rc)
      end
   end

   if nil ~= opts.queue_size_io then
      rc = cluster:set_queue_size_io(opts.queue_size_io)
      if 0 ~= rc then
	 return nil, db.cass_error_desc(rc)
      end
   end

   if nil ~= opts.queue_size_event then
      rc = cluster:set_queue_size_event(opts.queue_size_event)
      if 0 ~= rc then
	 return nil, db.cass_error_desc(rc)
      end
   end

   if nil ~= opts.reconnect_wait_time then
      rc = cluster:set_reconnect_wait_time(opts.reconnect_wait_time)
      if 0 ~= rc then
	 return nil, db.cass_error_desc(rc)
      end
   end

   if nil ~= opts.max_concurrent_creation then
      rc = cluster:set_max_concurrent_creation(opts.max_concurrent_creation)
      if 0 ~= rc then
	 return nil, db.cass_error_desc(rc)
      end
   end

   if nil ~= opts.max_concurrent_requests_threshold then
      rc = cluster:set_max_concurrent_requests_threshold(opts.max_concurrent_requests_threshold)
      if 0 ~= rc then
	 return nil, db.cass_error_desc(rc)
      end
   end

   if nil ~= opts.max_requests_per_flush then
      rc = cluster:set_max_requests_per_flush(opts.max_requests_per_flush)
      if 0 ~= rc then
	 return nil, db.cass_error_desc(rc)
      end
   end

   if nil ~= opts.write_bytes_high_water_mark then
      rc = cluster:set_write_bytes_high_water_mark(opts.write_bytes_high_water_mark)
      if 0 ~= rc then
	 return nil, db.cass_error_desc(rc)
      end
   end

   if nil ~= opts.write_bytes_low_water_mark then
      rc = cluster:set_write_bytes_low_water_mark(opts.write_bytes_low_water_mark)
      if 0 ~= rc then
	 return nil, db.cass_error_desc(rc)
      end
   end

   if nil ~= opts.pending_requests_high_water_mark then
      rc = cluster:set_pending_requests_high_water_mark(opts.pending_requests_high_water_mark)
      if 0 ~= rc then
	 return nil, db.cass_error_desc(rc)
      end
   end

   if nil ~= opts.pending_requests_low_water_mark then
      rc = cluster:set_pending_requests_low_water_mark(opts.pending_requests_low_water_mark)
      if 0 ~= rc then
	 return nil, db.cass_error_desc(rc)
      end
   end

   if nil ~= opts.connect_timeout then
      cluster:set_connect_timeout(opts.connect_timeout)
   end

   if nil ~= opts.request_timeout then
      cluster:set_request_timeout(opts.request_timeout)
   end

   if nil ~= opts.load_balance_round_robin and opts.load_balance_round_robin then
      cluster:set_load_balance_round_robin()
   end

   if nil ~= opts.load_balance_dc_aware then
      rc = cluster:set_load_balance_dc_aware(
	 opts.load_balance_dc_aware.local_dc,
	 opts.load_balance_dc_aware.used_hosts_per_remote_dc,
	 opts.load_balance_dc_aware.allow_remote_dcs_for_local_cl)
      if 0 ~= rc then
	 return nil, db.cass_error_desc(rc)
      end
   end

   if nil ~= opts.token_aware_routing then
      cluster:set_token_aware_routing(opts.token_aware_routing)
   end

   if nil ~= opts.latency_aware_routing then
      cluster:set_latency_aware_routing(opts.latency_aware_routing)
   end

   if nil ~= opts.latency_aware_routing_settings then
      cluster:set_latency_aware_routing_settings(
	 opts.latency_aware_routing_settings.exclusion_threshold,
	 opts.latency_aware_routing_settings.scale_ms,
	 opts.latency_aware_routing_settings.retry_period_ms,
	 opts.latency_aware_routing_settings.update_rate_ms,
	 opts.latency_aware_routing_settings.min_measured)
   end

   if nil ~= opts.whitelist_filtering then
      cluster:set_whitelist_filtering(opts.whitelist_filtering)
   end

   if nil ~= opts.blacklist_filtering then
      cluster:set_blacklist_filtering(opts.blacklist_filtering)
   end

   if nil ~= opts.whitelist_dc_filtering then
      cluster:set_whitelist_dc_filtering(opts.whitelist_dc_filtering)
   end

   if nil ~= opts.blacklist_dc_filtering then
      cluster:set_blacklist_dc_filtering(opts.blacklist_dc_filtering)
   end

   if nil ~= opts.tcp_nodelay then
      cluster:set_tcp_nodelay(opts.tcp_nodelay)
   end

   if nil ~= opts.tcp_keepalive then
      cluster:set_tcp_keepalive(opts.tcp_keepalive.enabled, opts.tcp_keepalive.delay_secs)
   end

   if nil ~= opts.timestamp_gen then
      if "monotonic" == opts.timestamp_gen then
	 cluster:set_timestamp_gen(db.cass_timestamp_gen_monotonic_new())
      elseif "server_side" == opts.timestamp_gen then
	 cluster:set_timestamp_gen(db.cass_timestamp_gen_server_side_new())
      end
   end

   if nil ~= opts.connection_heartbeat_interval then
      cluster:set_connection_heartbeat_interval(opts.connection_heartbeat_interval)
   end

   if nil ~= opts.connection_idle_timeout then
      cluster:set_connection_idle_timeout(opts.connection_idle_timeout)
   end

   if nil ~= opts.retry_policy then
      if "default" == opts.retry_policy.name then
	 retry_policy = db.cass_retry_policy_default_new()
      elseif "downgrading_consistency" == opts.retry_policy.name then
	 retry_policy = db.cass_retry_policy_downgrading_consistency_new()
      elseif "policy_fallthrough" == opts.retry_policy.name then
	 retry_policy = db.cass_retry_policy_fallthrough_new()
      end
      if opts.retry_policy.logging then
	 local inner_policy = child_policy
	 retry_policy = db.cass_retry_policy_logging_new(child_policy)
      end
      cluster:set_retry_policy(retry_policy)
   end

   if nil ~= opts.use_schema then
      cluster:set_use_schema(opts.use_schema)
   end

   return cluster, nil
end

local function init_session(cluster, keyspace)
   if nil == cluster then
      return nil
   end
   local session = db.cass_session_new()
   if nil == session then
      return nil
   end

   local future
   if nil ~= keyspace then
      future = session:connect_keyspace(cluster, keyspace)
   else
      future = session:connect(cluster)
   end
   future:wait()
   local rc = future:error_code()
   if 0 ~= rc then
      return nil
   end
   return session
end

local function cleanup_db(local_cluster, local_session, obj_cluster, obj_session)
   if local_session ~= obj_session then
      local future = local_session:close()
      if nil == future then
	 return "cannot get a future for session:close: probably oom"
      end
      future:wait()
      local rc = future:error_code()
      if 0 ~= rc then
	 return db.cass_error_desc(rc)
      end
      local_session = nil
   end
   if local_cluster ~= obj_cluster then
      local_cluster = nil
   end
--   collectgarbage()
   return nil
end

CassBinaryDB.dao_insert_values = {
  id = function()
    return uuid()
  end,
  timestamp = function()
    return timestamp.get_utc()
  end
}

function CassBinaryDB:new(options)
   local conn_opts = {
      shm                               = "cassandra_binary",
      prepared_shm                      = "cassandra_binary_prepared",
      contact_points                    = options.contact_points,
      keyspace                          = options.keyspace and options.keyspace or "kong",

      core_connections_per_host         = options.core_connections_per_host,
      max_connections_per_host          = options.max_connections_per_host,
      num_threads_io                    = options.num_threads_io,
      queue_size_io                     = options.queue_size_io,
      queue_size_event                  = options.queue_size_event,
      reconnect_wait_time               = options.reconnect_wait_time,
      max_concurrent_creation           = options.max_concurrent_creation,
      max_concurrent_requests_threshold = options.max_concurrent_requests_threshold,
      max_requests_per_flush            = options.max_requests_per_flush,
      write_bytes_high_water_mark       = options.write_bytes_high_water_mark,
      write_bytes_low_water_mark        = options.write_bytes_low_water_mark,
      pending_requests_high_water_mark  = options.pending_requests_high_water_mark,
      pending_requests_low_water_mark   = options.pending_requests_low_water_mark,
      connect_timeout                   = options.connect_timeout,
      request_timeout                   = options.request_timeout,
      load_balance_round_robin          = options.load_balance_round_robin,
      token_aware_routing               = options.token_aware_routing,
      latency_aware_routing             = options.latency_aware_routing,
      whitelist_filtering               = options.whitelist_filtering,
      blacklist_filtering               = options.blacklist_filtering,
      whitelist_dc_filtering            = options.whitelist_dc_filtering,
      blacklist_dc_filtering            = options.blacklist_dc_filtering,
      tcp_nodelay                       = options.tcp_nodelay,
      timestamp_gen                     = options.timestamp_gen,
      connection_heartbeat_interval     = options.connection_heartbeat_interval,
      connection_idle_timeout           = options.connection_idle_timeout,
      use_schema                        = options.use_schema,

      protocol_options = {
	 default_version = options.protocol_version and options.protocol_version or 3,
	 default_port    = options.port and options.port or 9042
      },
      query_options = {
	 prepare     = true,
	 consistency = options.consistency and options.consistency or db.CL.ONE,
	 auto_paging = options.auto_paging or false,
	 page_size   = options.page_size,
	 keyspace    = options.keyspace or "kong"
      },
      ssl_options = {
	 enabled = options.ssl and options.ssl.enabled or false,
	 verify  = options.ssl and options.ssl.verify or 0,
	 ca      = options.ssl and options.ssl.certificate_authority or nil
      }
   }

   if options.username and options.password then
      conn_opts.auth = {username = options.username, password = options.password}
   end

   if options.load_balance_dc_aware then
      conn_opts.load_balance_dc_aware = {
	 local_dc                      = options.load_balance_dc_aware.local_dc,
	 used_hosts_per_remote_dc      = options.load_balance_dc_aware.used_hosts_per_remote_dc,
	 allow_remote_dcs_for_local_cl = options.load_balance_dc_aware.allow_remote_dcs_for_local_cl
      }
   end

   if options.latency_aware_routing_settings then
      conn_opts.latency_aware_routing_settings = {
	 exclusion_threshold = options.latency_aware_routing_settings.exclusion_threshold,
	 scale_ms            = options.latency_aware_routing_settings.scale_ms,
	 retry_period_ms     = options.latency_aware_routing_settings.retry_period_ms,
	 update_rate_ms      = options.latency_aware_routing_settings.update_rate_ms,
	 min_measured        = options.latency_aware_routing_settings.min_measured
      }
   end

   if nil ~= options.tcp_keepalive then
      conn_opts.tcp_keepalive = {
	 enabled   = options.tcp_keepalive.enabled,
	 delay_sec = options.tcp_keepalive.delay_secs
      }
   end

   if nil ~= options.retry_policy then
      local name = options.retry_policy.name and options.retry_policy.name or options.retry_policy
      local logging = false
      if nil ~= options.retry_policy.logging and options.retry_policy_logging then
	 logging = true
      end
      conn_opts.retry_policy = {
	 name    = name,
	 logging = logging
      }
   end

   CassBinaryDB.super.new(self, "cassandra_binary", conn_opts)
end

function CassBinaryDB:infos()
   return {
      desc = "keyspace",
      name = self:_get_conn_options().keyspace
   }
end

function CassBinaryDB:init()
   local opts = self:_get_conn_options()

   local c, cerr = init_cluster(opts)
   if cerr then
      -- TODO
      return
   end

   singleton_cluster = c

   local s, serr = init_session(singleton_cluster, opts.keyspace)
   if serr then
      -- TODO
      return
   end

   singleton_session = s
end

local function init_db(opts, no_keyspace)
   local cluster

   if nil ~= singleton_cluster then
      cluster = singleton_cluster
   else
      local err
      cluster, err = init_cluster(opts)
      if err then
	 return nil, nil, err
      end
   end

   local session
   if nil ~= singleton_session then
      session = singleton_session
   else
      local err
      local ksp = opts.keyspace
      if no_keyspace then
	 ksp = nil
      end
      session, err = init_session(cluster, ksp)
      if err then
	 return nil, nil, err
      end
   end

   return cluster, session
end

local function bind_value_to_stmt(stmt, value, value_type, ndx)
   if nil == value then
      return stmt:bind_null(ndx)
   end

   if db.TYPES.VALUE_TYPE_ASCII == value_type or
      db.TYPES.VALUE_TYPE_TEXT == value_type or
      db.TYPES.VALUE_TYPE_VARCHAR == value_type
   then
      return stmt:bind_string(ndx, value)
   end

   if db.TYPES.VALUE_TYPE_BIGINT == value_type or
      db.TYPES.VALUE_TYPE_COUNTER == value_type or
      db.TYPES.VALUE_TYPE_TIMESTAMP == value_type or
      db.TYPES.VALUE_TYPE_TIME == value_type
   then
      return stmt:bind_int64(ndx, value)
   end

   if db.TYPES.VALUE_TYPE_BLOB == value_type or
      db.TYPES.VALUE_TYPE_VARINT == value_type
   then
      return stmt:bind_bytes(ndx, value)
   end

   if db.TYPES.VALUE_TYPE_BOOLEAN == value_type then
      return stmt:bind_bool(ndx, value)
   end

   if db.TYPES.VALUE_TYPE_DATE == value_type then
      return stmt:bind_uint32(ndx, value)
   end

   if db.TYPES.VALUE_TYPE_DECIMAL == value_type then
      return stmt:bind_decimal(ndx, value.number, value.scale)
   end

   if db.TYPES.VALUE_TYPE_DOUBLE == value_type then
      return stmt:bind_double(ndx, value)
   end

   if db.TYPES.VALUE_TYPE_FLOAT == value_type then
      return stmt:bind_float(ndx, value)
   end

   if db.TYPES.VALUE_TYPE_INET == value_type then
      return stmt:bind_inet(ndx, value)
   end

   if db.TYPES.VALUE_TYPE_INT == value_type then
      return stmt:bind_int32(ndx, value)
   end

   if db.TYPES.VALUE_TYPE_TINYINT == value_type then
      return stmt:bind_int8(ndx, value)
   end

   if db.TYPES.VALUE_TYPE_SMALLINT == value_type then
      return stmt:bind_int16(ndx, value)
   end

   if db.TYPES.VALUE_TYPE_UUID == value_type or
      db.TYPES.VALUE_TYPE_TIMEUUID == value_type
   then
      return stmt:bind_uuid(ndx, value)
   end

   if db.TYPES.VALUE_TYPE_LIST == value_type or
      db.TYPES.VALUE_TYPE_SET == value_type or
      db.TYPES.VALUE_TYPE_MAP == value_type
   then
      return stmt:bind_collection(ndx, value)
   end

   if db.TYPES.VALUE_TYPE_UDT == value_type then
      return stmt:bind_user_type(ndx, value)
   end

   if db.TYPES.VALUE_TYPE_TUPLE == value_type then
      return stmt:bind_tuple(ndx, value)
   end

   return db.ERRORS.LIB_INVALID_VALUE_TYPE
end

local function serialize_uuid(v)
   local r = ""
   for i=1,4 do
      r = r..string.format("%02x", v:byte(i))
   end
   r = r.."-"
   for i=5,6 do
      r = r..string.format("%02x", v:byte(i))
   end
   r = r.."-"
   for i=7,8 do
      r = r..string.format("%02x", v:byte(i))
   end
   r = r.."-"
   for i=9,10 do
      r = r..string.format("%02x", v:byte(i))
   end
   r = r.."-"
   for i=11,16 do
      r = r..string.format("%02x", v:byte(i))
   end
   return r
end

local function read_value(value, value_type)
   local elem = nil
   if nil == value then
      elem = nil
   elseif value:is_null() then
      elem = nil
   elseif db.TYPES.VALUE_TYPE_ASCII == value_type or
      db.TYPES.VALUE_TYPE_TEXT == value_type or
      db.TYPES.VALUE_TYPE_VARCHAR == value_type
   then
      local rc
      rc, elem = value:get_string()
      if 0 ~= rc then
	 return nil, db.cass_error_desc(rc)
      end
   elseif db.TYPES.VALUE_TYPE_BIGINT == value_type or
      db.TYPES.VALUE_TYPE_COUNTER == value_type or
      db.TYPES.VALUE_TYPE_TIMESTAMP == value_type or
      db.TYPES.VALUE_TYPE_TIME == value_type
   then
      local rc
      rc, elem = value:get_int64()
      if 0 ~= rc then
	 return nil, db.cass_error_desc(rc)
      end
   elseif db.TYPES.VALUE_TYPE_BLOB == value_type or
      db.TYPES.VALUE_TYPE_VARINT == value_type
   then
      local rc
      rc, elem = value:get_bytes()
      if 0 ~= rc then
	 return nil, db.cass_error_desc(rc)
      end
   elseif db.TYPES.VALUE_TYPE_BOOLEAN == value_type then
      local rc
      rc, elem = value:get_bool()
      if 0 ~= rc then
	 return nil, db.cass_error_desc(rc)
      end
   elseif db.TYPES.VALUE_TYPE_DATE == value_type then
      local rc
      rc, elem = value:get_uint32()
      if 0 ~= rc then
	 return nil, db.cass_error_desc(rc)
      end
   elseif db.TYPES.VALUE_TYPE_DECIMAL == value_type then
      local rc
      rc, elem = value:get_decimal()
      if 0 ~= rc then
	 return nil, db.cass_error_desc(rc)
      end
   elseif db.TYPES.VALUE_TYPE_DOUBLE == value_type then
      local rc
      rc, elem = value:get_double()
      if 0 ~= rc then
	 return nil, db.cass_error_desc(rc)
      end
   elseif db.TYPES.VALUE_TYPE_FLOAT == value_type then
      local rc
      rc, elem = value:get_float()
      if 0 ~= rc then
	 return nil, db.cass_error_desc(rc)
      end
   elseif db.TYPES.VALUE_TYPE_INET == value_type then
      local rc
      rc, elem = value:get_inet()
      if 0 ~= rc then
	 return nil, db.cass_error_desc(rc)
      end
   elseif db.TYPES.VALUE_TYPE_INT == value_type then
      local rc
      rc, elem = value:get_int32()
      if 0 ~= rc then
	 return nil, db.cass_error_desc(rc)
      end
   elseif db.TYPES.VALUE_TYPE_TINYINT == value_type then
      local rc
      rc, elem = value:get_int8()
      if 0 ~= rc then
	 return nil, db.cass_error_desc(rc)
      end
   elseif db.TYPES.VALUE_TYPE_SMALLINT == value_type then
      local rc
      rc, elem = value:get_int16()
      if 0 ~= rc then
	 return nil, db.cass_error_desc(rc)
      end
   elseif db.TYPES.VALUE_TYPE_UUID == value_type or
      db.TYPES.VALUE_TYPE_TIMEUUID == value_type
   then
      local rc, uid
      rc, uid = value:get_uuid()
      if 0 ~= rc then
	 return nil, db.cass_error_desc(rc)
      end
      elem = serialize_uuid(uid)
   elseif db.TYPES.VALUE_TYPE_LIST == value_type or
      db.TYPES.VALUE_TYPE_SET == value_type
   then
      local it = value:iterator_from_collection()
      local s = {}
      while it:next() do
	 local val = it:get_value()
	 if nil == val then
	    -- TODO
	 end
	 local v, err = read_value(val, val:type())
	 if err then
	    return nil, err
	 end
	 s[#s + 1] = v
      end
      elem = s
   elseif db.TYPES.VALUE_TYPE_MAP == value_type then
      local it = value:iterator_from_collection()
      local s = {}
      while it:next() do
	 local val = it:get_value()
	 if nil == val then
	    -- TODO
	 end
	 local k, err = read_value(val, val:type())
	 if err then
	    return nil, err
	 end
	 if not it:next() then
	    -- TODO
	 end
	 val = it:get_value()
	 if nil == val then
	    -- TODO
	 end
	 local v
	 v, err = read_value(val, val:type())
	 if err then
	    return nil, err
	 end
	 s[k] = v
      end
      elem = s
   elseif db.TYPES.VALUE_TYPE_UDT == value_type then
      local it = value:iterator_fields_from_user_type()
      local s = {}
      while it:next() do
	 local rc, name = it:get_user_type_field_name()
	 if 0 ~= rc then
	    return nil, db.cass_error_desc(rc)
	 end
	 local val = it:get_user_type_field_value()
	 if nil == val then
	    -- TODO
	 end
	 local v, err = read_value(val, val:type())
	 if err then
	    return nil, err
	 end
	 s[name] = v
      end
      elem = s
   elseif db.TYPES.VALUE_TYPE_TUPLE == value_type then
      local it = value:iterator_from_tuple()
      local s = {}
      while it:next() do
	 local val = it:get_value()
	 if nil == val then
	    -- TODO
	 end
	 local v, err = read_value(val, val:type())
	 if err then
	    return nil, err
	 end
	 s[#s + 1] = v
      end
      elem = s
   else
      elem = nil
   end

   return elem, nil
end

local function result_to_table(result, schema)
   local json = require "cjson"
   local column_count = result:column_count()
   local cols = {}
   for i = 1, column_count do
      local col_name
      local col_type
      local rc
      rc, col_name = result:column_name(i-1)
      if 0 ~= rc then
	 return nil, db.cass_error_desc(rc)
      end
      col_type = result:column_type(i-1)
      cols[#cols + 1] = {name = col_name, type = col_type}
   end

   local rows = {}
   local it = result:iterator()
   while it:next() do
      local row = {}
      local r = it:get_row()
      for i = 1, column_count do
	 local v, err = read_value(r:get_column(i-1), cols[i].type)
	 if err then
	    return nil, err
	 end
	 if nil ~= schema and schema.fields[cols[i].name] and ("table" == schema.fields[cols[i].name].type or "array" == schema.fields[cols[i].name].type) then
	    row[cols[i].name] = json.decode(v)
	 else
	    row[cols[i].name] = v
	 end
      end
      rows[#rows + 1] = row
   end

   return rows, nil
end

local function serialize_arg(field, value)
   if "table" == field.type or "array" == field.type then
      local json = require "cjson"
      return json.encode(value)
   end
   return value
end

local function get_where(schema, filter_keys, args)
   args = args or {}
   local fields = schema.fields
   local where = {}

   for col, value in pairs(filter_keys) do
      where[#where + 1] = col.." = ?"
      args[#args + 1] = serialize_arg(fields[col], value)
   end

   return table.concat(where, " AND "), args
end

local function get_select_query(table_name, where, select_clause)
  local query = string.format("SELECT %s FROM %s", select_clause or "*", table_name)
  if where ~= nil then
    query = query.." WHERE "..where.." ALLOW FILTERING"
  end

  return query
end

local prepared_cache = {}
local function get_prepared_from_cache(query)
   return prepared_cache[query]
end

local function set_prepared_into_cache(query, prepared)
   prepared_cache[query] = prepared
end

local function bind_args(prepared, args)
   local stmt = prepared:bind()
   if nil == stmt then
      return nil, "cannot get a statement from a prepared object: probably oom"
   end
   if nil == args then
      return stmt, nil
   end
   for i = 1, #args do
      local dt = prepared:parameter_data_type(i-1)
      if nil == dt then
	 return nil, query..": more arguments than unbound variables"
      end
      local rc = bind_value_to_stmt(stmt, args[i], dt:type(), i-1)
      if 0 ~= rc then
	 return nil, db.cass_error_desc(rc)
      end
   end
   return stmt, nil
end

local function prepare(sess, query)
   local future = sess:prepare(query)
   future:wait()
   local rc = future:error_code()
   if 0 ~= rc then
      return nil, db.cass_error_desc(rc)
   end
   local prepared = future:get_prepared()
   if nil == prepared then
      return nil, "cannot get a prepared object from future: probably oom"
   end
   return prepared, nil
end

local function attempt_stmt_execution(sess, stmt, opts, paging_state)
   if opts.page_size then
      local rc = stmt:set_paging_size(opts.page_size)
      if 0 ~= rc then
	 return nil, db.cass_error_desc(rc)
      end
   end
   if paging_state then
      local rc = stmt:set_paging_state_token(paging_state)
      if 0 ~= rc then
	 return nil, db.cass_error_desc(rc)
      end
   end

   local rc = stmt:set_consistency(cl(opts.consistency or db.CL.ONE))
   if 0 ~= rc then
      return nil, db.cass_error_desc(rc)
   end

   local future = sess:execute(stmt)
   if nil == future then
      return nil, "cannot get future from session:execute: probably oom"
   end
   future:wait()
   return future, nil
end

local function exec_stmt(sess, stmt, opts, paging_state)
   local future, err = attempt_stmt_execution(sess, stmt, opts, paging_state)
   if err then
      return nil, err
   end
   local rc = future:error_code()
   if 0 ~= rc then
      return nil, db.cass_error_desc(rc)
   end

   local result = future:get_result()
   if nil == result then
      return nil, "nil result from future: probably oom"      
   end

   return result, nil
end

-- should never be called with nil opts
-- opts.page_size
-- opts.consistency
-- opts.prepare
local function full_query_impl(sess, query, args, opts, paging_state)
--   io.stderr:write("full_query_impl: ", query, "\n")
   local stmt = nil
   local just_prepared = false
   if opts.prepare then
      local prepared = get_prepared_from_cache(query)
      if nil == prepared then
	 local err
	 prepared, err = prepare(sess, query)
	 if err then
	    return nil, err
	 end
	 set_prepared_into_cache(query, prepared)
	 just_prepared = true
      end
      local err
      stmt, err = bind_args(prepared, args)
      if err then
	 return nil, err
      end
   else
      stmt = db.cass_statement_new(query, 0)
      if nil == stmt then
	 return nil, "cannot get a statement from a prepared object: probably oom"
      end
   end

   local future, err = attempt_stmt_execution(sess, stmt, opts, paging_state)
   if err then
      return nil, err
   end

   local rc = future:error_code()
   if db.ERRORS.SERVER_UNPREPARED == rc then
      if not opts.prepare or just_prepared then
	 return nil, db.cass_error_desc(rc)
      end
      local prepared, err = prepare(sess, query)
      if err then
	 return nil, err
      end
      set_prepared_into_cache(query, prepared)
      stmt, err = bind_args(prepared, args)
      if err then
	 return nil, err
      end
      return exec_stmt(sess, stmt, opts, paging_state)
   elseif 0 ~= rc then
      return nil, db.cass_error_desc(rc)
   end

   local result = future:get_result()
   if nil == result then
      return nil, "nil result from future: probably oom"      
   end

   return result, nil
end

-- should never be called with nil opts
local function full_query(sess, query, args, opts, schema, paging_state)
   local result, err = full_query_impl(sess, query, args, opts, paging_state)
   if err then
      return nil, err
   end
   return result_to_table(result, schema)
end

-- state must contain non-nil opts
local function page_iterator(state)
   local page = 0
   return function(state, previous_result)
      local paging_state = nil
      if nil ~= previous_result then
	 if not previous_result:has_more_pages() then
	    return nil, nil
	 end
	 local rc
	 rc, paging_state = previous_result:paging_state_token()
	 if 0 ~= rc then
	    return nil, db.cass_error_desc(rc), page
	 end
      end
      page = page + 1
      local r, err = full_query_impl(state.sess, state.query, state.args, state.opts, paging_state)
      return r, err, page
   end, state, nil
end

-- should never be called with nil opts
local function paged_query(sess, query, args, opts, schema)
   local rows = {}
   local err = nil
   for r, page_err in page_iterator({sess = sess, query = query, args = args, opts = opts}) do
      if page_err then
	 err = page_err
	 rows = nil
	 break
      end
      local page, ec = result_to_table(r, schema)
      if ec then
	 err = ec
	 rows = nil
	 break
      end
      for _, row in ipairs(page) do
	 rows[#rows + 1] = row
      end
   end

   return rows, err
end

-- should never be called with nil opts
local function query_with_session(sess, query, args, opts, schema)
   local r, err
   if opts.auto_paging then
      r, err = paged_query(sess, query, args, opts, schema)
   else
      r, err = full_query(sess, query, args, opts, schema)
   end

   if err then
      return nil, err
   end

   return r, nil
end

-- may be called with nil opts
-- but is should call its callees with non-nil opts
function CassBinaryDB:query(query, args, opts, schema, no_keyspace)
--   io.stderr:write("query"..query.."\n")
   local conn_opts = self:_get_conn_options()
   local c, sess, err = init_db(conn_opts, no_keyspace)
   if err then
      return nil, err
   end

   local r, e = query_with_session(sess, query, args, build_opts(conn_opts.query_options, opts), schema)

   cleanup_db(c, sess, singleton_cluster, singleton_session)

   return r, e
end

-- should never be called with nil opts
local function find_impl(sess, opts, table_name, schema, filter_keys)
   local where, args = get_where(schema, filter_keys)
   local query = get_select_query(table_name, where)

   local rows, err = query_with_session(sess, query, args, opts, schema)
   if err then
      return nil, err
   elseif #rows > 0 then
      return rows[1], nil
   end

   return {}, nil
end

function CassBinaryDB:find(table_name, schema, filter_keys)
--   io.stderr:write("find\n")
   local opts = self:_get_conn_options()
   local c, sess, err = init_db(opts)
   if err then
      return nil, err
   end

   local r, e = find_impl(sess, build_opts(opts.query_options, nil), table_name, schema, filter_keys)

   cleanup_db(c, sess, singleton_cluster, singleton_session)

   return r, e
end

local function find_all_impl(sess, opts, table_name, tbl, schema)
   local where, args
   if nil ~= tbl then
      where, args = get_where(schema, tbl)
   end

   local query = get_select_query(table_name, where)
   return query_with_session(sess, query, args, opts, schema)
end

function CassBinaryDB:find_all(table_name, tbl, schema)
--   io.stderr:write("find_all\n")

   local opts = self:_get_conn_options()
   local c, sess, err = init_db(opts)
   if err then
      return nil, err
   end

   local r, e = find_all_impl(sess, build_opts(opts.query_options, {auto_paging = true}), table_name, tbl, schema)

   cleanup_db(c, sess, singleton_cluster, singleton_session)

   return r, e
end

-- should never be called with empty opts
local function find_page_impl(sess, opts, table_name, tbl, paging_state, schema)
   local where, args
   if tbl ~= nil then
      where, args = get_where(schema, tbl)
   end

   local query = get_select_query(table_name, where)
   local result, err = full_query_impl(sess, query, args, opts, paging_state)
   if err then
      return nil, err, nil
   end

   local ps = nil
   if result:has_more_pages() then
      local rc
      rc, ps = result:paging_state_token()
      if 0 ~= rc then
	 return nil, db.cass_error_desc(rc), nil
      end
   end

   local r, e = result_to_table(result, schema)
   if e then
      return nil, e, nil
   end
   return r, e, ps
end

function CassBinaryDB:find_page(table_name, tbl, paging_state, page_size, schema)
--   io.stderr:write("find_page\n")
   local opts = self:_get_conn_options()
   local c, sess, err = init_db(opts)
   if err then
      return nil, err, nil
   end

   local r, e, p = find_page_impl(sess, build_opts(opts.query_options, {page_size = page_size}), table_name, tbl, paging_state, schema)

   cleanup_db(c, sess, singleton_cluster, singleton_session)

   return r, e, p
end

-- should never be called with nil opts
local function check_unique_constraints(sess, opts, table_name, constraints, values, primary_keys, update)
   local errors
   
   for col, constraint in pairs(constraints.unique) do
      -- Only check constraints if value is non-null
      if nil ~= values[col] then
	 local where, args = get_where(constraint.schema, {[col] = values[col]})
	 local query = get_select_query(table_name, where)
	 local rows, err = query_with_session(sess, query, args, opts, constraint.schema)
	 if err then
	    return err
	 elseif #rows > 0 then
	    -- if in update, it's fine if the retrieved row is the same as the one updated
	    if update then
	       local same_row = true
	       for col, val in pairs(primary_keys) do
		  if val ~= rows[1][col] then
		     same_row = false
		     break
		  end
	       end
	       
	       if not same_row then
		  errors = utils.add_error(errors, col, values[col])
	       end
	    else
	       errors = utils.add_error(errors, col, values[col])
	    end
	 end
      end
   end

   return Errors.unique(errors)
end

-- should never be called with nil opts
local function check_foreign_constaints(sess, opts, values, constraints)
   local errors
   
   for col, constraint in pairs(constraints.foreign) do
      -- Only check foreign keys if value is non-null, if must not be null, field should be required
      if nil ~= values[col] then
	 local res, err = find_impl(sess, opts, constraint.table, constraint.schema, {[constraint.col] = values[col]})
	 if err then
	    return err
	 elseif nil == res then
	    errors = utils.add_error(errors, col, values[col])
	 end
      end
   end
   
   return Errors.foreign(errors)
end

-- should never be called with nil opts
local function insert_impl(sess, opts, table_name, schema, model, constraints)
   local err = check_unique_constraints(sess, opts, table_name, constraints, model)
   if err then
      return nil, err
   end
   
   err = check_foreign_constaints(sess, opts, model, constraints)
   if err then
      return nil, err
   end
   
   local cols, binds, args = {}, {}, {}
   for col, value in pairs(model) do
      local field = schema.fields[col]
      cols[#cols + 1] = col
      args[#args + 1] = serialize_arg(field, value)
      binds[#binds + 1] = "?"
   end
   
   cols = table.concat(cols, ", ")
   binds = table.concat(binds, ", ")
   
   local query = string.format("INSERT INTO %s(%s) VALUES(%s)%s",
			       table_name, cols, binds, opts.ttl and string.format(" USING TTL %d", opts.ttl) or "")
   local err = select(2, query_with_session(sess, query, args, opts))
   if err then
      return nil, err
   end

   local primary_keys = model:extract_keys()
   local row, err = find_impl(sess, opts, table_name, schema, primary_keys)
   if err then
      return nil, err
   end
   
   return row, nil
end

-- may be called with nil options but it must call its callees with non-nil opts
function CassBinaryDB:insert(table_name, schema, model, constraints, opts)
--   io.stderr:write("insert\n")
   local conn_opts = self:_get_conn_options()
   local c, sess, err = init_db(conn_opts)
   if err then
      return nil, err
   end

   local r, e = insert_impl(sess, build_opts(conn_opts.query_options, opts), table_name, schema, model, constraints)

   cleanup_db(c, sess, singleton_cluster, singleton_session)

   return r, e
end

local function update_impl(sess, opts, table_name, schema, constraints, filter_keys, values, nils, full, model)
  -- must check unique constaints manually too
  local err = check_unique_constraints(sess, opts, table_name, constraints, values, filter_keys, true)
  if err then
    return nil, err
    end
  err = check_foreign_constaints(sess, opts, values, constraints)
  if err then
    return nil, err
  end

  -- Cassandra TTL on update is per-column and not per-row, and TTLs cannot be updated on primary keys.
  -- Not only that, but TTL on other rows can only be incremented, and not decremented. Because of all
  -- of these limitations, the only way to make this happen is to do an upsert operation.
  -- This implementation can be changed once Cassandra closes this issue: https://issues.apache.org/jira/browse/CASSANDRA-9312
  if opts.ttl then
    if schema.primary_key and 1 == #schema.primary_key and filter_keys[schema.primary_key[1]] then
      local row, err = find_impl(sess, opts, table_name, schema, filter_keys)
      if err then
        return nil, err
      elseif row then
        for k, v in pairs(row) do
          if not values[k] then
            model[k] = v -- Populate the model to be used later for the insert
          end
        end

        -- Insert without any contraint check, since the check has already been executed
        return insert_impl(sess, opts, table_name, schema, model, {unique={}, foreign={}})
      end
    else
      return nil, "Cannot update TTL on entities that have more than one primary_key"
    end
  end

  local sets, args, where = {}, {}
  for col, value in pairs(values) do
    local field = schema.fields[col]
    sets[#sets + 1] = col.." = ?"
    args[#args + 1] = serialize_arg(field, value)
  end

  -- unset nil fields if asked for
  if full then
    for col in pairs(nils) do
      sets[#sets + 1] = col.." = ?"
      args[#args + 1] = nil
    end
  end

  sets = table.concat(sets, ", ")

  where, args = get_where(schema, filter_keys, args)
  local query = string.format("UPDATE %s%s SET %s WHERE %s",
                              table_name, opts.ttl and string.format(" USING TTL %d", opts.ttl) or "", sets, where)
  local res, err = query_with_session(sess, query, args, opts)
  if err then
    return nil, err
  end
  return find_impl(sess, opts, table_name, schema, filter_keys)
end

-- may be called with nil options but it must call its callees with non-nil opts
function CassBinaryDB:update(table_name, schema, constraints, filter_keys, values, nils, full, model, opts)
--   io.stderr:write("update\n")
   local conn_opts = self:_get_conn_options()
   local c, sess, err = init_db(conn_opts)
   if err then
      return nil, err
   end

   local r, e = update_impl(sess, build_opts(conn_opts.query_options, opts), table_name, schema, constraints, filter_keys, values, nils, full, model)
   cleanup_db(c, sess, singleton_cluster, singleton_session)

   return r, e
end

-- should never be called with nil opts
local function count_impl(sess, opts, table_name, tbl, schema)
   local where, args
   if tbl ~= nil then
      where, args = get_where(schema, tbl)
   end
   
   local query = get_select_query(table_name, where, "COUNT(*)")
   local r, err = query_with_session(sess, query, args, opts, schema)
   if err then
      return nil, err
   elseif r and #r > 0 then
      return r[1].count
   end

   return 0, nil
end

function CassBinaryDB:count(table_name, tbl, schema)
--   io.stderr:write("count\n")
   local opts = self:_get_conn_options()
   local c, sess, err = init_db(opts)
   if err then
      return nil, err
   end

   local r, e = count_impl(sess, build_opts(opts.query_options, nil), table_name, tbl, schema)

   cleanup_db(c, sess, singleton_cluster, singleton_session)

   return r, e
end

local delete_impl
local function cascade_delete(sess, opts, primary_keys, constraints)
   if nil == constraints.cascade then
      return
   end
   
   for f_entity, cascade in pairs(constraints.cascade) do
      local tbl = {[cascade.f_col] = primary_keys[cascade.col]}
      local rows, err = find_all_impl(sess, opts, cascade.table, tbl, cascade.schema)
      if err then
	 return nil, err
      end
      
      for _, row in ipairs(rows) do
	 local primary_keys_to_delete = {}
	 for _, primary_key in ipairs(cascade.schema.primary_key) do
	    primary_keys_to_delete[primary_key] = row[primary_key]
	 end
	 
	 local ok, err = delete_impl(sess, opts, cascade.table, cascade.schema, primary_keys_to_delete)
	 if not ok then
	    return nil, err
	 end
      end
   end
end

local delete_impl = function(sess, opts, table_name, schema, primary_keys, constraints)
   local row, err = find_impl(sess, opts, table_name, schema, primary_keys)
   if err then
      return nil, err
   end
   
   local where, args = get_where(schema, primary_keys)
   local query = string.format("DELETE FROM %s WHERE %s",
			       table_name, where)
   local res, err = query_with_session(sess, query, args, opts)

   if err then
      return nil, err
   end
   if nil ~= constraints then
      cascade_delete(sess, opts, primary_keys, constraints)
   end
   return row, nil
end

function CassBinaryDB:delete(table_name, schema, primary_keys, constraints)
--   io.stderr:write("delete\n")
   local opts = self:_get_conn_options()
   local c, sess, err = init_db(opts)
   if err then
      return nil, err
   end

   local r, e = delete_impl(sess, build_opts(opts.query_options, nil), table_name, schema, primary_keys, constraints)

   cleanup_db(c, sess, singleton_cluster, singleton_session)

   return r, e
end

-- should never be called with nil opts
local function queries_impl(sess, opts, queries)
   for _, query in ipairs(utils.split(queries, ";")) do
      if utils.strip(query) ~= "" then
	 local err = select(2, query_with_session(sess, query, nil, opts))
	 if err then
	    return err
	 end
      end
   end
   return nil
end

function CassBinaryDB:queries(queries, no_keyspace)
--   io.stderr:write("queries\n")
   local opts = self:_get_conn_options()
   local c, sess, err = init_db(opts, no_keyspace)
   if err then
      return err
   end

   local e = queries_impl(sess, build_opts(opts.query_options, {prepare = false, consistency = db.CL.QUORUM}), queries)

   cleanup_db(c, sess, singleton_cluster, singleton_session)

   return e
end

function CassBinaryDB:drop_table(table_name)
--   io.stderr:write("drop_table\n")
   local opts = self:_get_conn_options()
   local c, sess, err = init_db(opts)
   if err then
      return err
   end

   local e = select(2, query_with_session(sess, "DROP TABLE "..table_name, nil, build_opts(opts.query_options, {prepare = false, consistency = db.CL.QUORUM})))

   cleanup_db(c, sess, singleton_cluster, singleton_session)

   return e
end

function CassBinaryDB:truncate_table(table_name)
--   io.stderr:write("truncate_table\n")
   local opts = self:_get_conn_options()
   local c, sess, err = init_db(opts)
   if err then
      return err
   end

   local e = select(2, query_with_session(sess, "TRUNCATE "..table_name, nil, build_opts(opts.query_options, {prepare = false, consistency = db.CL.QUORUM})))

   cleanup_db(c, sess, singleton_cluster, singleton_session)

   return e
end

-- should never be called with nil opts
local function current_migrations_impl(sess, opts)
   -- Check if keyspace exists
   local result, err = query_with_session(sess, [[
    SELECT * FROM system.schema_keyspaces WHERE keyspace_name = ?
  ]], {opts.keyspace}, build_opts(opts, {prepare = true}))
   if err then
      return nil, err
   elseif 0 == #result then
      return {}
   end

   -- Check if schema_migrations table exists first
   result, err = query_with_session(sess, [[
    SELECT COUNT(*) FROM system.schema_columnfamilies
    WHERE keyspace_name = ? AND columnfamily_name = ?
  ]], {opts.keyspace, "schema_migrations"}, build_opts(opts, {prepare = true}))
   if err then
      return nil, err
   end

   if 0 == #result or result[1].count <= 0 then
      return {}
   end
   return query_with_session(sess, "SELECT * FROM kong.schema_migrations", nil, build_opts(opts, {prepare = false}))
end

function CassBinaryDB:current_migrations()
--   io.stderr:write("current_migrations\n")
   local opts = self:_get_conn_options()
   local c, sess, err = init_db(opts, true)
   if err then
      return nil, err
   end

   local r, e = current_migrations_impl(sess, build_opts(opts.query_options, {consistency = db.CL.QUORUM}))

   cleanup_db(c, sess, singleton_cluster, singleton_session)

   return r, e
end

-- should never be called with nil opts
local function record_migration_impl(sess, opts, id, name)
   local st = db.cass_data_type_new(db.TYPES.VALUE_TYPE_LIST)
   if nil == st then
      return nil, "cannot create data type: possibly oom"
   end
   local names = st:new_collection(1)
   if nil == names then
      return nil, "cannot create collection: possibly oom"
   end
   local rc = names:append_string(name)
   if 0 ~= rc then
      return nil, db.cass_error_desc(rc)
   end

   return select(2, query_with_session(sess, "UPDATE schema_migrations SET migrations = migrations + ? WHERE id = ?",
				       {names, id}, build_opts(opts, {prepare = true})))
end

function CassBinaryDB:record_migration(id, name)
--   io.stderr:write("record_migration\n")

   local opts = self:_get_conn_options()
   local c, sess, err = init_db(opts)
   if err then
      return err
   end

   local e = record_migration_impl(sess, build_opts(opts.query_options, {consistency = db.CL.QUORUM}), id, name)

   cleanup_db(c, sess, singleton_cluster, singleton_session)

   return e
end

return CassBinaryDB

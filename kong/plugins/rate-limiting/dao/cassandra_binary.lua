local CassBinaryDB = require "kong.dao.cassandra_binary_db"
local timestamp = require "kong.tools.timestamp"

local _M = CassBinaryDB:extend()

_M.table = "ratelimiting_metrics"
_M.schema = require("kong.plugins.response-ratelimiting.schema")

function _M:increment(api_id, identifier, current_timestamp, value)
   local periods = timestamp.get_timestamps(current_timestamp)

   local ok = true
   for period, period_date in pairs(periods) do
      local r, err = self:query([[
	UPDATE ratelimiting_metrics SET value = value + ? WHERE
		api_id = ? AND
		identifier = ? AND
		period_date = ? AND
		period = ?
	]],
	 {
	    value,
	    api_id,
	    identifier,
	    period_date,
	    period
	 }, {prepare = true})
      if err then
	 ok = false
	 ngx.log(ngx.ERR, "[rate-limiting] could not increment counter for period '"..period.."': "..tostring(err))
      end
   end

   return ok
end

function _M:find(api_id, identifier, current_timestamp, period)
   local periods = timestamp.get_timestamps(current_timestamp)
   local rows, err = self:query([[
	SELECT * FROM ratelimiting_metrics WHERE
		api_id = ? AND
		identifier = ? AND
		period_date = ? AND
		period = ?
	]],
      {
	 api_id,
	 identifier,
	 periods[period],
	 period
      }, {prepare = true})
   if err then
      return nil, err
   elseif #rows <= 0 then
      return nil, nil
   end

   return rows[1], nil
end

function _M:count()
  return _M.super.count(self, _M.table, nil, _M.schema)
end

return {ratelimiting_metrics = _M}

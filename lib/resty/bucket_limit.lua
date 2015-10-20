-- @author homeway
-- @link http://homeway.me
-- @version 2015.10.19

local _M = { _VERSION = "2015.10.19", OK = 1, BUSY = 2, FORBIDDEN = 3 }

function _M.is_black_list(red, ip)
	local ok, err = red:sismember("black-list", ip)
	if not ok then
		-- redis error, return 503
		ngx.log(ngx.WARN, "redis smember error: ", err)
		return nil, err
	elseif ok == 0 then
		-- in black list, return ok
		return _M.OK, nil
	end
	return nil, err
end

function _M.need_limit_traffic(red, uri)
	local ok, err = red:hmget("token_bucket_uri", uri)
	if not ok then
		-- redis error, return 503
		ngx.log(ngx.WARN, "redis hash get error: ", err)
		return ok, err
	end
	return ok, nil
end

function _M.limit(cfg)
	if not cfg.conn then
		local ok, redis = pcall(require, "resty.redis")
		if not ok then
			ngx.log(ngx.ERR, "failed to require redis")
			return _M.OK
		end

		local rds = cfg.rds or {}
		rds.timeout = rds.timeout or 1
		rds.host = rds.host or "127.0.0.1"
		rds.port = rds.port or 6379

		local red = redis:new()

		red:set_timeout(rds.timeout * 1000)

		local ok, err = red:connect(rds.host, rds.port)
		if not ok then
			ngx.log(ngx.WARN, "redis connect err: ", err)
			return _M.OK
		end
		cfg.conn = red
	end

	local conn = cfg.conn
	local zone = cfg.zone or "limit_req"
	local key = cfg.key or ngx.var.remote_addr
	local rate = cfg.rate or "1r/s"
	local interval = cfg.interval or 0
	local log_level = cfg.log_level or ngx.NOTICE

	local scale = 1
	local len = #rate

	if len > 3 and rate:sub(len - 2) == "r/s" then
		scale = 1
		rate = rate:sub(1, len - 3)
	elseif len > 3 and rate:sub(len - 2) == "r/m" then
		scale = 60
		rate = rate:sub(1, len - 3)
	end

	-- [[ kill black list traffic ]]
	local ok, err = _M.is_black_list(cfg.conn, ngx.var.remote_addr)
	if not ok then
		return nil
	end

	-- [[ check need limit traffic ]]
	local ok, error = _M.need_limit_traffic(cfg.conn, ngx.var.uri)
	if not ok then
		return nil
	elseif type(ok) == "number" then
		local rate = ok
		ngx.say(ok)
	end
	ngx.say(ok)

	return _M.OK
end

return _M
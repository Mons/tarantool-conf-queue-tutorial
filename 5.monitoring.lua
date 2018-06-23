require'strict'.on()
fiber = require'fiber'
clock = require'clock'
log   = require'log'

box.cfg{
	listen = 'localhost:3311'
}
box.once('access:v1', function()
	box.schema.user.grant('guest', 'read,write,execute', 'universe')
end)

box.schema.create_space('queue',{
	format = {
		{ name = 'id';     type = 'number' },
		{ name = 'status'; type = 'string' },
		{ name = 'runat';  type = 'number' },
		{ name = 'data';   type = '*'      },
	};
	if_not_exists = true;
})

local F = {
	id     = 1;
	status = 2;
	runat  = 3;
	data   = 4;
}

STATUS = {}
STATUS.READY = 'R'
STATUS.TAKEN = 'T'
STATUS.WAITING = 'W'

box.space.queue:create_index('primary', {
	parts = {1,'number'};
	if_not_exists = true;
})

box.space.queue:create_index('status', {
	parts = {2, 'string', 1, 'number'};
	if_not_exists = true;
})

box.space.queue:create_index('runat', {
	parts = {3, 'number', 1, 'number'};
	if_not_exists = true;
})


queue = {}

do
	local c = 0
	for _,t in box.space.queue.index.status:pairs({STATUS.TAKEN}) do
		box.space.queue:update({t.id},{{'=', F.status, STATUS.READY }})
		c = c + 1
	end
	log.info("Autorelease %d tasks at start",c)
end

queue._stats = {
	R = 0LL,
	T = 0LL,
	W = 0LL,
}

log.info("Precalculate stats")
for _,t in box.space.queue:pairs() do
	queue._stats[ t[F.status] ]
		= (queue._stats[ t[F.status] ] or 0LL)+1
end

local msgpack = require 'msgpack'
local function keypack( key )
	return msgpack.encode(key)
end
local function keyunpack( data )
	return msgpack.decode(data)
end

local log = require 'log'
box.session.on_connect(function()
	log.info("connected %s:%s from %s", box.session.id(), box.session.user(), box.session.peer())
	box.session.storage.peer = box.session.peer()
end)

box.session.on_disconnect(function()
	log.info("disconnected %s:%s from %s", box.session.id(), box.session.user(), box.session.storage.peer)
	local sid = box.session.id()
	local bysid = queue.bysid[ sid ]
	if bysid then
		for key in pairs(bysid) do
			log.info("Autorelease %s by disconnect",keyunpack(key));
			queue.release( keyunpack(key) )
		end
		queue.bysid[ sid ] = nil
	end
end)

queue.taken = {};
queue.bysid = {};

local clock = require 'clock'
local function gen_id()
	local new_id = clock.monotonic64()/1e3
	while box.space.queue:get(new_id) do
		new_id = new_id + 1
	end
	return new_id
end

while true do
	local t = box.space.queue.index.status:pairs({STATUS.TAKEN}):nth(1)
	if not t then break end
	box.space.queue:update({t.id},{{'=', F.status, STATUS.READY }})
	log.info("Autireleased %s at start", t.id)
end

local fiber = require 'fiber'

queue.wait = fiber.channel(0)

function queue.put(data,opts)
	local id = gen_id()
	local runat = 0
	local status = STATUS.READY
	if opts and opts.delay then
		-- if queue.runch:has_readers() then queue.runch:put(true,0) end
		runat = clock.monotonic() + tonumber(opts.delay)
		status = STATUS.WAITING
	else
		if queue.wait:has_readers() then queue.wait:put(true,0) end
	end
	queue._stats[ status ] = queue._stats[ status ] + 1
	return box.space.queue:insert{ id, status, runat, unpack(data) }
end

-- queue.runch = fiber.channel()
queue.runat = fiber.create(function()
	fiber.name('queue.runat')
	while true do
		local now = clock.monotonic()
		local remaining
		for _,t in box.space.queue.index.runat:pairs({0},{iterator = box.index.GT}) do
			if t.runat > now then
				remaining = t.runat - now
				break
			else
				if t.status == STATUS.WAITING then
					log.info("Runat: W->R %s",t.id)
					if queue.wait:has_readers() then queue.wait:put(true,0) end
					box.space.queue:update({t.id},{
						{'=', F.status, STATUS.READY },
						{'=', F.runat, 0 },
					})
					queue._stats[ STATUS.WAITING ] = queue._stats[ STATUS.WAITING ] + 1
					queue._stats[ STATUS.READY ] = queue._stats[ STATUS.READY ] + 1
				else
					log.error("Runat: bad status %s for %s",t.status, tostring(t))
					box.space.queue:update({t.id},{{'=', F.runat, 0 }})
				end
			end
		end
		if not remaining or remaining > 1 then remaining = 1 end
		fiber.sleep(remaining)
		-- queue.runch:get(remaining)
	end
end)

function queue.take(timeout)
	if not timeout then timeout = 0 end
	local now = fiber.time()
	local found
	while not found do
		for _,t in box.space.queue.index.status:pairs({STATUS.READY},{ iterator = box.index.EQ }) do
			found = t
			break
		end
		if not found then
			local left = (now + timeout) - fiber.time()
			if left <= 0 then return end
			queue.wait:get(left)
		end
	end

	local sid = box.session.id()
	local packid = keypack(found.id)
	log.info("Register %s by %s",found.id, sid)
	queue.bysid[ sid ] = queue.bysid[ sid ] or {}
	queue.taken[ packid ] = sid
	queue.bysid[ sid ][ packid ] = true

	queue._stats[ STATUS.READY ] = queue._stats[ STATUS.READY ] - 1
	queue._stats[ STATUS.TAKEN ] = queue._stats[ STATUS.TAKEN ] + 1

	return box.space.queue:update({found.id},{{'=', F.status, STATUS.TAKEN }})
end

function queue.get_task(id)
	if not id then error("Task id required",2) end
	local key = tonumber64(id)
	if not key then error(string.format("Task id '%s' is wrong",id),2) end
    local t = box.space.queue:get{key}
    if not t then
        error(string.format( "Task {%s} was not found", key ),2)
    end
	local packid = keypack(t.id)
    if not queue.taken[packid] then
        error(string.format( "Task %s not taken by any", key ),2)
    end
    if queue.taken[packid] ~= box.session.id() then
        error(string.format( "Task %s taken by %d. Not you (%d)",
        	key, queue.taken[packid], box.session.id() ),2)
    end
    return t, packid
end

function queue.ack(id)
	local t,packid = queue.get_task(id)
	queue.taken[ packid ] = nil
	queue.bysid[ box.session.id() ][ packid ] = nil
	queue._stats[ t[F.status] ] = queue._stats[ t[F.status] ] - 1
	return box.space.queue:delete{t.id}
end

function queue.release(id)
	local t,packid = queue.get_task(id)
	queue.taken[ packid ] = nil
	queue.bysid[ box.session.id() ][ packid ] = nil
	if queue.wait:has_readers() then queue.wait:put(true,0) end
	queue._stats[ t[F.status] ] = queue._stats[ t[F.status] ] - 1
	queue._stats[ STATUS.READY ] = queue._stats[ STATUS.READY ] + 1
	return box.space.queue:update({t.id},{{'=', F.status, STATUS.READY }})
end

function queue.stats()
	return {
		total   = box.space.queue:len(),
		ready   = queue._stats[STATUS.READY] or 0,
		waiting = queue._stats[STATUS.WAITING] or 0,
		taken   = queue._stats[STATUS.TAKEN] or 0,
		-- ready   = box.space.queue.index.status:count({STATUS.READY}),
		-- waiting = box.space.queue.index.status:count({STATUS.WAITING}),
		-- taken   = box.space.queue.index.status:count({STATUS.TAKEN}),
	}
end

require'console'.start()
os.exit()

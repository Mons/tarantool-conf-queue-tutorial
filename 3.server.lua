require'strict'.on()
fiber = require'fiber'

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
		{ name = 'data';   type = '*'      },
	};
	if_not_exists = true;
})

local F = {
	id     = 1;
	status = 2;
	data   = 3;
}

box.space.queue:create_index('primary', {
	parts = {1,'number'};
	if_not_exists = true;
})

box.space.queue:create_index('status', {
	parts = {2, 'string', 1, 'number'};
	if_not_exists = true;
})


queue = {}

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

STATUS = {}
STATUS.READY = 'R'
STATUS.TAKEN = 'T'

while true do
	local t = box.space.queue.index.status:pairs({STATUS.TAKEN}):nth(1)
	if not t then break end
	box.space.queue:update({t.id},{{'=', F.status, STATUS.READY }})
	log.info("Autireleased %s at start", t.id)
end

local fiber = require 'fiber'

queue.wait = fiber.channel(0)

function queue.put(...)
	local id = gen_id()
	if queue.wait:has_readers() then queue.wait:put(true,0) end
	-- queue.wait:put(true,0)
	return box.space.queue:insert{ id, STATUS.READY, ... }
end

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
	return box.space.queue:delete{t.id}
end

function queue.release(id)
	local t,packid = queue.get_task(id)
	queue.taken[ packid ] = nil
	queue.bysid[ box.session.id() ][ packid ] = nil
	if queue.wait:has_readers() then queue.wait:put(true,0) end
	return box.space.queue:update({t.id},{{'=', F.status, STATUS.READY }})
end

require'console'.start()
os.exit()

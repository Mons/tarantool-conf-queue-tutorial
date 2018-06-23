require'strict'.on()

box.cfg{}

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

function queue.put(...)
	local id = gen_id()
	return box.space.queue:insert{ id, STATUS.READY, ... }
end

function queue.take(...)
	for _,t in box.space.queue.index.status:pairs({ STATUS.READY }, { iterator=box.index.EQ }) do
		return box.space.queue:update({t.id},{{'=', F.status, STATUS.TAKEN }})
	end
	return
end

function queue.ack(id)
	local t = box.space.queue:get{id}
	if t and t.status == STATUS.TAKEN then
		return box.space.queue:delete{t.id}
	elseif t then
		error("Task not taken")
	else
		error("Task not exists")
	end
end

function queue.release(id)
	local t = box.space.queue:get{id}
	if t and t.status == STATUS.TAKEN then
		return box.space.queue:update({t.id},{{'=', F.status, STATUS.READY }})
	elseif t then
		error("Task not taken")
	else
		error("Task not exists")
	end
end

require'console'.start()
os.exit()

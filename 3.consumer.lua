local netbox = require 'net.box'
local conn = netbox.connect('localhost:3311')
local yaml = require 'yaml'

while true do
	local task = conn:call('queue.take',{0})
	-- print(yaml.encode(task))
	if task then
		-- conn:call('os.exit',{})
		-- os.exit()
		-- conn:call('queue.release',{task[1]})
	end
end
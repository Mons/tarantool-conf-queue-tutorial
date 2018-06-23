local netbox = require 'net.box'
local conn = netbox.connect('localhost:3311')
local yaml = require 'yaml'

local res = conn:call('queue.put',{unpack(arg)})
print(yaml.encode(res))

conn:close()

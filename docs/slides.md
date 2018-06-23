layout:true

```
┌──┬──┐
│  │  │╿
├──┼──┤╽
│  │  │╫
└──┴──┘
┌──┴──┘
└──┬──┐
│
← ↑ → ↓
↔ ↕
↖ ↗ ↘ ↙
▲ ▼ ◀ ▶
┐
│
├
│
┘

16:9:
123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456
```

---
class: first
layout:false

.absolute.width80.left8.top8[
# Rich tarantool application<br>from scratch

## Mons Anderson
### Mail.Ru Cloud Solutions
]

---
class:center middle

.extra-huge[
◀◀
]

---

.absolute.bottom3.left51[
**kostja**: Implement beanstalkd-like queues.<br>
`f879dfb` on *Oct 3, 2012*
]

--

.left.extra-small[
```lua
box.queue = {}
local box_queue_id = 0

local c_id = 0
local c_fid = 1
local c_state = 2
local c_count = 3

local function lshift(number, bits)
   return number * 2^32
end

local function rshift(number, bits)
   return number / 2^32
end

function box.queue.id(delay)
   box_queue_id = box_queue_id + 2^12 
   return tonumber64(lshift(os.time() + delay) + bit.tobit(box_queue_id))
end

function box.queue.put(sno, delay, ...)
   sno, delay = tonumber(sno), tonumber(delay)
   local id = box.queue.id(delay)
   return box.insert(sno, id, 0, 'R', 0, ...)
end

function box.queue.push(sno, ...)
   sno = tonumber(sno)
   local minid = box.unpack('l', box.space[sno].index[0]:min()[0])
   local id = lshift(rshift(minid) - 1)
   return box.insert(sno, id, 0, 'R', 0, ...)
end

function box.queue.delete(sno, id)
   sno, id = tonumber(sno), tonumber64(id)
   return box.space[sno]:delete(id)
end

function box.queue.take(sno, timeout)
   sno, timeout = tonumber(sno), tonumber(timeout)
   if timeout == nil then
      timeout = 60*60*24*365
   end
   local task = nil
   while true do
      task = box.select(sno, 1, 0)
      if task ~= nil then
         break
      end
      if timeout > 0 then
         box.fiber.sleep(1)
         timeout = timeout - 1
      else
         return nil
      end
   end
   return box.update(sno, task[0], "=p=p+p",
                 c_fid, box.fiber.id(),
                 c_state, 'T', -- taken
                 c_count, 1)
end
```
]
.right.extra-small[
```lua
local function consumer_find_task(sno, id)
   local task = box.select(sno, 0, id)
   if task == nil then
      error("task not found")
   end
   if task[c_fid] ~= box.fiber.id() then
      error("the task does not belong to the consumer")
   end
   return task
end

function box.queue.ack(sno, id)
   sno, id = tonumber(sno), tonumber64(id)
   local task = consumer_find_task(sno, id)
   box.space[sno]:delete(id)
end

function box.queue.release(sno, id, delay)
   sno, id, delay = tonumber(sno), tonumber64(id), tonumber(delay)
   local task = consumer_find_task(sno, id)
   local newid = box.queue.id(delay)
   return box.update(sno, id, "=p=p=p", c_id, newid, c_fid, 0, c_state, 'R')
end

local function queue_expire(sno)
   box.fiber.detach()
   box.fiber.name("box.queue "..sno)
   local idx = box.space[sno].index[1].idx
   while true do
      local i = 0
      for it, tuple in idx.next, idx, box.fiber.id() do
         fid  = tuple[c_fid]
         if box.fiber.find(fid) == nil then
            local id = tuple[0]
            box.update(sno, id, "=p=p", c_fid, 0, c_state, 'R')
            break
         end
         i = i + 1
         if i == 100 then
            box.fiber.sleep(1)
            i = 0
         end
      end
      box.fiber.sleep(1)
   end
end

function box.queue.start(sno)
   sno = tonumber(sno)
   box.fiber.resume(box.fiber.create(queue_expire), sno)
end
```
]

---
class:center middle

.extra-huge[
▶▶
]

---

# init.lua

```lua
box.cfg{}
```

--

```lua
box.schema.create_space('queue',{})
```

---

# init.lua

```lua
box.cfg{}
```

```lua
box.schema.create_space('queue',{
   if_not_exists = true;
})
```

---

# init.lua

```lua
box.cfg{}
```

```lua
box.schema.create_space('queue',{
   format = {
      { name = 'id';     type = 'number' },
      { name = 'status'; type = 'string' },
      { name = 'data';   type = '*'      },
   };
   if_not_exists = true;
})
```

--

```lua
box.space.queue:create_index('primary', {
   parts = {1,'number'};
   if_not_exists = true;
})
```

---

# init.lua

```lua
box.cfg{}
```

```lua
box.schema.create_space('queue',{
   format = {
      { name = 'id';     type = '`number`' },
      { name = 'status'; type = 'string' },
      { name = 'data';   type = '*'      },
   };
   if_not_exists = true;
})
```

```lua
box.space.queue:create_index('primary', {
   parts = {1,'`number`'};
   if_not_exists = true;
})
```

---

# init.lua

```lua
box.cfg{}
```

```lua
box.schema.create_space('queue',{
   format = {
      { name = 'id';     type = 'number' },
      { name = 'status'; type = 'string' },
      { name = 'data';   type = '*'      },
   };
   if_not_exists = true;
})
```

```lua
box.space.queue:create_index('primary', {
   parts = {1,'number'};
   if_not_exists = true;
})
```

```lua
require'console'.start()
os.exit()
```

---

# init.lua

```terminal
$ tarantool init.lua
```

```terminal
main/101/init.lua C> Tarantool 1.9.0-83-g4158922
main/101/init.lua C> log level 5
main/101/init.lua I> mapping 268435456 bytes for memtx tuple arena...
main/101/init.lua I> mapping 134217728 bytes for vinyl tuple arena...
main/101/init.lua I> initializing an empty data directory
snapshot/101/main I> saving snapshot ./00000000000000000000.snap.inprogress
snapshot/101/main I> done
main/101/init.lua I> ready to accept requests
main/104/checkpoint_daemon I> started
tarantool>
```

---

# queue.lua

```lua
require'strict'.on()
```


--


```lua
queue = {}

function queue.put(...)

end

function queue.take(...)

end
```

---

# queue.lua

```lua
require'strict'.on()
```

```lua
STATUS = {}
STATUS.READY = 'R'
STATUS.TAKEN = 'T'
```

```lua
queue = {}

function queue.put(...)

end

function queue.take(...)

end
```

---

# queue.put

```lua
function queue.put(...)
   local id = gen_id()
   return box.space.queue:insert{ id, STATUS.READY, ... }
end
```

---

# queue.put

```lua
local clock = require 'clock'
local function gen_id()
   local new_id = clock.monotonic64()/1e3
   while box.space.queue:get(new_id) do
      new_id = new_id + 1
   end
   return new_id
end
```

```lua
function queue.put(...)
   local id = gen_id()
   return box.space.queue:insert{ id, STATUS.READY, ... }
end
```

---

# queue.put

```yaml
tarantool> queue.put("aaa")
---
- [1529262394933356, 'R', 'aaa']
...

tarantool> queue.put("my","data")
---
- [1529262398900772, 'R', 'my', 'data']
...
tarantool> queue.put({complex = {struct = 1}})
---
- [1529262413142216, 'R', {'complex': {'struct': 1}}]
...
```

---

# queue.put

```yaml
tarantool> box.space.queue:select()
---
- - [1529262394933356, 'R', 'aaa']
  - [1529262398900772, 'R', 'my', 'data']
  - [1529262413142216, 'R', {'complex': {'struct': 1}}]
...
```

---

# queue.take

```lua
box.space.queue:create_index('status', {
   parts = {2, 'string', 1, 'number'};
   if_not_exists = true;
})
```

--

```lua
function queue.take(...)
   for _,t in
      box.space.queue.index.status
      :pairs({ STATUS.READY }, { iterator=box.index.EQ })
   do
      return box.space.queue:update({t.id},{
         {'=', 2, STATUS.TAKEN }
      })
   end
   return
end
```

---

# queue.take

```lua
box.space.queue:create_index('status', {
   parts = {2, 'string', 1, 'number'};
   if_not_exists = true;
})
```

```lua
function queue.take(...)
   for _,t in
      box.space.queue.index.status
      :pairs({ STATUS.READY }, { iterator=box.index.EQ })
   do
      return box.space.queue:update({t.id},{
         {'=', `2`, STATUS.TAKEN }
      })
   end
   return
end
```

---

# queue.take

```lua
local F = {
   id     = 1;
   status = 2;
   data   = 3;
}
```

```lua
function queue.take(...)
   for _,t in
      box.space.queue.index.status
      :pairs({ STATUS.READY }, { iterator=box.index.EQ })
   do
      return box.space.queue:update({t.id},{
         {'=', `F.status`, STATUS.TAKEN }
      })
   end
   return
end
```


---

# put + take

```yaml
tarantool> queue.put("task 1")
---
- [1529262869440082, 'R', 'task 1']
...
```
--
```yaml
tarantool> queue.take()
---
- [1529262869440082, 'T', 'task 1']
...
```
--
```yaml
tarantool> queue.take()
---
...
```

---

# Task return: release + ack

```lua
function queue.ack(id)
   local t = assert(box.space.queue:get{id},"Task not exists")
   if t and t.status == STATUS.TAKEN then
      return box.space.queue:delete{t.id}
   else
      error("Task not taken")
   end
end

function queue.release(id)
   local t = assert(box.space.queue:get{id},"Task not exists")
   if t and t.status == STATUS.TAKEN then
      return box.space.queue:update({t.id},{{'=', F.status, STATUS.READY }})
   else
      error("Task not taken")
   end
end
```

---

```yaml
tarantool> queue.put("task 1")
- [1529263926863733, 'R', 'task 1']

tarantool> queue.put("task 2")
- [1529263928104079, 'R', 'task 2']
```

--

```yaml
tarantool> queue.take()
- [1529263926863733, 'T', 'task 1']
```

--

```yaml
tarantool> queue.release(1529263926863733)
- [1529263926863733, 'R', 'task 1']

```

--

```yaml
tarantool> queue.take()
- [1529263926863733, 'T', 'task 1']

tarantool> queue.ack(1529263926863733)
- [1529263926863733, 'T', 'task 1']
```

--

```yaml
tarantool> queue.take()
- [1529263928104079, 'T', 'task 2']

```

---

# Consumer

```lua
while true do
   local task = queue.take()
   if task then
      -- ...
   end
end
```

--

.center[
# 10K RPS of CPU heating
]

---

# Channel

```nohighlight



             ┌─────────┐                               ┌─────────┐
             │ fiber 1 │                               │ fiber 2 │
             └─────────┘                               └─────────┘
                 │               ┌────────────┐            │
                 │               │ channel(2) │            │
                 │               ├────────────┤            │
                 ├─put: ok─────> │ (.......)  │ <----- get─┘
                 ├─put: ok─────> │ [_______]  │
                 ├─put: ok─────> │ [_______]  │
                 └─put: blocks─X │            │
                                 └────────────┘
```

---

# Channel

```lua
local fiber = require 'fiber'


function queue.take(timeout)
   if not timeout then timeout = 0 end
   local now = fiber.time()
   local found
   while not found do
      found = box.space.queue.index.status
         :pairs({STATUS.READY},{ iterator = box.index.EQ }):nth(1)
      if not found then
         local left = (now + timeout) - fiber.time()
         if left <= 0 then return end

      end
   end
   return box.space.queue:update({found.id},
      {{'=', F.status, STATUS.TAKEN }})
end
```

---

# Channel

```lua
local fiber = require 'fiber'
*local wait = fiber.channel(0)

function queue.take(timeout)
   if not timeout then timeout = 0 end
   local now = fiber.time()
   local found
   while not found do
      found = box.space.queue.index.status
         :pairs({STATUS.READY},{ iterator = box.index.EQ }):nth(1)
      if not found then
         local left = (now + timeout) - fiber.time()
         if left <= 0 then return end
*        wait:get(left)
      end
   end
   return box.space.queue:update({found.id},
      {{'=', F.status, STATUS.TAKEN }})
end
```

---

# Channel

```lua
tarantool> do
   fiber.create(function()
      fiber.sleep(0.1)
      queue.put("task 1")
   end)

   local start = fiber.time()
   return queue.take(3), {wait = fiber.time()-start}
end
```
--
```yaml
---
- [1529264646170985, 'T', 'task 1']
- wait: `3.0040209293365`
...

```

---

# Channel

```lua
function queue.put(...)
   local id = gen_id()



   return box.space.queue:insert{ id, STATUS.READY, ... }
end

```

---

# Channel

```lua
function queue.put(...)
   local id = gen_id()

*  wait:put(true)

   return box.space.queue:insert{ id, STATUS.READY, ... }
end

```

---

# Channel

```lua
function queue.put(...)
   local id = gen_id()

   wait:put(true, `0`)

   return box.space.queue:insert{ id, STATUS.READY, ... }
end

```

---

# Channel

```lua
function queue.put(...)
   local id = gen_id()

   if wait:`has_readers`() then
      wait:put(true,0)
   end

   return box.space.queue:insert{ id, STATUS.READY, ... }
end

```

---

# Channel

```lua
tarantool> do
   fiber.create(function() fiber.sleep(0.1) queue.put("task 1") end)
   local start = fiber.time()
   return queue.take(3), {wait = fiber.time()-start}
end
```

```yaml
---
- [1529264977777259, 'T', 'task 1']
- wait: 0.10470604896545
...
```
--
```lua
tarantool> do local start = fiber.time() return
   queue.take(3), {wait = fiber.time()-start} end
```
```yaml
---
- null
- wait: 3.0040738582611
...
```

---
class:center middle

# Networking

---

# server.lua

```lua
box.cfg{
   listen = 'localhost:3311'
}
```

--

```lua
box.schema.user.grant('guest', 'super')
```

---

# server.lua

```lua
box.cfg{
   listen = 'localhost:3311'
}
```

```lua
box.once('access:v1', function()
   box.schema.user.grant('guest', 'super')
end)
```

???

```
box.schema.user.grant('guest', 'read,write,execute', 'universe')
```

---

# producer.lua

```lua
local netbox = require 'net.box'
local conn = netbox.connect('localhost:3311')
local yaml = require 'yaml'

local res = conn:call('queue.put',{unpack(arg)})
print(yaml.encode(res))

conn:close()
```

--

```yaml
$ tarantool producer.lua "test task"
--- [1529277044524945, 'R', 'test task']
...
```

---

# consumer.lua

```lua
local netbox = require 'net.box'
local conn = netbox.connect('localhost:3311')
local yaml = require 'yaml'

while true do
   local task = conn:call('queue.take',{1})
   print(yaml.encode(task))
   if task then
      conn:call('queue.release',{task.id})
   end
end
```

--

```yaml
tarantool consumer.lua
--- [1529277044524945, 'T', 'test task']
...
```

--

```yaml
*Invalid key part count in an exact match (expected 1, got 0)
```

---

# "Frozen" tasks

```yaml
$ tarantool consumer.lua
--- null
...

--- null
...
```

--

```lua
tarantool> box.space.queue:select{}
---
- - [1529277323023563, '`T`', 'test task']
...
```

---

# Triggers

```lua
local log = require 'log'
box.session.on_connect(function()
   log.info("connected %s:%s from %s", box.session.id(),
      box.session.user(), box.session.peer())
end)

box.session.on_disconnect(function()
   log.info("disconnected %s:%s from %s", box.session.id(),
      box.session.user(), box.session.peer())
end)
```

--

```terminal
main/103/main I> connected 2:guest from 127.0.0.1:62223
main/103/main I> disconnected 2:guest from nil
```

--

> WTF `nil` instead of peer?

---

# Hack peer info

```lua
local log = require 'log'
box.session.on_connect(function()
   log.info("connected %s:%s from %s", box.session.id(),
      box.session.user(), box.session.peer())
   `box.session.storage.peer` = box.session.peer()
end)

box.session.on_disconnect(function()
   log.info("disconnected %s:%s from %s", box.session.id(),
      box.session.user(), `box.session.storage.peer`)
end)
```

```terminal
main/103/main I> connected 2:guest from 127.0.0.1:49665
main/103/main I> disconnected 2:guest from 127.0.0.1:49665
```

---

# Task ownership

```lua
queue.taken = {};
queue.bysid = {};

function queue.take(timeout)
   -- ...
   local sid = box.session.id()
   log.info("Register %s by %s",found.id, sid)

   queue.bysid[ sid ] = queue.bysid[ sid ] or {}
   queue.taken[ found.id ] = `sid`
   queue.bysid[ `sid` ][ found.id ] = true

   return box.space.queue:update({found.id},{{'=', F.status, STATUS.TAKEN }})
end

```

---

# Task ownership

```lua
function queue.get_task(key)
   if not key then error("Task id required",2) end
   local t = box.space.queue:get{key}
   if not t then
      error(string.format( "Task {%s} was not found", key ),2)
   end
   if not queue.taken[key] then
      error(string.format( "Task %s not taken by any", key ),2)
   end
   if `queue.taken[key] ~= box.session.id()` then
      error(string.format( "Task %s taken by %d. Not you (%d)",
         key, queue.taken[key], box.session.id() ),2)
   end
   return t
end
```

---

# Task ownership

```lua
function queue.ack(id)
   local t = queue.get_task(id)
   return box.space.queue:delete{t.id}
end

function queue.release(id)
   local t = queue.get_task(id)
   if queue.wait:has_readers() then queue.wait:put(true,0) end
   return box.space.queue:update({t.id},{{'=', F.status, STATUS.READY }})
end
```

---

# Consumer

```terminal
tarantool consumer.lua
--- [1529278401159819, 'T', 'test task']
...

server.lua:122: Task id required
```

--

```patch
--- consumer.lua
   if task then
-       conn:call('queue.release',{ task.id })
+       conn:call('queue.release',{ task[1] })
   end
```

--

```yaml
--- [1529278570209869, 'T', 'test task']
...

server.lua:123: Task 1529278570209869ULL not taken by any
```

---

# WTF, Lua?

.absolute.width44[
```yaml
tarantool> t = {}
tarantool> t[1] = 1
tarantool> t[1LL] = 2
tarantool> t[1ULL] = 3
tarantool> t['1'] = 4
tarantool> t
```
]

--

.absolute.width44.top46[
```yaml
---
- 1: 1
  1: 3
  1: 2
  '1': 4
...
```
]

--

.absolute.width44.left50[
```yaml
tarantool> return t[1],
                  t['1'],
                  t[1LL],
                  t[1ULL]
```
]

--

.absolute.width44.left50.top46[
```yaml
---
- 1
- 4
- null
- null
...

```
]

---

# Pack key

```lua
local msgpack = require 'msgpack'

local function keypack( key )
   return msgpack.encode(key)
end

local function keyunpack( data )
   return msgpack.decode(data)
end
```

---

# Pack key

```lua
function queue.take(timeout)
   -- ...
   local sid = box.session.id()
   local `packid` = keypack(found.id)
   log.info("Register %s by %s",found.id, sid)
   queue.bysid[ sid ] = queue.bysid[ sid ] or {}
   queue.taken[ `packid` ] = sid
   queue.bysid[ sid ][ `packid` ] = true
```

---

# Pack key

```lua
function queue.get_task(id)
   if not id then error("Task id required",2) end
   local key = `tonumber64`(id)
   if not key then error(string.format("Task id '%s' is wrong",id),2) end
   local t = box.space.queue:get{key}
   if not t then
      error(string.format( "Task {%s} was not found", key ),2)
   end
   local packid = `keypack`(t.id)
   if not queue.taken[packid] then
      error(string.format( "Task %s not taken by any", key ),2)
   end
   if queue.taken[packid] ~= box.session.id() then
      error(string.format( "Task %s taken by %d. Not you (%d)",
         key, queue.taken[packid], box.session.id() ),2)
   end
   return t, `packid`
end
```

---

# Pack key

```lua
function queue.release(id)
   local t, `packid` = queue.get_task(id)
   queue.taken[ `packid` ] = nil
   queue.bysid[ box.session.id() ][ `packid` ] = nil
   ...
end

function queue.ack(id)
   local t, `packid` = queue.get_task(id)
   queue.taken[ `packid` ] = nil
   queue.bysid[ box.session.id() ][ `packid` ] = nil
   ...
end
```

---

# Autorelease by disconnect

```lua
box.session.on_disconnect(function()
   log.info("disconnected %s:%s from %s", box.session.id(),
      box.session.user(), box.session.storage.peer)

   local sid = box.session.id()
   local bysid = queue.bysid[ sid ]
   if bysid then
      for packid in pairs(bysid) do
         local key = keyunpack(packid)
         log.info("Autorelease %s by disconnect", key);
         queue.release( key )
      end
      queue.bysid[ sid ] = nil
   end
end)
```

---

# Autorelease at start

```lua
while true do
   local t = box.space.queue.index.status:pairs({STATUS.TAKEN}):nth(1)
   if not t then break end
   box.space.queue:update({t.id},{{'=', F.status, STATUS.READY }})
   log.info("Autireleased %s at start", t.id)
end
```

---
class: center middle

# Improve

---

# Delayed processing

```lua
box.schema.create_space('queue',{
   format = {
      { name = 'id';     type = 'number' },
      { name = 'status'; type = 'string' },
*     { name = 'runat';  type = 'number' },
      { name = 'data';   type = '*'      },
   };
   if_not_exists = true;
})
```

--

```lua
box.space.queue:create_index('runat', {
   parts = {3, 'number', 1, 'number'};
   if_not_exists = true;
})
```

--

```lua
STATUS.WAITING = 'W'
```

---

# Delayed put

```lua
function queue.put(data,opts)
   local id = gen_id()
   local runat = 0
   local status = STATUS.READY
   if opts and opts.delay then
      `runat` = clock.monotonic() + tonumber(opts.delay)
      status = STATUS.WAITING
   else
      if queue.wait:has_readers() then queue.wait:put(true,0) end
   end
   return box.space.queue:insert{ id, status, `runat`, unpack(data) }
end
```

---

# Background actions: fibers

```lua
queue.runat = fiber.create(function()
   fiber.name('queue.runat')
   while true do
      local remaining
      -- ...
      if not remaining or remaining > 1 then remaining = 1 end
      fiber.sleep(remaining)
   end
end)
```

---

# Background actions: fibers

```lua
      local remaining
      local now = clock.monotonic()
      for _,t in box.space.queue.index.runat:pairs({0},
         { iterator = box.index.GT }) do
         
         if `t.runat > now` then
            remaining = t.runat - now
            `break`
         else
            ---
         end
      end
      if not remaining or remaining > 1 then remaining = 1 end
```

---

# Background actions: fibers

```lua
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
            else
               log.error("Runat: bad status %s for %s", t.status, t.id)
               box.space.queue:update({t.id},{{'=', F.runat, 0 }})
            end
         end
```

---
# Background actions: fibers

.small[
```lua
queue.runat = fiber.create(function()
   fiber.name('queue.runat')
   while true do
      local remaining
      local now = clock.monotonic()
      for _,t in box.space.queue.index.runat:pairs({0},
         { iterator = box.index.GT }) do
         
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
            else
               log.error("Runat: bad status %s for %s", t.status, t.id)
               box.space.queue:update({t.id},{{'=', F.runat, 0 }})
            end
         end
      end
      if not remaining or remaining > 1 then remaining = 1 end
      fiber.sleep(remaining)
   end
end)
```
]

---

# Background actions: fibers

```yaml
tarantool> queue.put({1,2,3},{delay=5})
---
- [1529452967197172, 'W', 1529452972.1973, 1, 2, 3]
...
```

--

```yaml
tarantool> queue.take(0)
---
...
```

--

```yaml
tarantool> queue.take(5)
main/105/`queue.runat` I> Runat: W->R 1529452967197172ULL
main/101/background.lua I> Register 1529452967197172ULL by 1
---
- [1529452967197172, 'T', 0, 1, 2, 3]
...
```

---
class: center middle

# Statistics & monitoring

---

# queue.stats

```lua
function queue.stats()
   return {
      total   = box.space.queue:len(),
      ready   = box.space.queue.index.status:count({STATUS.READY}),
      waiting = box.space.queue.index.status:count({STATUS.WAITING}),
      taken   = box.space.queue.index.status:count({STATUS.TAKEN}),
   }
end
```

--

```yaml
tarantool> queue.stats()
---
- ready: 10
  taken: 2
  waiting: 5
  total: 17
...
```

---

# Small queue

```yaml
tarantool> local s = clock.time() queue.stats() return clock.time()-s
---
- 0.00019598007202148
...
```

```yaml
tarantool> queue.stats()
---
- ready: 10
  taken: 2
  waiting: 5
  total: 17
...
```

---

# Big queue

```lua
tarantool> for k = 1,1e6 do queue.put({1}) end
```

--

```yaml
tarantool> queue.stats()
---
- ready: 1000000
  taken: 0
  waiting: 0
  total: 1000000
...
```

--

```yaml
tarantool> local s = clock.time() queue.stats() return clock.time()-s
---
- `0.30088090896606`
...
```

---

# Cached counters

```lua
queue._stats = {
   ready   = 0LL,
   taken   = 0LL,
   waiting = 0LL,
}

for _,t in box.space.queue:pairs() do
   queue._stats[ t[F.status] ]
      = (queue._stats[ t[F.status] ] or 0LL)+1
end

function queue.stats()
   return {
      total   = box.space.queue:len(),
      ready   = queue._stats[STATUS.READY] or 0,
      waiting = queue._stats[STATUS.WAITING] or 0,
      taken   = queue._stats[STATUS.TAKEN] or 0,
   }
end
```

---

# Cached counters

```yaml
tarantool> queue.stats()
---
- ready: 1000000
  taken: 0
  waiting: 0
  total: 1000000
...

tarantool> local s = clock.time() queue.stats() return clock.time()-s
---
- 0.00002002716
...
```

---

# Cached counters

```lua
-- function queue.take(timeout)
   queue._stats[ STATUS.READY ] = queue._stats[ STATUS.READY ] - 1
   queue._stats[ STATUS.TAKEN ] = queue._stats[ STATUS.TAKEN ] + 1
```

```lua
-- function queue.ack(id)
   queue._stats[ t[F.status] ] = queue._stats[ t[F.status] ] - 1
```

```lua
-- function queue.release(id)
   queue._stats[ t[F.status] ] = queue._stats[ t[F.status] ] - 1
   queue._stats[ STATUS.READY ] = queue._stats[ STATUS.READY ] + 1
```

# ...

---

# Cached counters

```yaml
tarantool> queue.stats()
---
- ready: 999998
  taken: 2
  waiting: 1
  total: 1000001
...
```
--

# ...

--

```yaml
tarantool> queue.stats()
---
- ready: 4
  taken: -2
  waiting: 7
  total: 3
...
```

---

# Data modification trigger

```lua
box.space.queue:on_replace(function(old,new)
   if old then
      queue._stats[ old[ F.status ] ] =
         queue._stats[ old[ F.status ] ] - 1
   end
   if new then
      queue._stats[ new[ F.status ] ] =
         queue._stats[ new[ F.status ] ] + 1
   end
end)
```

---

# Data modification trigger

```lua
box.space.queue:on_replace(function(old,new)
   if old then
      queue._stats[ old[ F.status ] ] =
         (queue._stats[ old[ F.status ] ] or 0LL) - 1
   end
   if new then
      queue._stats[ new[ F.status ] ] =
         (queue._stats[ new[ F.status ] ] or 0LL) + 1
   end
end)
```

---
class: center middle

# Network monitoring

---

# Sockets

---

# Sockets inside database!

---

# Sockets inside database + appserver!

```lua
local socket = require 'socket'
```
--
```lua
local graphite_host = 'localhost'
local graphite_port = 2003
```
--
```lua
local ai = socket.getaddrinfo(graphite_host, graphite_port, 1,
      { type = 'SOCK_DGRAM' })
local addr,port
for _,info in pairs(ai) do
   addr,port = info.host,info.port
   break
end
if not addr then error("Failed to resolve host") end
```

--

```lua
local remote = socket('AF_INET', 'SOCK_DGRAM', 'udp')
remote:sendto(addr, port, msg)
```

---

# Monitoring fiber with socket

```lua
queue.monitor = fiber.create(function()
   fiber.name('queue.monitor')
   local remote = socket('AF_INET', 'SOCK_DGRAM', 'udp')

   while true do
      for k,v in pairs(queue.stats()) do
         local msg = string.format("queue.stats.%s %s %s\n",k,
            tonumber(v),math.floor(fiber.time()))
         remote:sendto(addr, port, msg)
      end
      fiber.sleep(1)
   end
end)
```

---

# Also TCP
```lua
while true do
   local remote =  socket.tcp_connect(graphite_host, graphite_port)
   if not remote then
      log.error("Failed to connect %s",errno.strerror()) fiber.sleep(1)
   else
      while true do
         local data = {}
         for k,v in pairs(queue.stats()) do
            table.insert(data,string.format("queue.stats.%s %s %s\n",k,
               tonumber(v),math.floor(fiber.time())))
         end
         data = table.concat(data,'')
         if not remote:send(data) then
            log.error("%s",errno.strerror())
            break
         end
         fiber.sleep(1)
      end
   end
end
```

---

class: center middle

# Get your data in *RAM*
# Reload code *without restart*

---

# Code load

--

```lua
require 'my.module'
```

--

```lua
if not package.loaded['my.module'] then
   package.loaded['my.module'] = dofile('my/module.lua')
end
```

--

# Reload

1. Remember `package.loaded`
2. Check first run
3. Clean loaded (`package.loaded[X]` = nil)
4. Proceed

---

# Reload

```lua
local fiber = require'fiber'
local clock = require'clock'
local log   = require'log'

local not_first_run = rawget(_G,'_NOT_FIRST_RUN')
_NOT_FIRST_RUN = true
if not_first_run then
   for k,v in pairs(package.loaded) do
      if not preloaded[k] then
         package.loaded[k] = nil
      end
   end
else
   preloaded = {}
   for k,v in pairs(package.loaded) do
      preloaded[k] = true
   end
end

-- ...
```

---

# Reload

```lua
local fiber = require'fiber'
local clock = require'clock'
local log   = require'log'

*require 'package.reload'
```

--

## reload by

```yaml
tarantool> dofile("/path/to/init.lua")
```

--

## or just

```yaml
tarantool> package.reload()
```


---

# Reloadable code

```diff
-queue = {}
+if not queue then
+   queue = {}
+   ...
+   -- Autorelease only at start
+   ...
+   -- Initial stats
+   ...
+else
+   log.info("Queue reload %s", package.reload.count)
+end
```

???

```lua
if not queue then
   log.info("First start")
   queue = {}

   -- Autorelease only at start
   local c = 0
   for _,t in box.space.queue.index.status:pairs({STATUS.TAKEN}) do
      box.space.queue:update({t.id},{{'=', F.status, STATUS.READY }})
      c = c + 1
   end
   log.info("Autorelease %d tasks at start",c)

   -- Initial stats
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

else
   log.info("Queue reload %s", package.reload.count)
end
```

---

# Reloadable trigger

```lua
 queue.on_replace_trigger = box.space.queue:on_replace(function(old,new)
   if old then
      queue._stats[ old[ F.status ] ] = queue._stats[ old[ F.status ] ] - 1
   end
   if new then
      queue._stats[ new[ F.status ] ] = queue._stats[ new[ F.status ] ] + 1
   end
 end, queue.on_replace_trigger)
```

---

# Reloadable trigger

```lua
 `queue.on_replace_trigger` = box.space.queue:on_replace(function(old,new)
   if old then
      queue._stats[ old[ F.status ] ] = queue._stats[ old[ F.status ] ] - 1
   end
   if new then
      queue._stats[ new[ F.status ] ] = queue._stats[ new[ F.status ] ] + 1
   end
 end, `queue.on_replace_trigger`)
```

--

```lua
queue.on_disconnect_trigger = box.session.on_disconnect(function()
   ...
end,queue.on_disconnect_trigger)
```

---

# Reloadable fiber
```lua

queue.runat = fiber.create(function()
   fiber.name('queue.runat')
   while true do
      local now = clock.monotonic()
      local remaining
      -- ...
      if not remaining or remaining > 1 then remaining = 1 end
      fiber.sleep(remaining)
   end
end)
```

---

# Reloadable fiber
```lua
*queue.runch = fiber.channel()
queue.runat = fiber.create(function()
   fiber.name('queue.runat')
   while true do
      local now = clock.monotonic()
      local remaining
      -- ...
      if not remaining or remaining > 1 then remaining = 1 end
*     queue.runch:get(remaining)
   end
end)
```

---

# Reloadable fiber

```lua
queue.runch = `queue.runch or` fiber.channel()
queue.runat = fiber.create(function()
*  local gen = package.reload.count
   fiber.name('queue.runat.'..`gen`)
   while true do
      local now = clock.monotonic()
      local remaining
      -- ...
      if not remaining or remaining > 1 then remaining = 1 end
      queue.runch:get(remaining)
   end
   log.info("I'm done")
end)
```

---

# Reloadable fiber

```lua
queue.runch = queue.runch or fiber.channel()
queue.runat = fiber.create(function()
   local gen = package.reload.count
   fiber.name('queue.runat.'..gen)
   while `gen == package.reload.count` do
      local now = clock.monotonic()
      local remaining
      -- ...
      if not remaining or remaining > 1 then remaining = 1 end
      queue.runch:get(remaining)
   end
   log.info("I'm done")
end)
```

--

```lua
while queue.runch:has_readers() do
   queue.runch:put(0,0)
end
```
---

# Do not start console

```lua
if not fiber.self().storage.console then
   require'console'.start()
   os.exit()
end
```

--

```lua
if package.reload.count == 1 then
   require'console'.start()
   os.exit()
end
```

---
class:bigger

# Summary

- Spaces & indices
- Console & strict
- Clock, id generation
- Channels
- Networking (listen + net.box)
- Connection triggers + peer hack
- Lua table keys: number vs cdata LL
- MsgPack
- Background fibers & by-index processing
- Accounting & monitoring
- Data modification trigger
- Code reload

---
class: extra-extra-small

.absolute.width29[
```lua
require'strict'.on()
fiber = require'fiber'
clock = require'clock'
log   = require'log'

require 'package.reload'

local msgpack = require 'msgpack'
local socket = require 'socket'


box.cfg{
   listen = 'localhost:3311'
}

box.once('access:v1', function()
   box.schema.user.grant('guest', 'super')
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

local STATUS = {}
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

if not rawget(_G,'queue') then
   log.info("First start")
   queue = {}
   -- Autorelease only at start
   local c = 0
   for _,t in box.space.queue.index.status:pairs({STATUS.TAKEN}) do
      box.space.queue:update({t.id},{{'=', F.status, STATUS.READY }})
      c = c + 1
   end
   log.info("Autorelease %d tasks at start",c)

   -- Initial stats
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

   queue.taken = {};
   queue.bysid = {};

else
   log.info("Queue reload %s", package.reload.count)

end

queue.on_replace_trigger = box.space.queue:on_replace(function(old,new)
   if old then
      queue._stats[ old[ F.status ] ] = queue._stats[ old[ F.status ] ] - 1
   end
   if new then
      queue._stats[ new[ F.status ] ] = queue._stats[ new[ F.status ] ] + 1
   end
end, queue.on_replace_trigger)

local function keypack( key )
   return msgpack.encode(key)
end
local function keyunpack( data )
   return msgpack.decode(data)
end
```
]

.absolute.width29.left35[
```lua
box.session.on_connect(function()
   log.info("connected %s:%s from %s", box.session.id(), box.session.user(), box.session.peer())
   box.session.storage.peer = box.session.peer()
end)


queue.on_disconnect_trigger = box.session.on_disconnect(function()
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
end,queue.on_disconnect_trigger)

local clock = require 'clock'
local function gen_id()
   local new_id = clock.monotonic64()/1e3
   while box.space.queue:get(new_id) do
      new_id = new_id + 1
   end
   return new_id
end

queue.wait = queue.wait or fiber.channel(0)

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
   return box.space.queue:insert{ id, status, runat, unpack(data) }
end

queue.runch = queue.runch or fiber.channel()
queue.runat = fiber.create(function()
   local gen = package.reload.count
   fiber.name('queue.runat.'..gen)
   while gen == package.reload.count do
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
            else
               log.error("Runat: bad status %s for %s",t.status, tostring(t))
               box.space.queue:update({t.id},{{'=', F.runat, 0 }})
            end
         end
      end
      if not remaining or remaining > 1 then remaining = 1 end
      queue.runch:get(remaining)
   end
   log.info("I'm done")
end)
while queue.runch:has_readers() do
   queue.runch:put(0,0)
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
```
]
.absolute.width29.left66[
```lua

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

function queue.stats()
   return {
      total   = box.space.queue:len(),
      ready   = queue._stats[STATUS.READY] or 0,
      waiting = queue._stats[STATUS.WAITING] or 0,
      taken   = queue._stats[STATUS.TAKEN] or 0,
   }
end

local graphite_host = 'localhost'
local graphite_port = 2003
queue.monitor = fiber.create(function()
   local gen = package.reload.count
   fiber.name('queue.mon.'..gen)
   while gen == package.reload.count do
      local remote =  require 'socket'.tcp_connect(graphite_host, graphite_port)
      if not remote then
         log.error("Failed to connect %s",errno.strerror())
         fiber.sleep(1)
      else
         while gen == package.reload.count do
            local data = {}
            for k,v in pairs(queue.stats()) do
               table.insert(data,string.format("queue.stats.%s %s %s\n",k,tonumber(v),math.floor(fiber.time())))
            end
            data = table.concat(data,'')
            if not remote:send(data) then
               log.error("%s",errno.strerror())
               break
            end
            fiber.sleep(1)
         end
      end
   end
   log.info("I'm done")
end)

if package.reload.count == 1 then
   require'console'.start()
   os.exit()
end
```
]
--
.absolute.width29.left66.bottom6[

## 20K RPS queue

## One day of coding

## 300 Lines of code

]

---
class:first

.absolute.width80.left8.top8[
# Questions?
]

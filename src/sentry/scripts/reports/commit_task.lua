-- KEYS: {
--   TASK_SET,  -- name of the set that contains all task IDs
--   DATA...    -- keys to remove if task list is empty
-- }
-- ARGV: {
--   TASK_ID    -- task to mark as completed
-- }
local TASK_SET = KEYS[1]
local TASK_ID = ARGV[1]

-- Remove the task from the task set.
redis.call('SREM', TASK_SET, TASK_ID)

-- If there are tasks left to run, we don't need to do anything.
if tonumber(redis.call('SCARD', TASK_SET)) > 0 then
    return redis.status_reply("OK")
end

-- Otherwise, delete all of the data keys.
local results = {}
for i = 2, #KEYS do
    table.insert(results, redis.call('DEL', KEYS[i]))
end
return results

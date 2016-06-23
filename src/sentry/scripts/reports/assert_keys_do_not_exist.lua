local existing = {}
for i = 1, #KEYS do
    local key = KEYS[i]
    if redis.call('EXISTS', key) > 0 then
        table.insert(existing, key)
    end
end

if #existing > 0 then
    return redis.error_reply(
        string.format(
            "keys exist: %s",
            table.concat(existing, ", ")
        ))
else
    return redis.status_reply("OK")
end

local clock = require 'clock'
local fiber = require 'fiber'
local log = require 'log'
local json = require 'json'.new()
local indexpiration = require 'indexpiration'

--================================================================================
-- Centrifuge Tarantool module, provides Broker and PresenceManager functionality.
--================================================================================

local centrifuge = {}

function on_disconnect()
    local id = box.session.storage.subscriber_id
    if id then
        local channelsById = centrifuge.id_to_channels[id]
        if channelsById then
            while next(channelsById) do
                for key, _ in pairs(channelsById) do
                    centrifuge.channel_to_ids[key][id] = nil
                    if next(centrifuge.channel_to_ids[key]) == nil then
                        centrifuge.channel_to_ids[key] = nil
                    end
                    channelsById[key] = nil
                end
            end
            centrifuge.id_to_channels[id] = nil
        end
        centrifuge.id_to_fiber[id]:close()
        centrifuge.id_to_fiber[id] = nil
        centrifuge.id_to_messages[id] = nil
    end
end

centrifuge.init = function(opts)
    if not opts then opts = {} end

    --log.info("Centrifuge init with opts: %s", json.encode(opts))

    local pubs_temporary = false --opts.pubs_temporary or false
    local meta_temporary = false --opts.meta_temporary or false
    local presence_temporary = false --opts.presence_temporary or false

    box.schema.create_space('pubs', {if_not_exists = true; temporary = pubs_temporary})
    box.space.pubs:format( {
        {name = 'id';      type = 'unsigned'},
        {name = 'channel'; type = 'string'},
        {name = 'offset';  type = 'unsigned'},
        {name = 'exp';     type = 'number'},
        {name = 'data';    type = 'string'},
        {name = 'info';    type = 'string'},
    });
    box.space.pubs:create_index('primary', {
        parts = {{field='id', type='unsigned'}};
        if_not_exists = true;
    })
    box.space.pubs:create_index('channel', {
        parts = {{field='channel', type='string'}, {field='offset', type='unsigned'}};
        if_not_exists = true;
    })
    box.space.pubs:create_index('exp', {
        parts = {{field='exp', type='number'}, {field='id', type='unsigned'}};
        if_not_exists = true;
    })

    box.schema.create_space('meta', {if_not_exists = true; temporary = meta_temporary})
    box.space.meta:format({
        {name = 'channel'; type = 'string'},
        {name = 'offset';  type = 'unsigned'},
        {name = 'epoch';   type = 'string'},
        {name = 'exp';     type = 'number'},
    });
    box.space.meta:create_index('primary', {
        parts = {{field='channel', type='string'}};
        if_not_exists = true;
    })
    box.space.meta:create_index('exp', {
        parts = {{field='exp', type='number'}, {field='channel', type='string'}};
        if_not_exists = true;
    })

    box.schema.create_space('presence', {if_not_exists = true; temporary = presence_temporary})
    box.space.presence:format({
        {name = 'channel';   type = 'string'},
        {name = 'client_id'; type = 'string'},
        {name = 'user_id';   type = 'string'},
        {name = 'conn_info'; type = 'string'},
        {name = 'chan_info'; type = 'string'},
        {name = 'exp';       type = 'number'},
    });
    box.space.presence:create_index('primary', {
        parts = {{field='channel', type='string'}, {field='client_id', type='string'}};
        if_not_exists = true;
    })
    box.space.presence:create_index('exp', {
        parts = {{field='exp', type='number'}};
        if_not_exists = true;
    })

    indexpiration(box.space.pubs, {
        field = 'exp';
        kind = 'time';
        precise = true;
        on_delete = function(t) end
    })

    indexpiration(box.space.meta, {
        field = 'exp';
        kind = 'time';
        precise = true;
        on_delete = function(t) end
    })

    indexpiration(box.space.presence, {
        field = 'exp';
        kind = 'time';
        precise = true;
        on_delete = function(t) end
    })

    --box.session.on_connect()
    box.session.on_disconnect(on_disconnect)

    rawset(_G, 'centrifuge', centrifuge)
end

centrifuge.id_to_channels = {}
centrifuge.channel_to_ids = {}
centrifuge.id_to_messages = {}
centrifuge.id_to_fiber = {}

function centrifuge.get_messages(id, use_polling, timeout)
    if not box.session.storage.subscriber_id then
        -- register poller connection. Connection will use this id
        -- to register or remove subscriptions.
        box.session.storage.subscriber_id = id
        centrifuge.id_to_fiber[id] = fiber.channel()
        return
    end
    box.session.storage.subscriber_id = id
    if not timeout then timeout = 0 end
    local now = fiber.time()
    while true do
        local messages = centrifuge.id_to_messages[id]
        centrifuge.id_to_messages[id] = nil
        if messages then
            if use_polling then
                return messages
            else
                local ok = box.session.push(messages)
                if ok ~= true then
                    error("write error")
                end
            end
        else
            local left = (now + timeout) - fiber.time()
            if left <= 0 then
                -- timed out, poller will call get_messages again.
                return
            end
            centrifuge.id_to_fiber[id]:get(left)
        end
    end
end

function centrifuge.subscribe(id, channels)
    for k,v in pairs(channels) do
        local idChannels = centrifuge.id_to_channels[id] or {}
        idChannels[v] = true
        centrifuge.id_to_channels[id] = idChannels

        local channelIds = centrifuge.channel_to_ids[v] or {}
        channelIds[id] = true
        centrifuge.channel_to_ids[v] = channelIds
    end
end

function centrifuge.unsubscribe(id, channels)
    for k,v in pairs(channels) do
        if centrifuge.id_to_channels[id] then
            centrifuge.id_to_channels[id][v] = nil
        end
        if centrifuge.channel_to_ids[v] then
            centrifuge.channel_to_ids[v][id] = nil
        end
        if centrifuge.id_to_channels[id] then
            centrifuge.id_to_channels[id] = nil
        end
        if centrifuge.channel_to_ids[v] and next(centrifuge.channel_to_ids[v]) == nil then
            centrifuge.channel_to_ids[v] = nil
        end
    end
end

local function publish_to_subscribers(channel, message_tuple)
    local channelIds = centrifuge.channel_to_ids[channel] or {}
    if channelIds then
        for k,v in pairs(channelIds) do
            local id_to_messages = centrifuge.id_to_messages[k] or {}
            table.insert(id_to_messages, message_tuple)
            centrifuge.id_to_messages[k] = id_to_messages
        end
    end
end

local function wake_up_subscribers(channel)
    local ids = centrifuge.channel_to_ids[channel]
    if ids then
        for k, _ in pairs(ids) do
            local channel = centrifuge.id_to_fiber[k]
            if channel:has_readers() then channel:put(true, 0) end
        end
    end
end

function centrifuge.publish(msg_type, channel, data, info, ttl, size, meta_ttl)
    if not ttl then ttl = 0 end
    if not size then size = 0 end
    if not meta_ttl then meta_ttl = 0 end
    local epoch = ""
    local offset = 0
    box.begin()
    if ttl > 0 and size > 0 then
        local now = clock.realtime()
        local meta_exp = 0
        if meta_ttl > 0 then
            meta_exp = now + meta_ttl
        end
        local stream_meta = box.space.meta:get(channel)
        if stream_meta then
            offset = stream_meta[2] + 1
            epoch = stream_meta[3]
        else
            epoch = tostring(now)
            offset = 1
        end
        box.space.meta:upsert({channel, offset, epoch, meta_exp}, {{'=', 'channel', channel}, {'+', 'offset', 1}, {'=', 'exp', meta_exp}})
        box.space.pubs:auto_increment{channel, offset, clock.realtime() + tonumber(ttl), data, info}
        local max_offset_to_keep = offset - size
        if max_offset_to_keep > 0 then
            for _, v in box.space.pubs.index.channel:pairs({channel, max_offset_to_keep}, {iterator = box.index.LE}) do
                box.space.pubs:delete{v.id}
            end
        end
    end
    publish_to_subscribers(channel, {msg_type, channel, offset, epoch, data, info})
    wake_up_subscribers(channel)
    box.commit()
    return offset, epoch
end

function centrifuge.history(channel, since_offset, limit, include_pubs, meta_ttl)
    if not meta_ttl then meta_ttl = 0 end
    local meta_exp = 0
    local now = clock.realtime()
    if meta_ttl > 0 then
        meta_exp = now + meta_ttl
    end
    local epoch = tostring(now)
    box.begin()
    box.space.meta:upsert({channel, 0, epoch, meta_exp}, {{'=', 'channel', channel}, {'=', 'exp', meta_exp}})
    local stream_meta = box.space.meta:get(channel)
    if not include_pubs then
        box.commit()
        return stream_meta[2], stream_meta[3], nil
    end
    if stream_meta[2] == since_offset - 1 then
        box.commit()
        return stream_meta[2], stream_meta[3], nil
    end
    local num_entries = 0
    local pubs = box.space.pubs.index.channel:pairs({channel, since_offset}, {iterator = box.index.GE}):take_while(function(x)
        num_entries = num_entries + 1
        return x.channel == channel and (limit < 1 or num_entries < limit + 1)
    end):totable()
    box.commit()
    return stream_meta[2], stream_meta[3], pubs
end

function centrifuge.remove_history(channel)
    box.begin()
    for _, v in box.space.pubs.index.channel:pairs{channel} do
        box.space.pubs:delete{v.id}
    end
    box.commit()
end

function centrifuge.add_presence(channel, ttl, client_id, user_id, conn_info, chan_info)
    if not ttl then ttl = 0 end
    if not conn_info then conn_info = "" end
    if not chan_info then chan_info = "" end
    local exp = clock.realtime() + ttl
    box.space.presence:put({channel, client_id, user_id, conn_info, chan_info, exp})
end

function centrifuge.remove_presence(channel, client_id)
    for _, v in box.space.presence:pairs({channel, client_id}, {iterator = box.index.EQ}) do
        box.space.presence:delete{channel, client_id}
    end
end

function centrifuge.presence(channel)
    return box.space.presence:select{channel}
end

function centrifuge.presence_stats(channel)
    local users = {}
    local num_clients = 0
    local num_users = 0
    for _, v in box.space.presence:pairs({channel}, {iterator = box.index.EQ}) do
        num_clients = num_clients + 1
        if not users[v.user_id] then
            num_users = num_users + 1
            users[v.user_id] = true
        end
    end
    return num_clients, num_users
end

function centrifuge.validate_config(conf, old)
    return true
end

function centrifuge.apply_config(conf, opts)
    return true
end

function centrifuge.stop()
    box.session.on_disconnect(on_disconnect, nil)
end

centrifuge.role_name="centrifuge"

return centrifuge

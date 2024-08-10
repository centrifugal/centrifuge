require 'strict'.on()
fiber = require 'fiber'

local instance_id = string.match(arg[1], '^%d+$')
assert(instance_id, 'malformed instance id')

local port = 3300 + instance_id
local workdir = 'ha_tnt'..instance_id

box.cfg{
    listen = '0.0.0.0:'..port,
    wal_dir = workdir,
    memtx_dir = workdir,
    readahead = 10 * 1024 * 1024, -- to keep up with benchmark load.
    net_msg_max = 1024, -- to keep up with benchmark load.

    instance_uuid='aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa'..instance_id,

    replication = {
        'tnt1:'..3301,
        'tnt2:'..3302,
        'tnt3:'..3303,
    },
    replication_connect_quorum=0,

    -- The instance is set to candidate, so it may become leader itself
    -- as well as vote for other instances.
    --
    -- Alternative: set one of the three instances to `voter`, so that it
    -- never becomes a leader but still votes for one of its peers and helps
    -- it reach election quorum (2 in our case).
    election_mode='candidate',
    -- Quorum for both synchronous transactions and
    -- leader election votes.
    replication_synchro_quorum=2,
    -- Synchronous replication timeout. The transaction will be
    -- rolled back if no quorum is achieved during 1 second.
    replication_synchro_timeout=1,
    -- Heartbeat timeout. A leader is considered dead if it doesn't
    -- send heartbeats for 4 * replication_timeout (1 second in our case).
    -- Once the leader is dead, remaining instances start a new election round.
    replication_timeout=0.25,
    -- Timeout between elections. Needed to restart elections when no leader
    -- emerges soon enough.
    election_timeout=0.25,
}

box.schema.user.grant('guest', 'super', nil, nil, { if_not_exists = true })

centrifuge = require 'centrifuge'

box.once("centrifuge:schema:1", function()
    centrifuge.init({
        presence_temporary=true
    })
end)

require'console'.start()

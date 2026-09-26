/**
 * Atomic broker operations as Redis Lua scripts.
 *
 * Competing consumers are safe because each operation (publish, consume,
 * ack, requeue, delete, TTL change, cancel, sweep) runs atomically inside
 * a single script. The HTTP layer passes keys explicitly and parses the
 * small array replies — no Redis details leak past the broker service.
 */

/** SADD queue + initialise counters. Returns 1 when newly created. */
export const DECLARE_SCRIPT = `
if redis.call('SISMEMBER', KEYS[1], ARGV[1]) == 1 then
  return 0
end
redis.call('SADD', KEYS[1], ARGV[1])
redis.call('HSET', KEYS[2], 'createdAt', ARGV[2], 'published', 0, 'delivered', 0, 'acked', 0, 'requeued', 0, 'deleted', 0)
return 1
`;

/**
 * Publish one message, or upsert it when the id already exists.
 * KEYS: registry, meta, ready, delayed, msg
 * ARGV: queue, id, dataJson, availableAt, now, upsert ('1' = update in place)
 * Returns {'OK', state, upserted, createdAt} | {'CONFLICT'} | {'LEASED'}.
 *
 * Upsert replaces data and TTL as if freshly published (moved to the tail
 * of ready, or re-scored when delayed; deliveries reset; createdAt kept).
 * Leased (unacked) messages are never overwritten: {'LEASED'}.
 */
export const PUBLISH_SCRIPT = `
if redis.call('SISMEMBER', KEYS[1], ARGV[1]) == 0 then
  redis.call('SADD', KEYS[1], ARGV[1])
end
if redis.call('EXISTS', KEYS[2]) == 0 then
  redis.call('HSET', KEYS[2], 'createdAt', ARGV[5], 'published', 0, 'delivered', 0, 'acked', 0, 'requeued', 0, 'deleted', 0)
end
local exists = redis.call('EXISTS', KEYS[5])
local upserted = 0
if exists == 1 then
  if ARGV[6] ~= '1' then
    return {'CONFLICT'}
  end
  local current = redis.call('HGET', KEYS[5], 'state')
  if current == 'unacked' then
    return {'LEASED'}
  end
  upserted = 1
  if current == 'ready' then
    redis.call('LREM', KEYS[3], 0, ARGV[2])
  elseif current == 'delayed' then
    redis.call('ZREM', KEYS[4], ARGV[2])
  end
end
local state
if tonumber(ARGV[4]) <= tonumber(ARGV[5]) then
  state = 'ready'
  redis.call('RPUSH', KEYS[3], ARGV[2])
else
  state = 'delayed'
  redis.call('ZADD', KEYS[4], ARGV[4], ARGV[2])
end
redis.call('HSET', KEYS[5],
  'id', ARGV[2], 'queue', ARGV[1], 'data', ARGV[3], 'state', state,
  'consumer', '', 'deliveries', 0, 'availableAt', ARGV[4], 'visibleAt', 0,
  'updatedAt', ARGV[5])
local createdAt = ARGV[5]
if exists == 1 then
  createdAt = redis.call('HGET', KEYS[5], 'createdAt')
  if not createdAt then
    createdAt = ARGV[5]
  end
else
  redis.call('HSET', KEYS[5], 'createdAt', ARGV[5])
end
redis.call('HINCRBY', KEYS[2], 'published', 1)
return {'OK', state, upserted, createdAt}
`;

/**
 * Consume up to `count` ready messages for one consumer.
 * Promotes due delayed messages and reclaims expired unacked leases first.
 * KEYS: registry, meta, ready, delayed, unacked, consumers, pending
 * ARGV: queue, consumer, count, prefetch(-1 = keep), visibilityMs, now,
 *       maxScan, msgPrefix, pendPrefix, defaultPrefetch
 * Returns {'NOT_FOUND'} | {'OK', delivered, outstanding, id, data, deliveries, ...}.
 */
export const CONSUME_SCRIPT = `
if redis.call('SISMEMBER', KEYS[1], ARGV[1]) == 0 then
  return {'NOT_FOUND'}
end
local now = tonumber(ARGV[6])
local scan = tonumber(ARGV[7])
local vis = tonumber(ARGV[5])
local requestedPrefetch = tonumber(ARGV[4])
local effPrefetch
if requestedPrefetch >= 0 then
  effPrefetch = requestedPrefetch
  redis.call('HSET', KEYS[6], ARGV[2], ARGV[4])
else
  local current = redis.call('HGET', KEYS[6], ARGV[2])
  if current then
    effPrefetch = tonumber(current)
  else
    effPrefetch = tonumber(ARGV[10])
    redis.call('HSET', KEYS[6], ARGV[2], ARGV[10])
  end
end
local due = redis.call('ZRANGEBYSCORE', KEYS[4], 0, now, 'LIMIT', 0, scan)
for _, id in ipairs(due) do
  redis.call('ZREM', KEYS[4], id)
  local m = ARGV[8] .. id
  if redis.call('EXISTS', m) == 1 then
    redis.call('HSET', m, 'state', 'ready', 'availableAt', now, 'updatedAt', now)
    redis.call('RPUSH', KEYS[3], id)
  end
end
local expired = redis.call('ZRANGEBYSCORE', KEYS[5], 0, now, 'LIMIT', 0, scan)
for _, id in ipairs(expired) do
  redis.call('ZREM', KEYS[5], id)
  local m = ARGV[8] .. id
  local owner = redis.call('HGET', m, 'consumer')
  if owner and owner ~= '' then
    redis.call('SREM', ARGV[9] .. owner .. ':pending', id)
  end
  if redis.call('EXISTS', m) == 1 then
    redis.call('HSET', m, 'state', 'ready', 'consumer', '', 'visibleAt', 0, 'updatedAt', now)
    redis.call('RPUSH', KEYS[3], id)
    redis.call('HINCRBY', KEYS[2], 'requeued', 1)
  end
end
local outstanding = redis.call('SCARD', KEYS[7])
local allowed = math.min(tonumber(ARGV[3]), effPrefetch - outstanding)
if allowed <= 0 then
  return {'OK', 0, outstanding}
end
local delivered = 0
local out = {'OK', 0, 0}
for i = 1, allowed do
  local id = redis.call('LPOP', KEYS[3])
  if not id then
    break
  end
  local m = ARGV[8] .. id
  if redis.call('EXISTS', m) == 1 and redis.call('HGET', m, 'state') == 'ready' then
    local deliveries = redis.call('HINCRBY', m, 'deliveries', 1)
    redis.call('HSET', m, 'state', 'unacked', 'consumer', ARGV[2],
      'visibleAt', now + vis, 'updatedAt', now)
    redis.call('ZADD', KEYS[5], now + vis, id)
    redis.call('SADD', KEYS[7], id)
    delivered = delivered + 1
    local data = redis.call('HGET', m, 'data')
    table.insert(out, id)
    table.insert(out, data)
    table.insert(out, deliveries)
  end
end
redis.call('HINCRBY', KEYS[2], 'delivered', delivered)
out[2] = delivered
out[3] = outstanding + delivered
return out
`;

/**
 * Acknowledge one leased message (removes it permanently).
 * KEYS: meta, unacked, msg
 * ARGV: id, consumerOrEmpty, now, pendPrefix
 * Returns {'OK', deliveries} | {'NOT_FOUND'} | {'CONFLICT', state} |
 *         {'WRONG_OWNER', owner}.
 */
export const ACK_SCRIPT = `
if redis.call('EXISTS', KEYS[3]) == 0 then
  return {'NOT_FOUND'}
end
local state = redis.call('HGET', KEYS[3], 'state')
if state ~= 'unacked' then
  return {'CONFLICT', state or ''}
end
local owner = redis.call('HGET', KEYS[3], 'consumer')
if ARGV[2] ~= '' and owner ~= ARGV[2] then
  return {'WRONG_OWNER', owner or ''}
end
local deliveries = redis.call('HGET', KEYS[3], 'deliveries')
redis.call('ZREM', KEYS[2], ARGV[1])
if owner and owner ~= '' then
  redis.call('SREM', ARGV[4] .. owner .. ':pending', ARGV[1])
end
redis.call('DEL', KEYS[3])
redis.call('HINCRBY', KEYS[1], 'acked', 1)
return {'OK', deliveries or '0'}
`;

/**
 * Requeue one leased message back to the ready tail.
 * KEYS: meta, ready, unacked, msg
 * ARGV: id, consumerOrEmpty, now, pendPrefix
 * Returns {'OK', deliveries} | {'NOT_FOUND'} | {'CONFLICT', state} |
 *         {'WRONG_OWNER', owner}.
 */
export const REQUEUE_SCRIPT = `
if redis.call('EXISTS', KEYS[4]) == 0 then
  return {'NOT_FOUND'}
end
local state = redis.call('HGET', KEYS[4], 'state')
if state ~= 'unacked' then
  return {'CONFLICT', state or ''}
end
local owner = redis.call('HGET', KEYS[4], 'consumer')
if ARGV[2] ~= '' and owner ~= ARGV[2] then
  return {'WRONG_OWNER', owner or ''}
end
local deliveries = redis.call('HGET', KEYS[4], 'deliveries')
redis.call('ZREM', KEYS[3], ARGV[1])
if owner and owner ~= '' then
  redis.call('SREM', ARGV[4] .. owner .. ':pending', ARGV[1])
end
redis.call('HSET', KEYS[4], 'state', 'ready', 'consumer', '', 'visibleAt', 0, 'updatedAt', ARGV[3])
redis.call('RPUSH', KEYS[2], ARGV[1])
redis.call('HINCRBY', KEYS[1], 'requeued', 1)
return {'OK', deliveries or '0'}
`;

/**
 * Delete one waiting message (ready or delayed only).
 * KEYS: meta, ready, delayed, msg
 * ARGV: id
 * Returns {'OK', state} | {'NOT_FOUND'} | {'CONFLICT', state}.
 */
export const DELETE_MESSAGE_SCRIPT = `
if redis.call('EXISTS', KEYS[4]) == 0 then
  return {'NOT_FOUND'}
end
local state = redis.call('HGET', KEYS[4], 'state')
if state == 'unacked' then
  return {'CONFLICT', state}
end
if state == 'ready' then
  redis.call('LREM', KEYS[2], 0, ARGV[1])
elseif state == 'delayed' then
  redis.call('ZREM', KEYS[3], ARGV[1])
end
redis.call('DEL', KEYS[4])
redis.call('HINCRBY', KEYS[1], 'deleted', 1)
return {'OK', state or ''}
`;

/**
 * Change/reset one waiting message's TTL (availableAt = now + ttl).
 * Moves the message between ready and delayed as needed.
 * KEYS: meta, ready, delayed, msg
 * ARGV: id, availableAt, now
 * Returns {'OK', state} | {'NOT_FOUND'} | {'CONFLICT', state}.
 */
export const SET_TTL_SCRIPT = `
if redis.call('EXISTS', KEYS[4]) == 0 then
  return {'NOT_FOUND'}
end
local state = redis.call('HGET', KEYS[4], 'state')
if state == 'unacked' then
  return {'CONFLICT', state}
end
if state == 'ready' then
  redis.call('LREM', KEYS[2], 0, ARGV[1])
elseif state == 'delayed' then
  redis.call('ZREM', KEYS[3], ARGV[1])
end
local next
if tonumber(ARGV[2]) <= tonumber(ARGV[3]) then
  next = 'ready'
  redis.call('RPUSH', KEYS[2], ARGV[1])
else
  next = 'delayed'
  redis.call('ZADD', KEYS[3], ARGV[2], ARGV[1])
end
redis.call('HSET', KEYS[4], 'state', next, 'availableAt', ARGV[2], 'updatedAt', ARGV[3])
return {'OK', next}
`;

/**
 * Cancel one consumer: requeue all its leased messages to the ready tail.
 * Idempotent — cancelling an unknown consumer returns zero.
 * KEYS: meta, ready, unacked, consumers, pending
 * ARGV: consumer, now, maxScan, msgPrefix
 * Returns {'OK', requeued}.
 */
export const CANCEL_CONSUMER_SCRIPT = `
local ids = redis.call('SMEMBERS', KEYS[5])
local requeued = 0
local limit = tonumber(ARGV[3])
for _, id in ipairs(ids) do
  if requeued >= limit then
    break
  end
  redis.call('ZREM', KEYS[3], id)
  local m = ARGV[4] .. id
  if redis.call('EXISTS', m) == 1 then
    redis.call('HSET', m, 'state', 'ready', 'consumer', '', 'visibleAt', 0, 'updatedAt', ARGV[2])
    redis.call('RPUSH', KEYS[2], id)
    requeued = requeued + 1
  end
end
redis.call('DEL', KEYS[5])
redis.call('HDEL', KEYS[4], ARGV[1])
if requeued > 0 then
  redis.call('HINCRBY', KEYS[1], 'requeued', requeued)
end
return {'OK', requeued}
`;

/**
 * Background sweep for one queue: promote due delayed messages and
 * reclaim expired unacked leases (retry/redelivery).
 * KEYS: meta, ready, delayed, unacked
 * ARGV: now, maxScan, pendPrefix, msgPrefix
 * Returns {'OK', promoted, reclaimed}.
 */
export const SWEEP_SCRIPT = `
local now = tonumber(ARGV[1])
local scan = tonumber(ARGV[2])
local promoted = 0
local due = redis.call('ZRANGEBYSCORE', KEYS[3], 0, now, 'LIMIT', 0, scan)
for _, id in ipairs(due) do
  redis.call('ZREM', KEYS[3], id)
  local m = ARGV[4] .. id
  if redis.call('EXISTS', m) == 1 then
    redis.call('HSET', m, 'state', 'ready', 'availableAt', now, 'updatedAt', now)
    redis.call('RPUSH', KEYS[2], id)
    promoted = promoted + 1
  end
end
local reclaimed = 0
local expired = redis.call('ZRANGEBYSCORE', KEYS[4], 0, now, 'LIMIT', 0, scan)
for _, id in ipairs(expired) do
  redis.call('ZREM', KEYS[4], id)
  local m = ARGV[4] .. id
  local owner = redis.call('HGET', m, 'consumer')
  if owner and owner ~= '' then
    redis.call('SREM', ARGV[3] .. owner .. ':pending', id)
  end
  if redis.call('EXISTS', m) == 1 then
    redis.call('HSET', m, 'state', 'ready', 'consumer', '', 'visibleAt', 0, 'updatedAt', now)
    redis.call('RPUSH', KEYS[2], id)
    reclaimed = reclaimed + 1
  end
end
if reclaimed > 0 then
  redis.call('HINCRBY', KEYS[1], 'requeued', reclaimed)
end
return {'OK', promoted, reclaimed}
`;

/**
 * Stats for one queue.
 * KEYS: registry, meta, ready, delayed, unacked, consumers
 * ARGV: queue
 * Returns {'NOT_FOUND'} | {'OK', ready, delayed, unacked, consumersFlat, metaFlat}.
 */
export const STATS_SCRIPT = `
if redis.call('SISMEMBER', KEYS[1], ARGV[1]) == 0 then
  return {'NOT_FOUND'}
end
return {'OK',
  redis.call('LLEN', KEYS[3]),
  redis.call('ZCARD', KEYS[4]),
  redis.call('ZCARD', KEYS[5]),
  redis.call('HGETALL', KEYS[6]),
  redis.call('HGETALL', KEYS[2])}
`;

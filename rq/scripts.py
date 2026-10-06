import calendar
import logging
import time
from datetime import timedelta, timezone
from typing import Any, Literal

from .exceptions import DuplicateJobError
from .logutils import blue, green
from .utils import as_text

logger = logging.getLogger('rq.scripts')

# Per-job outcomes of a queue-to-queue transfer.
JOB_TRANSFER_MOVED = 'moved'
JOB_TRANSFER_NO_SUCH_JOB = 'no_such_job'
JOB_TRANSFER_NOT_IN_QUEUE = 'not_in_queue'

# Lua script for atomic unique enqueue: check existence, save job, push to queue
UNIQUE_ENQUEUE_SCRIPT = """
    -- KEYS[1] = job key (rq:job:{job_id})
    -- KEYS[2] = queue key (rq:queue:{queue_name})
    -- ARGV[1] = job_id
    -- ARGV[2] = push direction ("L", "R", or "N" for no push)
    -- ARGV[3] = TTL in seconds (-1 for no TTL)
    -- ARGV[4+] = field1, value1, field2, value2, ... for HSET

    -- Check if job hash already exists
    if redis.call("EXISTS", KEYS[1]) == 1 then
        return 0  -- Duplicate, reject
    end

    -- Save job hash
    if #ARGV > 3 then
        redis.call("HSET", KEYS[1], unpack(ARGV, 4))
    end

    -- Set TTL if specified
    local ttl = tonumber(ARGV[3])
    if ttl and ttl > 0 then
        redis.call("EXPIRE", KEYS[1], ttl)
    end

    -- Push job ID to queue (skip if "N")
    if ARGV[2] == "L" then
        redis.call("LPUSH", KEYS[2], ARGV[1])
    elseif ARGV[2] == "R" then
        redis.call("RPUSH", KEYS[2], ARGV[1])
    end

    return 1  -- Success
"""

# Cache registered scripts per connection
_registered_scripts: dict[Any, Any] = {}


def get_unique_enqueue_script(connection):
    """Get or create the registered Lua script for unique enqueue."""
    if connection not in _registered_scripts:
        _registered_scripts[connection] = connection.register_script(UNIQUE_ENQUEUE_SCRIPT)
    return _registered_scripts[connection]


def save_unique_job(connection, queue_key, job, enqueue=True, at_front=False):
    """Atomically check uniqueness, save job, and optionally push to queue using Lua script.

    Args:
        connection: Redis connection
        queue_key (str): The Redis key for the queue
        job (Job): The job to enqueue
        enqueue (bool): Whether to push job ID to the queue. Defaults to True.
            Set to False for sync jobs that don't need to be queued.
        at_front (bool): Whether to push to front of queue

    Returns:
        bool: True if job was enqueued, False if duplicate exists

    Raises:
        DuplicateJobError: If a job with the same ID already exists
    """
    script = get_unique_enqueue_script(connection)

    hset_args = _build_hset_args(job)

    # Determine TTL (-1 means no TTL)
    ttl = job.ttl if job.ttl is not None else -1

    # Determine push direction: "L" for front, "R" for back, "N" for no push
    if not enqueue:
        push_direction = 'N'
    elif at_front:
        push_direction = 'L'
    else:
        push_direction = 'R'

    # Execute the Lua script
    result = script(
        keys=[job.key, queue_key],
        args=[job.id, push_direction, ttl] + hset_args,
    )

    if result == 0:
        raise DuplicateJobError(f"Job with ID '{job.id}' already exists")

    logger.debug('Uniquely enqueued job %s into %s', blue(job.id), green(queue_key))
    return True


# Lua script for atomic unique schedule: check existence, save job, add to scheduled registry
UNIQUE_SCHEDULE_SCRIPT = """
    -- KEYS[1] = job key (rq:job:{job_id})
    -- KEYS[2] = scheduled registry key (rq:scheduled:{queue_name})
    -- KEYS[3] = queues key (rq:queues)
    -- ARGV[1] = job_id
    -- ARGV[2] = TTL in seconds (-1 for no TTL)
    -- ARGV[3] = scheduled timestamp (UTC)
    -- ARGV[4] = queue key (rq:queue:{queue_name})
    -- ARGV[5+] = field1, value1, field2, value2, ... for HSET

    -- Check if job hash already exists
    if redis.call("EXISTS", KEYS[1]) == 1 then
        return 0  -- Duplicate, reject
    end

    -- Save job hash
    if #ARGV > 4 then
        redis.call("HSET", KEYS[1], unpack(ARGV, 5))
    end

    -- Set TTL if specified
    local ttl = tonumber(ARGV[2])
    if ttl and ttl > 0 then
        redis.call("EXPIRE", KEYS[1], ttl)
    end

    -- Add to scheduled registry sorted set
    redis.call("ZADD", KEYS[2], tonumber(ARGV[3]), ARGV[1])

    -- Register queue
    redis.call("SADD", KEYS[3], ARGV[4])

    return 1  -- Success
"""


def _build_hset_args(job):
    """Build flat list of field/value pairs from job.to_dict() for use in Lua HSET calls."""
    job_data = job.to_dict()
    hset_args = []
    for key, value in job_data.items():
        if value is not None:
            hset_args.append(key)
            if isinstance(value, bytes):
                hset_args.append(value)
            else:
                hset_args.append(str(value) if not isinstance(value, str) else value)
    return hset_args


_registered_schedule_scripts: dict[Any, Any] = {}


def get_unique_schedule_script(connection):
    """Get or create the registered Lua script for unique schedule."""
    if connection not in _registered_schedule_scripts:
        _registered_schedule_scripts[connection] = connection.register_script(UNIQUE_SCHEDULE_SCRIPT)
    return _registered_schedule_scripts[connection]


def schedule_unique_job(connection, queue_key, registry_key, job, scheduled_datetime):
    """Atomically check uniqueness, save job, and add to scheduled registry using Lua script.

    Args:
        connection: Redis connection
        queue_key (str): The Redis key for the queue (e.g. rq:queue:default)
        registry_key (str): The Redis key for the scheduled registry (e.g. rq:scheduled:default)
        job (Job): The job to schedule
        scheduled_datetime (datetime): The scheduled execution time

    Returns:
        bool: True if job was scheduled successfully

    Raises:
        DuplicateJobError: If a job with the same ID already exists
    """
    script = get_unique_schedule_script(connection)

    hset_args = _build_hset_args(job)

    # Determine TTL (-1 means no TTL)
    ttl = job.ttl if job.ttl is not None else -1

    # Convert datetime to UTC timestamp (same logic as ScheduledJobRegistry.schedule)
    if not scheduled_datetime.tzinfo:
        tz = timezone(timedelta(seconds=-(time.timezone if time.daylight == 0 else time.altzone)))
        scheduled_datetime = scheduled_datetime.replace(tzinfo=tz)
    timestamp = calendar.timegm(scheduled_datetime.utctimetuple())

    queues_key = 'rq:queues'

    result = script(
        keys=[job.key, registry_key, queues_key],
        args=[job.id, ttl, timestamp, queue_key] + hset_args,
    )

    if result == 0:
        raise DuplicateJobError(f"Job with ID '{job.id}' already exists")

    logger.debug('Uniquely scheduled job %s in %s', blue(job.id), green(registry_key))
    return True


# Lua script for atomic lock acquire-or-refresh.
ACQUIRE_OR_REFRESH_LOCK_SCRIPT = """
    -- KEYS[1] = lock key
    -- ARGV[1] = owner token
    -- ARGV[2] = TTL in seconds
    -- returns 1 = acquired, 2 = refreshed, 0 = taken by another owner
    local value = redis.call('GET', KEYS[1])
    if not value then
        redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
        return 1
    elseif value == ARGV[1] then
        redis.call('EXPIRE', KEYS[1], ARGV[2])
        return 2
    end
    return 0
"""


def acquire_or_refresh_lock(
    connection, lock_key: str, owner_token: str, ttl: int
) -> Literal['acquired', 'refreshed', 'taken']:
    """Atomically acquire a lock or refresh its TTL if already held by `owner_token`.

    Args:
        connection: Redis connection
        lock_key (str): The Redis key for the lock
        owner_token (str): Token identifying the lock owner
        ttl (int): Lock TTL in seconds

    Returns:
        str: `acquired` if the lock was empty and is now owned, `refreshed` if it
            already held `owner_token` and its TTL was extended, `taken` if it is
            held by another owner (left untouched).
    """
    script = connection.register_script(ACQUIRE_OR_REFRESH_LOCK_SCRIPT)
    result = script(keys=[lock_key], args=[owner_token, ttl])
    if result == 1:
        return 'acquired'
    elif result == 2:
        return 'refreshed'
    return 'taken'


# Lua script for atomic token-checked lock release. A plain GET + DEL sequence is racy:
# the lock can change owners between the two commands, deleting another owner's lock.
RELEASE_LOCK_SCRIPT = """
    -- KEYS[1] = lock key
    -- ARGV[1] = owner token
    -- returns 1 = deleted, 0 = not owned (absent or taken by another owner, left untouched)
    if redis.call('GET', KEYS[1]) == ARGV[1] then
        return redis.call('DEL', KEYS[1])
    end
    return 0
"""


def release_lock(connection, lock_key: str, owner_token: str) -> bool:
    """Atomically delete a lock if it is still held by `owner_token`.

    Args:
        connection: Redis connection
        lock_key (str): The lock key
        owner_token (str): Token identifying the lock owner

    Returns:
        bool: True if the lock was deleted, False if it was absent or held by
            another owner (left untouched).
    """
    script = connection.register_script(RELEASE_LOCK_SCRIPT)
    return bool(script(keys=[lock_key], args=[owner_token]))


# Lua script for atomically moving waiting jobs from one queue to another.
TRANSFER_JOBS_SCRIPT = """
    -- KEYS[1] = source queue key (rq:queue:{source})
    -- KEYS[2] = destination queue key (rq:queue:{destination})
    -- KEYS[3] = all-queues set key (rq:queues)
    -- ARGV[1] = job hash namespace prefix (e.g. "rq:job:")
    -- ARGV[2] = destination queue name (the jobs' new origin)
    -- ARGV[3...] = job ids requested for transfer
    --
    -- Returns an array of status strings, one per requested id (request order):
    --   "moved"        - job was waiting in source and is now in destination
    --   "no_such_job"  - no job hash exists for the id (unknown job)
    --   "not_in_queue" - job exists, but is no longer waiting in source
    --                    (e.g. taken by a worker, canceled, or in another queue)

    local source = KEYS[1]
    local destination = KEYS[2]
    local prefix = ARGV[1]
    local destination_name = ARGV[2]

    local queue_job_ids = redis.call("LRANGE", source, 0, -1)
    local in_source = {}
    for _, id in ipairs(queue_job_ids) do
        in_source[id] = true
    end

    local requested = {}
    local statuses = {}
    for i = 3, #ARGV do
        local id = ARGV[i]
        requested[id] = true
        local status
        if redis.call("EXISTS", prefix .. id) == 0 then
            status = "no_such_job"
        elseif in_source[id] then
            status = "moved"
        else
            status = "not_in_queue"
        end
        statuses[#statuses + 1] = status
    end

    if source ~= destination then
        -- Walk source head-to-tail so transferred jobs keep their source-relative
        -- order; RPUSH appends them behind the destination's existing jobs.
        for _, id in ipairs(queue_job_ids) do
            if requested[id] and redis.call("EXISTS", prefix .. id) == 1 then
                -- count 0: remove every (possibly duplicated) occurrence, so the
                -- same id can never remain in both queues.
                redis.call("LREM", source, 0, id)
                redis.call("HSET", prefix .. id, "origin", destination_name)
                redis.call("RPUSH", destination, id)
            end
        end
        redis.call("SADD", KEYS[3], destination)
    end

    return statuses
"""

_registered_transfer_scripts: dict[Any, Any] = {}


def get_transfer_jobs_script(connection):
    """Get or create the registered Lua script for queue-to-queue transfers."""
    if connection not in _registered_transfer_scripts:
        _registered_transfer_scripts[connection] = connection.register_script(TRANSFER_JOBS_SCRIPT)
    return _registered_transfer_scripts[connection]


def transfer_jobs(
    connection,
    source_key: str,
    destination_key: str,
    job_key_prefix: str,
    destination_name: str,
    job_ids: list[str],
) -> list[str]:
    """Atomically move waiting jobs from source queue to destination queue.

    Jobs that exist and are still waiting in source are removed from source and
    appended to destination in their source-relative order, with each job's
    `origin` field updated. Unknown ids (missing job hash) and ids no longer
    waiting in source are reported per job and left untouched.

    Args:
        connection: Redis connection
        source_key: Redis key of the source queue
        destination_key: Redis key of the destination queue
        job_key_prefix: Namespace prefix for job hash keys (e.g. "rq:job:")
        destination_name: Name of the destination queue (new `origin` value)
        job_ids: Job ids requested for transfer

    Returns:
        Status strings (`moved`, `no_such_job` or `not_in_queue`), one per
        requested id in request order.
    """
    script = get_transfer_jobs_script(connection)
    result = script(
        keys=[source_key, destination_key, 'rq:queues'],
        args=[job_key_prefix, destination_name, *job_ids],
    )
    return [as_text(status) for status in result]

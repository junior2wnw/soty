"""Synthetic qualification peer; stdin receives fixed test-only app pins."""
import asyncio
import json
import sys
from soty_native_entry import NativeEntryGuard, NativeEntryError


def emit(value):
    print(json.dumps(value), flush=True)


async def run():
    configuration = json.loads(sys.stdin.readline())
    guard = NativeEntryGuard(configuration)
    ended = asyncio.Event()
    def closed(reason):
        emit({'type': 'closed', 'reason': reason})
        ended.set()
    try:
        lease = await guard.acquire()
        guard.assert_current(lease)
        stream = await guard.open_stream(closed)
        emit({'type': 'ready', 'policyEpoch': lease.policy_epoch})
        await asyncio.wait_for(ended.wait(), 10)
        try:
            stream.assert_current()
            emit({'type': 'error', 'code': 'closed_stream_reopened'})
        except NativeEntryError:
            emit({'type': 'finished', 'closed': True})
    except NativeEntryError as error:
        emit({'type': 'error', 'code': error.code})
        raise SystemExit(1) from None
    finally:
        await guard.close()


asyncio.run(run())

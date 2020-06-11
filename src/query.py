import aiohttp
import asyncio
from datetime import datetime
import logging
from queue import Empty
from requests import get
from requests.exceptions import Timeout


def query_baseline(queue, results, throttle, info, num):
    url = 'https://jsonplaceholder.typicode.com/posts'
    log = logging.getLogger('Runner.Event')
    while queue.qsize():
        try:
            work = queue.get_nowait()
        except Empty:
            break
        log.debug('Agent-%i: Batch %i: Acquiring semaphore', num, work[0])
        with throttle:
            start = datetime.now()
            try:
                response = get(url, timeout=10)
            except Timeout:
                log.error('Agent-%i: Batch %i timed out', num, work[0])
                queue.put_nowait(work)
                info[1] += 1
                continue
            except Exception as e:
                log.error('Agent-%i: %s', num, e)
                queue.put_nowait(work)
                info[2] += 1
                continue
            log.debug(
                'Agent-%i: Batch %i: Putting result in queue',
                num,
                work[0])
            results.put_nowait(response.json())
            end = datetime.now()
            info[0] += 1
            log.info(
                'Agent-%i: Queried batch %i, runtime %f',
                num,
                work[0],
                (end - start).total_seconds()
            )


def alt_baseline(queue, results, throttle, info, num):
    url = 'https://api-xbox-console.worldoftanks.com/wotx/'
    log = logging.getLogger('Runner.Event')
    while queue.qsize():
        try:
            work = queue.get_nowait()
        except Empty:
            break
        log.debug('Agent-%i: Batch %i: Acquiring semaphore', num, work[0])
        with throttle:
            start = datetime.now()
            try:
                response = get(url, timeout=10)
            except Timeout:
                log.error('Agent-%i: Batch %i timed out', num, work[0])
                queue.put_nowait(work)
                info[1] += 1
                continue
            except Exception as e:
                log.error('Agent-%i: %s', num, e)
                queue.put_nowait(work)
                info[2] += 1
                continue
            log.debug(
                'Agent-%i: Batch %i: Putting result in queue',
                num,
                work[0])
            results.put_nowait(response.json())
            end = datetime.now()
            info[0] += 1
            log.info(
                'Agent-%i: Queried batch %i, runtime %f',
                num,
                work[0],
                (end - start).total_seconds()
            )


def query_process(key, queue, results, throttle, info, num):
    url = 'https://api-xbox-console.worldoftanks.com/wotx/account/info/'
    params = {
        'account_id': None,
        'application_id': key,
        'fields': 'account_id',
        'language': 'en'
    }
    log = logging.getLogger('Runner.Event')
    while queue.qsize():
        try:
            work = queue.get_nowait()
        except Empty:
            break
        log.debug('Agent-%i: Batch %i: Acquiring semaphore', num, work[0])
        with throttle:
            start = datetime.now()
            params['account_id'] = ','.join(map(str, range(*work[1])))
            try:
                response = get(url, params=params, timeout=10)
                j = response.json()
                if 'error' in j:
                    raise Exception(j)
            except Timeout:
                log.error('Agent-%i: Batch %i timed out', num, work[0])
                queue.put_nowait(work)
                info[1] += 1
                continue
            except Exception as e:
                log.error('Agent-%i: %s', num, e)
                queue.put_nowait(work)
                info[2] += 1
                continue
            log.debug(
                'Agent-%i: Batch %i: Putting result in queue',
                num,
                work[0])
            results.put_nowait(j)
            end = datetime.now()
            info[0] += 1
            log.info(
                'Agent-%i: Queried batch %i, runtime %f',
                num,
                work[0],
                (end - start).total_seconds()
            )


async def base_query_async(session, workqueue, resultqueue, stats_list, log, num):
    url = 'https://jsonplaceholder.typicode.com/posts'
    try:
        work = workqueue.get_nowait()
    except Empty:
        log.warning('Worker-%i: Queue empty', num)
        return
    log.debug('Worker-%i: Batch %i: Starting', num, work[0])
    start = datetime.now()
    try:
        log.debug('Worker-%i: Batch %i: Querying API', num, work[0])
        response = await session.get(url)
        log.debug(
            'Worker-%i: Batch %i: %f seconds to complete request',
            num,
            work[0],
            (datetime.now() - start).total_seconds()
        )
    except aiohttp.ClientConnectionError:
        stats_list[1] += 1
        workqueue.put_nowait(work)
        log.warning('Worker-%i: Batch %i: Timeout reached', num, work[0])
        return
    if response.status != 200:
        workqueue.put_nowait(work)
        log.warning(
            'Worker-%i: Batch %i: Status code %i',
            num,
            work[0],
            response.status
        )
        stats_list[2] += 1
        return
    log.debug('Worker-%i: Batch %i: Awaiting response', num, work[0])
    result = await response.json()  # Benchmark requires finished JSON. Mimic.
    log.debug('Worker-%i: Batch %i: Putting result in queue', num, work[0])
    resultqueue.put_nowait(result)
    stats_list[0] += 1
    end = datetime.now()
    log.info(
        'Worker-%i: Batch %i: %f seconds of runtime',
        num,
        work[0],
        (end - start).total_seconds()
    )


async def base_query_loop(workqueue, resultqueue, stats_list, workers, log, num):
    gap = (1 / 50) * workers
    async with aiohttp.ClientSession() as session:
        # Reference: https://stackoverflow.com/a/48682456/1993468
        while workqueue.qsize():
            if len(asyncio.all_tasks()) >= 100:
                log.error('Worker-%i: Max tasks in play', num)
            else:
                log.debug('Worker-%i: Creating task', num)
                asyncio.ensure_future(
                    base_query_async(
                        session,
                        workqueue,
                        resultqueue,
                        stats_list,
                        log,
                        num
                    )
                )
                stats_list[3][num - 1] = len(asyncio.all_tasks())
            await asyncio.sleep(gap)


def base_query_handler(workqueue, resultqueue, stats_list, workers, num):
    loop = asyncio.new_event_loop()
    log = logging.getLogger('Runner.Event')
    asyncio.set_event_loop(loop)
    asyncio.run(
        base_query_loop(
            workqueue,
            resultqueue,
            stats_list,
            workers,
            log,
            num
        )
    )


async def alt_query_async(session, workqueue, resultqueue, stats_list, log, num):
    url = 'https://api-xbox-console.worldoftanks.com/wotx/'
    try:
        work = workqueue.get_nowait()
    except Empty:
        log.warning('Worker-%i: Queue empty', num)
        return
    log.debug('Worker-%i: Batch %i: Starting', num, work[0])
    start = datetime.now()
    try:
        log.debug('Worker-%i: Batch %i: Querying API', num, work[0])
        response = await session.get(url)
        log.debug(
            'Worker-%i: Batch %i: %f seconds to complete request',
            num,
            work[0],
            (datetime.now() - start).total_seconds()
        )
    except aiohttp.ClientConnectionError:
        stats_list[1] += 1
        workqueue.put_nowait(work)
        log.warning('Worker-%i: Batch %i: Timeout reached', num, work[0])
        return
    if response.status != 200:
        workqueue.put_nowait(work)
        log.warning(
            'Worker-%i: Batch %i: Status code %i',
            num,
            work[0],
            response.status
        )
        stats_list[2] += 1
        return
    log.debug('Worker-%i: Batch %i: Awaiting response', num, work[0])
    result = await response.json()  # Benchmark requires finished JSON. Mimic.
    log.debug('Worker-%i: Batch %i: Putting result in queue', num, work[0])
    resultqueue.put_nowait(result)
    stats_list[0] += 1
    end = datetime.now()
    log.info(
        'Worker-%i: Batch %i: %f seconds of runtime',
        num,
        work[0],
        (end - start).total_seconds()
    )


async def alt_query_loop(workqueue, resultqueue, stats_list, workers, log, num):
    gap = (1 / 50) * workers
    async with aiohttp.ClientSession() as session:
        # Reference: https://stackoverflow.com/a/48682456/1993468
        while workqueue.qsize():
            if len(asyncio.all_tasks()) >= 100:
                log.error('Worker-%i: Max tasks in play', num)
            else:
                log.debug('Worker-%i: Creating task', num)
                asyncio.ensure_future(
                    alt_query_async(
                        session,
                        workqueue,
                        resultqueue,
                        stats_list,
                        log,
                        num
                    )
                )
                stats_list[3][num - 1] = len(asyncio.all_tasks())
            await asyncio.sleep(gap)


def alt_query_handler(workqueue, resultqueue, stats_list, workers, num):
    loop = asyncio.new_event_loop()
    log = logging.getLogger('Runner.Event')
    asyncio.set_event_loop(loop)
    asyncio.run(
        alt_query_loop(
            workqueue,
            resultqueue,
            stats_list,
            workers,
            log,
            num
        )
    )


async def bench_query_async(key, session, workqueue, resultqueue, stats_list, log, num):
    url = 'http://api-xbox-console.worldoftanks.com/wotx/account/info/'
    try:
        work = workqueue.get_nowait()
    except Empty:
        log.warning('Worker-%i: Queue empty', num)
        return
    log.debug('Worker-%i: Batch %i: Starting', num, work[0])
    start = datetime.now()
    params = {
        'account_id': ','.join(map(str, range(*work[1]))),
        'application_id': key,
        'fields': 'created_at,account_id,last_battle_time,nickname,updated_at,statistics.all.battles',
        'language': 'en'
    }
    try:
        log.debug('Worker-%i: Batch %i: Querying API', num, work[0])
        response = await session.get(url, params=params)
        log.debug(
            'Worker-%i: Batch %i: %f seconds to complete request',
            num,
            work[0],
            (datetime.now() - start).total_seconds()
        )
    except aiohttp.ClientConnectionError:
        stats_list[1] += 1
        workqueue.put_nowait(work)
        log.warning('Worker-%i: Batch %i: Timeout reached', num, work[0])
        return
    if response.status != 200:
        workqueue.put_nowait(work)
        log.warning(
            'Worker-%i: Batch %i: Status code %i',
            num,
            work[0],
            response.status
        )
        stats_list[2] += 1
        return
    log.debug('Worker-%i: Batch %i: Awaiting response', num, work[0])
    result = await response.json()
    if 'error' in result:
        workqueue.put_nowait(work)
        log.error('Worker-%i: Batch %i: %s', num, work[0], str(result))
        return
    log.debug('Worker-%i: Batch %i: Putting result in queue', num, work[0])
    resultqueue.put_nowait(result)
    stats_list[0] += 1
    end = datetime.now()
    log.info(
        'Worker-%i: Batch %i: %f seconds of runtime',
        num,
        work[0],
        (end - start).total_seconds()
    )


async def bench_query_loop(key, workqueue, resultqueue, stats_list, workers, log, num):
    gap = (1 / 20) * workers
    async with aiohttp.ClientSession() as session:
        # Reference: https://stackoverflow.com/a/48682456/1993468
        while workqueue.qsize():
            if len(asyncio.all_tasks()) >= 100:
                log.error('Worker-%i: Max tasks in play', num)
            else:
                log.debug('Worker-%i: Creating task', num)
                asyncio.ensure_future(
                    bench_query_async(
                        key,
                        session,
                        workqueue,
                        resultqueue,
                        stats_list,
                        log,
                        num
                    )
                )
                stats_list[3][num - 1] = len(asyncio.all_tasks())
            await asyncio.sleep(gap)


def bench_query_handler(key, workqueue, resultqueue, stats_list, workers, num):
    loop = asyncio.new_event_loop()
    log = logging.getLogger('Runner.Event')
    asyncio.set_event_loop(loop)
    asyncio.run(
        bench_query_loop(
            key,
            workqueue,
            resultqueue,
            stats_list,
            workers,
            log,
            num
        )
    )


class QueryAPI(object):

    data_fields = (
        'created_at,'
        'account_id,'
        'last_battle_time,'
        'nickname,'
        'updated_at,'
        'statistics.all.battles'
    )
    api_url = 'http://api-xbox-console.worldoftanks.com/wotx/account/info/'

    def __init__(self, key, session, workqueue, resultqueue, workdone, log):
        self.key = key
        self.log = log
        self.session = session
        self.work = workqueue
        self.results = resultqueue
        self.workdone = workdone

    async def query(self):
        try:
            work = self.work.get_nowait()
        except Empty:
            self.log.warning('Queue empty')
            self.workdone[3] += 1
            return
        self.log.debug('Batch %i: Starting', work[0])
        start = datetime.now()
        params = {
            'account_id': ','.join(map(str, range(*work[1]))),
            'application_id': self.key,
            'fields': self.data_fields,
            'language': 'en'
        }
        try:
            # self.workdone[4] += 1
            self.log.debug('Batch %i: Querying API', work[0])
            response = await self.session.get(
                self.api_url,
                params=params
            )
            self.log.debug(
                'Batch %i: %f seconds to complete request',
                work[0],
                (datetime.now() - start).total_seconds()
            )
            # self.workdone[4] -= 1
        except aiohttp.ClientConnectionError:
            # self.workdone[4] -= 1
            self.workdone[1] += 1
            self.work.put_nowait(work)
            self.log.warning('Batch %i: Timeout reached', work[0])
            return
        if response.status != 200:
            self.work.put_nowait(work)
            self.log.warning(
                'Batch %i: Status code %i',
                work[0],
                response.status
            )
            self.workdone[2] += 1
            return
        self.log.debug('Batch %i: Awaiting full result response', work[0])
        result = await response.json()
        if 'error' in result:
            self.work.put_nowait(work)
            self.log.error('Batch %i: %s', work[0], str(result))
            return
        self.log.debug('Batch %i: Sending JSON to result queue', work[0])
        self.results.put_nowait(
            (
                result,
                start,
                work
            )
        )
        self.workdone[0] += 1
        end = datetime.now()
        self.log.debug(
            'Batch %i: %f seconds of runtime',
            work[0],
            (end - start).total_seconds()
        )


async def class_query_loop(key, workqueue, resultqueue, stats_list, workers, num, log):
    gap = (1 / 20) * workers
    conn = aiohttp.TCPConnector(ttl_dns_cache=3600)
    async with aiohttp.ClientSession(connector=conn) as session:
        worker = QueryAPI(
            key,
            session,
            workqueue,
            resultqueue,
            stats_list,
            log
        )
        while workqueue.qsize():
            if len(asyncio.all_tasks()) >= 100:
                worker.log.error('Max tasks in play')
            else:
                asyncio.ensure_future(worker.query())
            stats_list[3][num - 1] = len(asyncio.all_tasks())
            await asyncio.sleep(gap)


def class_query_handler(key, workqueue, resultqueue, stats_list, workers, num):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.set_debug(enabled=True)
    log = logging.getLogger('Runner.Event')
    asyncio.run(
        class_query_loop(
            key,
            workqueue,
            resultqueue,
            stats_list,
            workers,
            num,
            log
        )
    )

from argparse import ArgumentParser
from cpuinfo import get_cpu_info
import logging
from multiprocessing import Process
from os import mkdir
from os.path import exists
from psutil import cpu_count
from time import sleep
from uuid import uuid4
from zipfile import ZipFile, ZIP_DEFLATED

from query import query_process, alt_baseline, query_baseline, bench_query_handler, base_query_handler, alt_query_handler, class_query_handler
from utils import EnhancedSyncManager

log_run = logging.getLogger('Runner.Event')
log_stats = logging.getLogger('Runner.Stats')

# Two sets of rounds
#   Set 1 -> https://yesno.wtf/api?ref=public-apis
#   Set 2 -> https://api-{}-console.worldoftanks.com
#   Set 3 -> http://api-xbox-console.worldoftanks.com/wotx/
#   Set 4 -> Use classes
# Three rounds
#   Round 1 -> One process per core
#   Round 2 -> One asyncio event loop
#   Round 3 -> Many asyncio event loops

logfiles = {
    1: {
        1: (
            'logs/baseline_multiprocess_events.log',
            'logs/baseline_multiprocess_stats.log'
        ),
        2: (
            'logs/baseline_oneloop_events.log',
            'logs/baseline_oneloop_stats.log'
        ),
        3: (
            'logs/baseline_manyloop_events.log',
            'logs/baseline_manyloop_stats.log'
        )
    },
    2: {
        1: (
            'logs/benchmark_multiprocess_events.log',
            'logs/benchmark_multiprocess_stats.log'
        ),
        2: (
            'logs/benchmark_oneloop_events.log',
            'logs/benchmark_oneloop_stats.log'
        ),
        3: (
            'logs/benchmark_manyloop_events.log',
            'logs/benchmark_manyloop_stats.log'
        )
    },
    3: {
        1: (
            'logs/benchmark_alt_multiprocess_events.log',
            'logs/benchmark_alt_multiprocess_stats.log'
        ),
        2: (
            'logs/benchmark_alt_oneloop_events.log',
            'logs/benchmark_alt_oneloop_stats.log'
        ),
        3: (
            'logs/benchmark_alt_manyloop_events.log',
            'logs/benchmark_alt_manyloop_stats.log'
        )
    },
    4: {
        1: (
            'logs/class_events.log',
            'logs/class_stats.log'
        )
    }
}


def setup_logs(level=logging.INFO, run_set=1, round=1):
    eventlog, statlog = logfiles[run_set][round]
    formatter = logging.Formatter(
        '%(asctime)s.%(msecs)03d | %(name)-12s | %(levelname)-8s | %(message)s',
        datefmt='%m-%d %H:%M:%S'
    )
    fh = logging.FileHandler(eventlog)
    fh.setLevel(level)
    fh.setFormatter(formatter)
    log_run.addHandler(fh)
    log_run.setLevel(level)

    formatter = logging.Formatter(
        '%(asctime)s.%(msecs)03d,%(message)s',
        datefmt='%H:%M:%S'
    )
    fh = logging.FileHandler(statlog)
    fh.setLevel(level)
    fh.setFormatter(formatter)
    log_stats.addHandler(fh)
    log_stats.setLevel(level)
    if round == 1:
        log_stats.info('Completed,Timeouts,Errors')
    else:
        log_stats.info('Completed,Timeouts,Errors,Tasks')


def reset_logs():
    __ = tuple(map(log_run.removeHandler, log_run.handlers[:]))
    __ = tuple(map(log_stats.removeHandler, log_stats.handlers[:]))


def reset_stats(stats, manager):
    stats[0] = 0
    stats[1] = 0
    stats[2] = 0
    stats[3] = manager.list()


def collect_hardware_stats():
    cpu_info = get_cpu_info()
    with open('logs/cpu.txt', 'w') as f:
        f.write(
            'Processor: {}\nCores: {}'.format(
                cpu_info['brand'],
                cpu_count(logical=False)
            )
        )
    physical_cores = cpu_count(logical=False)
    return physical_cores


def empty_the_queue(queue):
    while queue.qsize():
        queue.get_nowait()


def reset_env(stats, queue, manager, results):
    empty_the_queue(work)
    empty_the_queue(results)
    reset_logs()
    reset_stats(stats, manager)


def run_baseline(max_processes, loglevel, manager, work, stats, throttler):
    # Round 1
    setup_logs(loglevel, 1, 1)
    for i in range(1, 5001):
        work.put_nowait((i, ))

    runners = [
        Process(
            target=query_baseline,
            args=(
                work,
                results,
                throttler,
                stats,
                i
            )
        ) for i in range(1, max_processes + 1)
    ]
    log_run.info('Starting runners')
    try:
        for process in runners:
            process.start()
        while work.qsize():
            log_stats.info('%i,%i,%i', *stats[:3])
            sleep(1)
    except KeyboardInterrupt:
        pass
    for process in runners:
        process.terminate()
    reset_env(stats, work, manager, results)

    # Round 2
    setup_logs(loglevel, 1, 2)
    for i in range(1, 5001):
        work.put_nowait((i, ))
    workers = 1
    runners = [
        Process(
            target=base_query_handler,
            args=(
                work,
                results,
                stats,
                workers,
                i
            )
        ) for i in range(1, workers + 1)
    ]
    try:
        for process in runners:
            process.start()
            stats[3].append(0)
        while work.qsize():
            log_stats.info('%i,%i,%i,%s', *stats[:3], ','.join(map(str, stats[3])))
            sleep(1)
    except KeyboardInterrupt:
        pass
    for process in runners:
        process.terminate()
    reset_env(stats, work, manager, results)

    # Round 3
    setup_logs(loglevel, 1, 3)
    for i in range(1, 5001):
        work.put_nowait((i, ))
    workers = max_processes
    runners = [
        Process(
            target=base_query_handler,
            args=(
                work,
                results,
                stats,
                workers,
                i
            )
        ) for i in range(1, workers + 1)
    ]
    try:
        for process in runners:
            process.start()
            stats[3].append(0)
        while work.qsize():
            log_stats.info('%i,%i,%i,%s', *stats[:3], ','.join(map(str, stats[3])))
            sleep(1)
    except KeyboardInterrupt:
        pass
    for process in runners:
        process.terminate()
    reset_env(stats, work, manager, results)


def run_alt(max_processes, loglevel, manager, work, stats, throttler):
    # Round 1
    setup_logs(loglevel, 3, 1)
    for i in range(1, 5001):
        work.put_nowait((i, ))

    runners = [
        Process(
            target=alt_baseline,
            args=(
                work,
                results,
                throttler,
                stats,
                i
            )
        ) for i in range(1, max_processes + 1)
    ]
    log_run.info('Starting runners')
    try:
        for process in runners:
            process.start()
        while work.qsize():
            log_stats.info('%i,%i,%i', *stats[:3])
            sleep(1)
    except KeyboardInterrupt:
        pass
    for process in runners:
        process.terminate()
    reset_env(stats, work, manager, results)

    # Round 2
    setup_logs(loglevel, 3, 2)
    for i in range(1, 5001):
        work.put_nowait((i, ))
    workers = 1
    runners = [
        Process(
            target=alt_query_handler,
            args=(
                work,
                results,
                stats,
                workers,
                i
            )
        ) for i in range(1, workers + 1)
    ]
    try:
        for process in runners:
            process.start()
            stats[3].append(0)
        while work.qsize():
            log_stats.info('%i,%i,%i,%s', *stats[:3], ','.join(map(str, stats[3])))
            sleep(1)
    except KeyboardInterrupt:
        pass
    for process in runners:
        process.terminate()
    reset_env(stats, work, manager, results)

    # Round 3
    setup_logs(loglevel, 3, 3)
    for i in range(1, 5001):
        work.put_nowait((i, ))
    workers = max_processes
    runners = [
        Process(
            target=alt_query_handler,
            args=(
                work,
                results,
                stats,
                workers,
                i
            )
        ) for i in range(1, workers + 1)
    ]
    try:
        for process in runners:
            process.start()
            stats[3].append(0)
        while work.qsize():
            log_stats.info('%i,%i,%i,%s', *stats[:3], ','.join(map(str, stats[3])))
            sleep(1)
    except KeyboardInterrupt:
        pass
    for process in runners:
        process.terminate()
    reset_env(stats, work, manager, results)


def run_benchmark(key, max_processes, loglevel, manager,
                  work, results, stats, throttler):
    # Round 1
    setup_logs(loglevel, 2, 1)
    for i, start in enumerate(range(5000, 505000, 100), 1):
        work.put_nowait((i, (start, start + 100)))

    runners = [
        Process(
            target=query_process,
            args=(
                key,
                work,
                results,
                throttler,
                stats,
                i
            )
        ) for i in range(1, max_processes + 1)
    ]
    log_run.info('Starting runners')
    try:
        for process in runners:
            process.start()
        while work.qsize():
            log_stats.info('%i,%i,%i', *stats[:3])
            sleep(1)
    except KeyboardInterrupt:
        pass
    for process in runners:
        process.terminate()
    reset_env(stats, work, manager, results)

    # Round 2
    setup_logs(loglevel, 2, 2)
    for i, start in enumerate(range(5000, 505000, 100), 1):
        work.put_nowait((i, (start, start + 100)))
    workers = 1
    runners = [
        Process(
            target=bench_query_handler,
            args=(
                key,
                work,
                results,
                stats,
                workers,
                i
            )
        ) for i in range(1, workers + 1)
    ]
    try:
        for process in runners:
            process.start()
            stats[3].append(0)
        while work.qsize():
            log_stats.info('%i,%i,%i,%s', *stats[:3], ','.join(map(str, stats[3])))
            sleep(1)
    except KeyboardInterrupt:
        pass
    for process in runners:
        process.terminate()
    reset_env(stats, work, manager, results)

    # Round 3
    setup_logs(loglevel, 2, 3)
    for i, start in enumerate(range(5000, 505000, 100), 1):
        work.put_nowait((i, (start, start + 100)))
    workers = max_processes
    runners = [
        Process(
            target=bench_query_handler,
            args=(
                key,
                work,
                results,
                stats,
                workers,
                i
            )
        ) for i in range(1, workers + 1)
    ]
    try:
        for process in runners:
            process.start()
            stats[3].append(0)
        while work.qsize():
            log_stats.info('%i,%i,%i,%s', *stats[:3], ','.join(map(str, stats[3])))
            sleep(1)
    except KeyboardInterrupt:
        pass
    for process in runners:
        process.terminate()
    reset_env(stats, work, manager, results)


def run_classes(key, loglevel, manager, work, results, stats):
    # Round 1
    setup_logs(loglevel, 4, 1)
    for i, start in enumerate(range(5000, 505000, 100), 1):
        work.put_nowait((i, (start, start + 100)))
    workers = 1
    runners = [
        Process(
            target=class_query_handler,
            args=(
                key,
                work,
                results,
                stats,
                workers,
                i
            )
        ) for i in range(1, workers + 1)
    ]
    try:
        for process in runners:
            process.start()
            stats[3].append(0)
        while work.qsize():
            log_stats.info('%i,%i,%i,%s', *stats[:3], ','.join(map(str, stats[3])))
            sleep(1)
    except KeyboardInterrupt:
        pass
    for process in runners:
        process.terminate()
    reset_env(stats, work, manager, results)


if __name__ == '__main__':
    agp = ArgumentParser(description='WoT API Stress Test')
    agp.add_argument(
        '-a',
        '--api-key',
        type=str,
        default='key.txt',
        help='File with API key')
    agp.add_argument('-d', '--debug', action='store_true')
    agp.add_argument('-r', '--requests', type=int, default=20)
    args = agp.parse_args()

    with open(args.api_key) as f:
        api_key = ''.join(f.readlines()).strip()

    if not api_key:
        raise Exception('No key in {}'.format(args.api_key))

    loglevel = logging.DEBUG if args.debug else logging.INFO
    if not exists('logs'):
        mkdir('logs')
    # Main process and Manager need their own process spaces
    max_processes = collect_hardware_stats() - 2
    # manager = Manager()
    manager = EnhancedSyncManager()
    manager.start()
    # Manager needs time to initizlize before creating Queue and List
    sleep(0.1)
    work = manager.Queue()
    results = manager.Queue()
    stats = manager.list()
    throttler_baseline = manager.RatedSemaphore(50)
    throttler_benchmark = manager.RatedSemaphore(args.requests)

    stats.append(0)  # Completed
    stats.append(0)  # Timeouts
    stats.append(0)  # Errors
    stats.append(manager.list())  # Asyncio Tasks

    run_baseline(
        max_processes,
        loglevel,
        manager,
        work,
        stats,
        throttler_baseline
    )

    run_benchmark(
        api_key,
        max_processes,
        loglevel,
        manager,
        work,
        results,
        stats,
        throttler_benchmark
    )

    run_alt(
        max_processes,
        loglevel,
        manager,
        work,
        stats,
        throttler_baseline
    )

    run_classes(
        api_key,
        loglevel,
        manager,
        work,
        results,
        stats
    )

    manager.shutdown()
    zipf = ZipFile('{}.zip'.format(uuid4()), 'w', ZIP_DEFLATED)
    for key in logfiles.keys():
        for a, b in logfiles[key].values():
            try:
                zipf.write(a)
            except:
                pass
            try:
                zipf.write(b)
            except:
                pass
    try:
        zipf.write('logs/cpu.txt')
    except:
        pass
    zipf.close()

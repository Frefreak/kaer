#!/usr/bin/env python

import argparse
import signal
import sys
import time
from threading import Event, Thread

import click
import kafka
from prompt_toolkit import prompt
#  from prompt_toolkit.auto_suggest import AutoSuggestFromHistory, AutoSuggest
from prompt_toolkit.completion import filesystem, WordCompleter
from prompt_toolkit.history import FileHistory
from prompt_toolkit.shortcuts import confirm

parser = argparse.ArgumentParser()
parser.add_argument("-b", "--brokers")

topics = []

def list_stuffs(title, li):
    s = f"\x1b[31;1m{title}\x1b[0m({len(li)})\n"
    li = sorted(li)
    s += "\n".join(sorted(li))
    print(s)
    if len(li) > click.get_terminal_size()[1]:
        click.echo_via_pager(s)


def prelude(args):
    client = kafka.KafkaAdminClient(bootstrap_servers=args.brokers)

    consumer_groups = [f"{x[0]} ({x[1]})" for x in client.list_consumer_groups()]
    list_stuffs("consumer groups", consumer_groups)

    brokers = [f"{x['host']}:{x['port']}" for x in client.describe_cluster()["brokers"]]
    list_stuffs("brokers", brokers)

    global topics
    topics = client.list_topics()
    list_stuffs("topic", topics)

    print("\x1b[32;1m---\x1b[0m")


def mk_prompt(title, hist_file):
    return prompt(
        title, history=FileHistory(hist_file), completer=WordCompleter(topics),
    )


has_fg_job = Event()
fg_need_to_stop = Event()


def ctrl_c_handler(_, __):
    if has_fg_job.is_set():
        print('setting fg_need_to_stop')
        sys.stdout.flush()
        fg_need_to_stop.set()
    else:
        sys.exit(0)


def consume_topic(client, topic, bg, bg_need_stop_evt):
    if not bg:
        has_fg_job.set()
    client.subscribe(topic)
    while not fg_need_to_stop.is_set() and not bg_need_stop_evt.is_set():
        for msg in client:
            try:
                print(f"[\x1b[33;1m{topic}\x1b[0m]: {msg.value.decode()}")
            except UnicodeDecodeError:
                print(f"[\x1b[33;1m{topic}\x1b[0m]: {msg.value}")
            if fg_need_to_stop.is_set() or bg_need_stop_evt.is_set():
                break
    print(f"job consuming {topic} exiting")
    if not bg:
        has_fg_job.clear()
        fg_need_to_stop.clear()


def do_cmd_consume(args, bg_jobs):
    bg = confirm("running in background?")
    topic = mk_prompt("topic: ", "topics.txt")
    client = kafka.KafkaConsumer(
        bootstrap_servers=args.brokers, consumer_timeout_ms=500
    )
    bg_stop_evt = Event()
    if bg:
        th = Thread(target=consume_topic, args=(client, topic, bg, bg_stop_evt))
        job_n = len(bg_jobs) + 1
        bg_jobs[job_n] = {
            "no": job_n,
            "info": f"topic {topic}, time {time.ctime()}",
            "thread": th,
            "stop_evt": bg_stop_evt,
        }
        print(f"job {job_n} created")
        th.start()
        return
    consume_topic(client, topic, bg, bg_stop_evt)

def do_cmd_produce(args):
    topic = mk_prompt("topic: ", "topics.txt")
    client = kafka.KafkaProducer(
        bootstrap_servers=args.brokers
    )
    print('1. stdin')
    print('2. file')
    while True:
        choice = prompt('which mode? ')
        if choice not in ['1', '2']:
            print('invalid')
            continue
        break
    if choice == '2':
        file = prompt('which file: ', completer=filesystem.PathCompleter())
        with open(file) as f:
            for ln in f.read().splitlines():
                client.send(topic, ln.encode())
    elif choice == '1':
        print('type whatever you want to send.(Ctrl-D) to break')
        while not fg_need_to_stop.is_set():
            try:
                inp = prompt('> ')
                client.send(topic, inp.encode())
            except EOFError:
                break


def do_cmd_kill(args, bg_jobs):
    for i, job in bg_jobs.items():
        print(f'{i}: {job["info"]}')
    while True:
        choice = prompt("which job? ")
        try:
            choice = int(choice)
            break
        except ValueError:
            continue
    if choice not in bg_jobs:
        print("job not exist")
    else:
        bg_jobs[choice]["stop_evt"].set()
        bg_jobs.pop(choice)


def main():
    signal.signal(signal.SIGINT, ctrl_c_handler)
    args = parser.parse_args()
    if args.brokers is None:
        args.brokers = input("broker list: ")
    prelude(args)

    bg_jobs = {}
    while True:
        print("1. consume")
        print("2. produce")
        print("3. kill bg job")
        choice = prompt("what do you say? ")
        if choice not in ["1", "2", "3"]:
            print("invalid.")
            continue
        if choice == "1":
            do_cmd_consume(args, bg_jobs)
        elif choice == '2':
            do_cmd_produce(args)
        elif choice == "3":
            do_cmd_kill(args, bg_jobs)


if __name__ == "__main__":
    main()

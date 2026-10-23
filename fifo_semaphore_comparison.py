#!/usr/bin/env python3

from dataclasses import dataclass, field
from collections import deque
from math import lcm
import csv

@dataclass
class Task:
    name: str
    C: int
    T: int
    D: int

@dataclass
class Job:
    task: Task
    jid: int
    release: int
    deadline: int
    remaining: int
    first_start: int | None = None
    completion: int | None = None
    executed: int = 0
    resource_requested: bool = False
    resource_acquired: bool = False
    resource_wait_start: int | None = None
    resource_wait: int = 0
    blocked_ticks: int = 0
    in_cs: bool = False
    cs_remaining: int = 0

    @property
    def label(self):
        return f"{self.task.name}{self.jid}"

def simulate(tasks, policy, horizon=None):
    H = lcm(*(t.T for t in tasks))
    horizon = horizon or H
    jobs, ready, blocked = [], [], []
    running = None
    lock_owner = None
    cs_left = 0
    jid_count = {t.name: 0 for t in tasks}
    gantt = []
    for now in range(horizon):
        # Release jobs at this integer time.
        for t in tasks:
            if now % t.T == 0:
                jid_count[t.name] += 1
                new_job = Job(t, jid_count[t.name], now, now+t.D, t.C)
                jobs.append(new_job)
                ready.append(new_job)

        # Try to acquire the semaphore for blocked jobs, in FIFO request order.
        still_blocked = []
        for j in blocked:
            if lock_owner is None:
                lock_owner = j
                j.resource_acquired = True
                j.in_cs = True
                j.resource_wait += now - j.resource_wait_start
                j.resource_wait_start = None
                ready.append(j)
            else:
                j.blocked_ticks += 1
                still_blocked.append(j)
        blocked = still_blocked

        # FIFO scheduling is non-preemptive; RM and EDF are preemptive.
        if policy == "FIFO":
            if running is None or running.remaining <= 0:
                running = None
                if ready:
                    running = min(ready, key=lambda j: (j.release, j.task.name, j.jid))
                    ready.remove(running)
        else:
            if ready:
                if policy == "RM":
                    chosen = min(ready, key=lambda j: (j.task.T, j.release, j.task.name, j.jid))
                elif policy == "EDF":
                    chosen = min(ready, key=lambda j: (j.deadline, j.release, j.task.name, j.jid))
                else:
                    raise ValueError(policy)
                if running is not None and running.remaining > 0:
                    ready.append(running)
                running = chosen
                ready.remove(chosen)
            elif running is not None and running.remaining <= 0:
                running = None

        # If a job is ready to request the semaphore, it blocks before using CPU.
        # Critical section is the final min(1,C) execution unit.
        if running is not None:
            cs_len = min(2, running.task.C)
            non_cs = running.task.C - cs_len
            if (not running.resource_requested and running.executed >= non_cs):
                running.resource_requested = True
                if lock_owner is None:
                    lock_owner = running
                    running.resource_acquired = True
                    running.in_cs = True
                    running.cs_remaining = min(2, running.task.C)
                elif lock_owner is not running:
                    running.resource_wait_start = now
                    running.blocked_ticks += 1
                    blocked.append(running)
                    running = None

        if running is None:
            gantt.append((now, now+1, "IDLE"))
            continue

        if running.first_start is None:
            running.first_start = now
        running.remaining -= 1
        running.executed += 1
        gantt.append((now, now+1, running.label))

        if running.in_cs:
            running.cs_remaining -= 1
            if running.cs_remaining <= 0:
                if lock_owner is running:
                    lock_owner = None
                running.in_cs = False
        if running.remaining == 0:
            running.completion = now + 1
            running = None

    # Jobs whose completion falls after the measured horizon are censored;
    # include only completed jobs for timing averages, but report incomplete count.
    completed = [j for j in jobs if j.completion is not None]
    def avg(xs):
        return round(sum(xs)/len(xs), 3) if xs else None
    rows = []
    for j in jobs:
        turnaround = (j.completion-j.release) if j.completion is not None else None
        waiting = (turnaround-j.task.C) if turnaround is not None else None
        response = (j.first_start-j.release) if j.first_start is not None else None
        rows.append({
            "policy": policy, "job": j.label, "task": j.task.name,
            "release": j.release, "deadline": j.deadline, "C": j.task.C,
            "first_start": j.first_start, "completion": j.completion,
            "waiting_time": waiting, "turnaround_time": turnaround,
            "response_time": response, "resource_waiting_time": j.resource_wait,
            "block_time": j.blocked_ticks,
            "deadline_missed": (j.completion is not None and j.completion > j.deadline)
        })
    summary = {
        "policy": policy, "hyperperiod": H, "jobs_released": len(jobs),
        "jobs_completed": len(completed),
        "avg_waiting_time": avg([j.completion-j.release-j.task.C for j in completed]),
        "avg_turnaround_time": avg([j.completion-j.release for j in completed]),
        "avg_response_time": avg([j.first_start-j.release for j in completed if j.first_start is not None]),
        "avg_resource_waiting_time": avg([j.resource_wait for j in completed]),
        "avg_block_time": avg([j.blocked_ticks for j in completed]),
        "deadline_misses": sum(1 for j in completed if j.completion > j.deadline),
        "idle_time": sum(1 for a,b,l in gantt if l == "IDLE"),
    }
    # Coalesce adjacent same-label slots for a readable Gantt chart.
    compressed = []
    for a,b,label in gantt:
        if compressed and compressed[-1][2] == label and compressed[-1][1] == a:
            compressed[-1] = (compressed[-1][0], b, label)
        else:
            compressed.append((a,b,label))
    return summary, rows, compressed

CASES = {
    "A": [Task("A",1,4,4), Task("B",2,5,5), Task("C",3,10,10)],
    "B": [Task("X",2,5,5), Task("Y",4,7,7)],
}

def main():
    summaries, details, gantts = [], [], []
    for case_name, tasks in CASES.items():
        for policy in ("FIFO", "RM", "EDF"):
            summary, rows, gantt = simulate(tasks, policy)
            summary["case"] = case_name
            summaries.append(summary)
            for row in rows:
                row["case"] = case_name
                details.append(row)
            for a,b,label in gantt:
                gantts.append({"case":case_name,"policy":policy,"start":a,"end":b,"slot":label})
    for name, rows in [("semaphore_summary.csv", summaries),
                       ("semaphore_job_details.csv", details),
                       ("semaphore_gantt.csv", gantts)]:
        with open(name, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader(); w.writerows(rows)
    print("case,policy,avg_wait,avg_turnaround,avg_resource_wait,avg_block,misses,idle")
    for s in summaries:
        print(f'{s["case"]},{s["policy"]},{s["avg_waiting_time"]},{s["avg_turnaround_time"]},'
              f'{s["avg_resource_waiting_time"]},{s["avg_block_time"]},{s["deadline_misses"]},{s["idle_time"]}')

if __name__ == "__main__":
    main()

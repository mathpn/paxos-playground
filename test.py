import argparse
import asyncio
import multiprocessing
import os
import random
from collections import Counter
from enum import Enum, auto
from typing import Callable, NamedTuple
from uuid import uuid4

from paxos import (
    Acceptor,
    AcceptResponse,
    Learner,
    NodeId,
    PrepareResponse,
    Proposal,
    Proposer,
    Value,
)


class Task(NamedTuple):
    msg: str
    task_fn: Callable
    fut: asyncio.Future
    response: bool


class MockAcceptorComms:
    def __init__(
        self,
        acceptor: Acceptor,
        accepted_props: dict[NodeId, Proposal],
        task_queue: asyncio.Queue[Task],
        failure_rate: float = 0.2,
    ) -> None:
        self.acc = acceptor
        self.accepted_props = accepted_props
        self.failure_rate = failure_rate
        self.task_queue = task_queue
        self.dead = False

    async def prepare(self, proposer_id: NodeId, number: int) -> PrepareResponse:
        async def prepare_task():
            if self.dead:
                msg = f"[A{self.acc._id} -> P{proposer_id}] dead comm"
                return msg, PrepareResponse(prepared=False)

            res = self.acc.receive_prepare(number)
            msg = f"[A{self.acc._id} -> P{proposer_id}] PrepareRequest({number=}) PrepareResponse({res})"
            return msg, res

        msg = f"[P{proposer_id} -> A{self.acc._id}] PrepareRequest({number=})"
        fut = asyncio.get_event_loop().create_future()
        await self.task_queue.put(Task(msg, prepare_task, fut, False))

        res = await fut
        if res is None:
            res = PrepareResponse(prepared=False)

        return res

    async def accept(self, proposer_id: NodeId, prop: Proposal) -> AcceptResponse:
        async def accept_task():
            if self.dead:
                msg = f"[A{self.acc._id} -> P{proposer_id}] dead comm"
                return msg, AcceptResponse(accepted=False)

            res = self.acc.receive_accept(prop)
            if res.accepted:
                self.accepted_props[self.acc._id] = prop

            msg = f"[A{self.acc._id} -> P{proposer_id}] {prop} AcceptResponse({res})"
            return msg, res

        msg = f"[P{proposer_id} -> A{self.acc._id}] AcceptRequest({prop})"
        fut = asyncio.get_event_loop().create_future()
        await self.task_queue.put(Task(msg, accept_task, fut, False))

        res = await fut
        if res is None:
            res = AcceptResponse(accepted=False)

        return res

    def kill(self):
        self.dead = True


class MockLearnerComms:
    def __init__(self, learner: Learner, task_queue: asyncio.Queue[Task]) -> None:
        self.learner = learner
        self.task_queue = task_queue
        self.dead = False

    async def send_accepted(self, acceptor_id: NodeId, prop: Proposal):
        async def send_accepted_task():
            if self.dead:
                msg = f"[A{acceptor_id} -> L{self.learner.id}] dead comm"
                return msg, None

            msg = f"[A{acceptor_id} -> L{self.learner.id}] ack AcceptedProposal({acceptor_id=} {prop=})"
            self.learner.receive_accepted(acceptor_id, prop)
            return msg, None

        msg = f"[A{acceptor_id} -> L{self.learner.id}] AcceptedProposal({acceptor_id=} {prop=})"
        fut = asyncio.get_event_loop().create_future()
        await self.task_queue.put(Task(msg, send_accepted_task, fut, False))

        return await fut

    def kill(self):
        self.dead = True


def get_consensus(
    accepted_values: dict[NodeId, Proposal], majority: int
) -> Value | None:
    counts = Counter(accepted_values.values())
    majority_props = [k for k, v in counts.items() if v >= majority]
    return majority_props[0].value if majority_props else None


def _response_wrapper(res):
    async def respond():
        return res

    return respond


async def consume_one_task(
    task_queue: asyncio.Queue[Task], messages: list[str]
) -> asyncio.Task | None:
    task = await task_queue.get()

    try:
        messages.append(task.msg)

        coro = task.task_fn()
        res_msg, result = await coro
        if task.response:
            if res_msg:
                messages.append(res_msg)
            task.fut.set_result(result)
        else:
            await task_queue.put(
                Task(res_msg, _response_wrapper(("", result)), task.fut, True)
            )
    except Exception as e:
        task.fut.set_exception(e)
    finally:
        task_queue.task_done()


async def shuffle_queue(queue: asyncio.Queue):
    items = []
    while not queue.empty():
        items.append(await queue.get())
        queue.task_done()

    random.shuffle(items)

    for item in items:
        await queue.put(item)


class Operation(Enum):
    PROPOSE = auto()
    EXECUTE = auto()
    DROP = auto()
    DUPLICATE = auto()
    SHUFFLE = auto()
    KILL_COMM = auto()
    RESTART_NODE = auto()


def node_state_message(proposers, acceptors):
    msg = ""
    for prop in proposers:
        state = {k: v for k, v in prop.__dict__.items() if k in prop._persisted}
        msg += f"-> P{prop._id} state: {state}\n"
    for acc in acceptors:
        state = {k: v for k, v in acc.__dict__.items() if k in acc._persisted}
        msg += f"-> A{acc._id} state: {state}\n"
    return msg.strip()


def restart_node(node):
    for attr in node._persisted:
        setattr(node, attr, None)
    node._load()


async def test_run(
    n_nodes: int,
    random_seed: int,
    n_operations: int = 1_000,
    log_state: bool = False,
) -> list[str] | None:
    majority = n_nodes // 2 + 1
    accepted_props: dict[NodeId, Proposal] = {}
    task_queue: asyncio.Queue[Task] = asyncio.Queue()
    bg_tasks: set[asyncio.Task] = set()
    messages: list[str] = []

    instance_id = uuid4().hex
    learners = [Learner(i, n_nodes) for i in range(n_nodes)]
    learner_comms = [MockLearnerComms(learner, task_queue) for learner in learners]
    acceptors = [
        Acceptor(instance_id=instance_id, acceptor_id=i, learner_comms=learner_comms)
        for i in range(n_nodes)
    ]
    acceptor_comms = [
        MockAcceptorComms(acc, accepted_props, task_queue) for acc in acceptors
    ]
    proposers = [
        Proposer(
            instance_id=instance_id,
            proposer_id=i,
            n_proposers=n_nodes,
            n_acceptors=n_nodes,
            acceptor_comms=acceptor_comms,
        )
        for i in range(n_nodes)
    ]

    def _available_op(op: Operation) -> bool:
        match op:
            case Operation.PROPOSE | Operation.RESTART_NODE:
                return True
            case (
                Operation.EXECUTE
                | Operation.SHUFFLE
                | Operation.DROP
                | Operation.DUPLICATE
            ):
                return not task_queue.empty()
            case Operation.KILL_COMM:
                return sum(comm.dead for comm in acceptor_comms) < majority

    random.seed(random_seed)
    consensus = None
    for i in range(n_operations):
        new_consensus = get_consensus(accepted_props, majority) or consensus
        if new_consensus != consensus:
            msg = f"[consensus] new consensus {new_consensus}: {accepted_props}"
            messages.append(msg)

        if new_consensus != consensus and consensus is not None:
            msg = "[ERROR] consensus has changed"
            messages.append(msg)
            messages.append(f"SEED={random_seed}")
            return messages

        consensus = new_consensus
        learner_consensus = [learner.get_value() for learner in learners]
        if any(lc is not None and lc != consensus for lc in learner_consensus):
            msg = (
                "[ERROR] learners consensus differ from true consensus: "
                f"{learner_consensus = } "
                f"{new_consensus = }"
            )
            messages.append(msg)
            messages.append(f"SEED={random_seed}")
            return messages

        weights = [1, 5, 1, 1, 1, 0.01, 0.01]
        _available_ops = [
            (op, weights[i]) for i, op in enumerate(Operation) if _available_op(op)
        ]
        action, _ = random.choices(
            _available_ops, weights=[op[1] for op in _available_ops], k=1
        )[0]
        match action:
            case Operation.PROPOSE:
                node = random.choice(range(n_nodes))
                value = f"prop_{i}"
                msg = f"[prop {node}] proposing value {value}"
                messages.append(msg)
                bg_task = asyncio.create_task(proposers[node].propose(value))
                bg_tasks.add(bg_task)
                bg_task.add_done_callback(bg_tasks.discard)
            case Operation.DROP:
                msg, *_ = await task_queue.get()
                messages.append(f"{msg} -> DROPPED")
            case Operation.EXECUTE:
                if log_state:
                    state_msg = node_state_message(proposers, acceptors)
                    messages.append(state_msg)
                await consume_one_task(task_queue, messages)
            case Operation.DUPLICATE:
                task = await task_queue.get()
                await task_queue.put(task)
                fut_ = asyncio.get_event_loop().create_future()
                await task_queue.put(Task(task.msg, task.task_fn, fut_, task.response))
            case Operation.SHUFFLE:
                await shuffle_queue(task_queue)
            case Operation.KILL_COMM:
                node = random.choice(range(n_nodes))
                msg = f"[kill comm] killing communations with node {node}"
                messages.append(msg)
                for comm in (acceptor_comms[node], learner_comms[node]):
                    comm.kill()
            case Operation.RESTART_NODE:
                node = random.choice(range(n_nodes))
                messages.append(f"[restart] node {node}")
                restart_node(proposers[node])
                restart_node(acceptors[node])

        await asyncio.sleep(1e-10)

    for task in bg_tasks:
        task.cancel()


def wrapper_func(args):
    return asyncio.run(test_run(*args))


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--seed", type=int)
    parser.add_argument("--n-nodes", type=int, default=3)
    parser.add_argument("--n-simulations", type=int, default=10_000)
    parser.add_argument("--n-operations", type=int, default=1_000)
    parser.add_argument("--log-state", action="store_true")
    parser.add_argument("--cpu", type=int, default=(os.cpu_count() or 2) - 1)
    args = parser.parse_args()

    if args.seed:
        messages = await test_run(args.n_nodes, args.seed, log_state=args.log_state)
        if messages:
            print("\n".join(messages))
        return

    seeds = random.sample(range(1_000_000), args.n_simulations)
    args_list = [
        (args.n_nodes, seed, args.n_operations, args.log_state) for seed in seeds
    ]
    with multiprocessing.Pool(args.cpu) as p:
        all_messages = p.map(wrapper_func, args_list)

    for msgs in all_messages:
        if msgs:
            print("\n".join(msgs))


if __name__ == "__main__":
    asyncio.run(main())

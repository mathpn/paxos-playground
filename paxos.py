import argparse
import asyncio
import json
import multiprocessing
import os
import random
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, is_dataclass
from enum import Enum, auto
from typing import Any, Protocol, Sequence
from uuid import uuid4

PERSIST_DIR = "/tmp/paxos-persist"

Value = str


@dataclass
class Proposal:
    number: int
    value: Value


@dataclass
class PrepareResponse:
    prepared: bool
    last_proposal: Proposal | None = None


@dataclass
class AcceptResponse:
    accepted: bool


class AcceptorCommunication(Protocol):
    async def prepare(self, proposer_id: int, number: int) -> PrepareResponse: ...

    async def accept(self, proposer_id: int, prop: Proposal) -> AcceptResponse: ...


class LearnerCommunication(Protocol):
    async def send_accepted(self, acceptor_id: int, value: Value) -> None: ...


def persist(id_: str, d: dict[str, Any]):
    os.makedirs(PERSIST_DIR, exist_ok=True)
    tmp_fpath = f"{PERSIST_DIR}/.tmp_{id_}.json"
    fpath = f"{PERSIST_DIR}/{id_}.json"
    with open(tmp_fpath, "w", encoding="utf-8") as f:
        f.write(json.dumps(d))
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_fpath, fpath)
    # TODO fsync no diretorio


def load(id_: str) -> dict | None:
    os.makedirs(PERSIST_DIR, exist_ok=True)
    try:
        with open(f"{PERSIST_DIR}/{id_}.json", "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return None


class Proposer:
    _persisted = {"_n", "_id", "_n_proposers"}

    def __init__(
        self,
        instance_id: str,
        proposer_id: int,
        n_proposers: int,
        acceptor_comms: Sequence[AcceptorCommunication],
    ) -> None:
        self.id = proposer_id
        self._n = -1
        self._n_proposers = n_proposers
        self._acceptor_comms = acceptor_comms
        self._majority = len(acceptor_comms) // 2 + 1
        self._persist_id = f"prop_{instance_id}_{self.id}"
        self._load()

    def _load(self) -> None:
        previous_state = load(self._persist_id)
        if previous_state:
            self.__dict__ = {**self.__dict__, **previous_state}

    def _persist(self) -> None:
        state = {k: v for k, v in self.__dict__.items() if k in self._persisted}
        persist(self._persist_id, state)

    def _increment(self) -> None:
        self._n += 1
        self._persist()

    def _proposal_number(self) -> int:
        return self.id + self._n * self._n_proposers

    async def propose(self, value: Value) -> tuple[bool, Value | None]:
        self._increment()
        number = self._proposal_number()
        prepared, prop = await self._prepare(number)
        if not prepared:
            return False, None

        value = value if prop is None else prop.value
        prop = Proposal(number=number, value=value)
        accepted = await self._request_acceptance(prop)

        return accepted, value

    async def _prepare(self, number: int) -> tuple[bool, Proposal | None]:
        responses = await asyncio.gather(
            *[comm.prepare(self.id, number) for comm in self._acceptor_comms]
        )
        prepared_res = [r for r in responses if r.prepared]

        if len(prepared_res) < self._majority:
            return False, None

        proposals = [
            r.last_proposal for r in prepared_res if r.last_proposal is not None
        ]
        highest_proposal = None
        if proposals:
            highest_proposal = max(proposals, key=lambda x: x.number)

        return True, highest_proposal

    async def _request_acceptance(self, prop: Proposal) -> bool:
        responses = await asyncio.gather(
            *[comm.accept(self.id, prop) for comm in self._acceptor_comms]
        )
        accepted = [r for r in responses if r.accepted]
        return len(accepted) >= self._majority


class Acceptor:
    _persisted = {"_highest_promise", "_last_proposal"}

    def __init__(
        self,
        instance_id: str,
        acceptor_id: int,
        learner_comms: Sequence[LearnerCommunication],
    ) -> None:
        self.id = acceptor_id
        self._highest_promise = 0
        self._last_proposal: Proposal | None = None
        self._learner_comms = learner_comms
        self._persist_id = f"acc_{instance_id}_{self.id}"
        self._load()
        self._tasks = set()

    def _load(self) -> None:
        previous_state = load(self._persist_id)
        if previous_state:
            if previous_state["_last_proposal"] is not None:
                previous_state["_last_proposal"] = Proposal(
                    **previous_state["_last_proposal"]
                )
            self.__dict__ = {**self.__dict__, **previous_state}

    def _persist(self) -> None:
        state = {}
        for k, v in self.__dict__.items():
            if k not in self._persisted:
                continue
            if is_dataclass(v) and not isinstance(v, type):
                v = asdict(v)
            state[k] = v

        persist(self._persist_id, state)

    def receive_prepare(self, number: int) -> PrepareResponse:
        if number > self._highest_promise:
            self._highest_promise = number
            self._persist()
            return PrepareResponse(prepared=True, last_proposal=self._last_proposal)

        return PrepareResponse(prepared=False)

    async def receive_accept(self, prop: Proposal) -> AcceptResponse:
        if prop.number < self._highest_promise:
            return AcceptResponse(accepted=False)

        self._last_proposal = prop
        self._persist()

        for comm in self._learner_comms:
            task = asyncio.create_task(comm.send_accepted(self.id, prop.value))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)

        return AcceptResponse(accepted=True)


class Learner:
    def __init__(self, learner_id: int, n_acceptors: int) -> None:
        self.id = learner_id
        self._accepted: defaultdict[Value, set[int]] = defaultdict(set)
        self._n_acceptors = n_acceptors
        self._majority = n_acceptors // 2 + 1

    def receive_accepted(self, acceptor_id: int, value: Value) -> None:
        self._accepted[value].add(acceptor_id)

    def get_value(self) -> Value | None:
        if not self._accepted:
            return None

        items = ((k, len(v)) for k, v in self._accepted.items())
        sorted_items = list(sorted(items, key=lambda item: item[1], reverse=True))
        value, count = sorted_items[0]
        if count >= self._majority:
            return value

        return None


class MockAcceptorComms:
    def __init__(
        self,
        acceptor: Acceptor,
        accepted_props: dict[int, Proposal],
        task_queue: asyncio.Queue,
        failure_rate: float = 0.2,
    ) -> None:
        self.acc = acceptor
        self.accepted_props = accepted_props
        self.failure_rate = failure_rate
        self.task_queue = task_queue
        self.dead = False

    async def prepare(self, proposer_id: int, number: int) -> PrepareResponse:
        async def prepare_task():
            if self.dead:
                msg = f"[A{self.acc.id} -> P{proposer_id}] dead comm"
                return msg, PrepareResponse(prepared=False)

            res = self.acc.receive_prepare(number)
            msg = f"[A{self.acc.id} -> P{proposer_id}] PrepareRequest({number=}) PrepareResponse({res}) | highest_promise={self.acc._highest_promise}"
            return msg, res

        msg = f"[P{proposer_id} -> A{self.acc.id}] PrepareRequest({number=})"
        fut = asyncio.get_event_loop().create_future()
        await self.task_queue.put((msg, prepare_task, fut))

        res = await fut
        if res is None:
            res = PrepareResponse(prepared=False)

        return res

    async def accept(self, proposer_id: int, prop: Proposal) -> AcceptResponse:
        async def accept_task():
            if self.dead:
                msg = f"[A{self.acc.id} -> P{proposer_id}] dead comm"
                return msg, AcceptResponse(accepted=False)

            res = await self.acc.receive_accept(prop)
            if res.accepted:
                self.accepted_props[self.acc.id] = prop

            msg = (
                f"[A{self.acc.id} -> P{proposer_id}] {prop} AcceptResponse({res}) | highest_promise={self.acc._highest_promise}\n"
                f"[accepted proposals] {self.accepted_props}"
            )
            return msg, res

        msg = f"[P{proposer_id} -> A{self.acc.id}] AcceptRequest({prop})"
        fut = asyncio.get_event_loop().create_future()
        await self.task_queue.put((msg, accept_task, fut))

        res = await fut
        if res is None:
            res = AcceptResponse(accepted=False)

        return res

    def kill(self):
        self.dead = True


class MockLearnerComms:
    def __init__(self, learner: Learner, task_queue: asyncio.Queue) -> None:
        self.learner = learner
        self.task_queue = task_queue
        self.dead = False

    async def send_accepted(self, acceptor_id: int, value: Value):
        async def send_accepted_task():
            if self.dead:
                msg = f"[A{acceptor_id} -> L{self.learner.id}] dead comm"
                return msg, None

            msg = f"[A{acceptor_id} -> L{self.learner.id}] ack AcceptevValue({acceptor_id=} {value=})"
            self.learner.receive_accepted(acceptor_id, value)
            return msg, None

        msg = f"[A{acceptor_id} -> L{self.learner.id}] AcceptedValue({acceptor_id=} {value=})"
        fut = asyncio.get_event_loop().create_future()
        await self.task_queue.put((msg, send_accepted_task, fut))

        return await fut

    def kill(self):
        self.dead = True


def get_consensus(accepted_values: dict[int, Proposal], majority: int) -> Value | None:
    counts = Counter(v.value for v in accepted_values.values())
    majority_values = [k for k, v in counts.items() if v >= majority]
    return majority_values[0] if majority_values else None


# TODO new task before setting future result
async def consume_one_task(
    task_queue: asyncio.Queue, messages: list[str], response: bool = True
) -> asyncio.Task | None:
    msg, task_fn, fut = await task_queue.get()

    try:
        messages.append(msg)
        coro = task_fn()
        msg, result = await coro
        if response:
            messages.append(msg)
            fut.set_result(result)
        else:
            messages.append(f"{msg} -> DROPPED")
            fut.set_result(None)
    except Exception as e:
        fut.set_exception(e)
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
    EXECUTE_NO_RESPONSE = auto()
    DROP = auto()
    DUPLICATE = auto()
    SHUFFLE = auto()
    KILL_COMM = auto()
    # TODO restart node with persistence


async def test_run(
    n_nodes: int, random_seed: int, n_operations: int = 1_000
) -> list[str] | None:
    majority = n_nodes // 2 + 1
    accepted_props: dict[int, Proposal] = {}
    task_queue = asyncio.Queue()
    bg_tasks: set[asyncio.Task] = set()
    messages: list[str] = []

    instance_id = uuid4().hex
    learners = [Learner(i, n_nodes) for i in range(n_nodes)]
    learner_comms = [MockLearnerComms(learner, task_queue) for learner in learners]
    acceptors = [
        Acceptor(instance_id, i, learner_comms=learner_comms) for i in range(n_nodes)
    ]
    acceptor_comms = [
        MockAcceptorComms(acc, accepted_props, task_queue) for acc in acceptors
    ]
    proposers = [
        Proposer(instance_id, i, n_nodes, acceptor_comms) for i in range(n_nodes)
    ]

    def _available_op(op: Operation) -> bool:
        match op:
            case Operation.PROPOSE:
                return True
            case (
                Operation.EXECUTE
                | Operation.EXECUTE_NO_RESPONSE
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
        new_consensus = get_consensus(accepted_props, majority)
        if new_consensus != consensus:
            msg = f"[consensus] new consensus {new_consensus}: {accepted_props}"
            messages.append(msg)

        learner_consensus = [learner.get_value() for learner in learners]
        if any(lc is not None and lc != new_consensus for lc in learner_consensus):
            msg = (
                "[ERROR] learners consensus differ from true consensus: "
                f"{learner_consensus = } "
                f"{new_consensus = }"
            )
            messages.append(msg)
            messages.append(f"SEED={random_seed}")
            return messages

        if consensus is not None and consensus != new_consensus:
            msg = "[ERROR] consensus has changed"
            messages.append(msg)
            messages.append(f"SEED={random_seed}")
            return messages

        consensus = new_consensus

        # XXX 0
        weights = [0.2, 0.5, 0.1, 0.1, 0.1, 0.1, 0]
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
                await consume_one_task(task_queue, messages)
            case Operation.EXECUTE_NO_RESPONSE:
                await consume_one_task(task_queue, messages, response=False)
            case Operation.DUPLICATE:
                msg, task_fn, fut = await task_queue.get()
                await task_queue.put((msg, task_fn, fut))
                fut_ = asyncio.get_event_loop().create_future()
                await task_queue.put((msg, task_fn, fut_))
            case Operation.SHUFFLE:
                await shuffle_queue(task_queue)
            case Operation.KILL_COMM:
                node = random.choice(range(n_nodes))
                for comm in [acceptor_comms[node], learner_comms[node]]:
                    comm.kill()
                await shuffle_queue(task_queue)

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
    parser.add_argument("--cpu", type=int, default=(os.cpu_count() or 2) - 1)
    args = parser.parse_args()

    if args.seed:
        messages = await test_run(args.n_nodes, args.seed)
        if messages:
            print("\n".join(messages))
        return

    seeds = random.sample(range(1_000_000), args.n_simulations)
    args_list = [(args.n_nodes, seed, args.n_operations) for seed in seeds]
    with multiprocessing.Pool(args.cpu) as p:
        all_messages = p.map(wrapper_func, args_list)

    for msgs in all_messages:
        if msgs:
            print("\n".join(msgs))


if __name__ == "__main__":
    asyncio.run(main())

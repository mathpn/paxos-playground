import asyncio
import json
import os
import random
from collections import Counter, defaultdict
from typing import Any, Protocol, Sequence

from pydantic import BaseModel

PERSIST_DIR = "./persist"

Value = str


class Proposal(BaseModel):
    number: int
    value: Value


class PrepareResponse(BaseModel):
    prepared: bool
    last_proposal: Proposal | None = None


class AcceptResponse(BaseModel):
    accepted: bool


class AcceptorCommunication(Protocol):
    async def prepare(self, number: int) -> PrepareResponse: ...

    async def accept(self, prop: Proposal) -> AcceptResponse: ...


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
        proposer_id: int,
        n_proposers: int,
        acceptor_comms: Sequence[AcceptorCommunication],
    ) -> None:
        self._n = -1
        self._id = proposer_id
        self._n_proposers = n_proposers
        self._acceptor_comms = acceptor_comms
        self._majority = len(acceptor_comms) // 2 + 1
        self._persist_id = f"prop_{self._id}"
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
        return self._id + self._n * self._n_proposers

    async def propose(self, value: Value) -> tuple[bool, Value | None]:
        self._increment()
        prepared, prop = await self._prepare()
        if not prepared:
            return False, None

        value = value if prop is None else prop.value
        accepted = await self._request_acceptance(value)
        return accepted, value

    async def _prepare(self) -> tuple[bool, Proposal | None]:
        print(
            f"[prop {self._id}] requesting prepare for number {self._proposal_number()}"
        )
        responses = await asyncio.gather(
            *[comm.prepare(self._proposal_number()) for comm in self._acceptor_comms]
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

    async def _request_acceptance(self, value: Value) -> bool:
        prop = Proposal(number=self._proposal_number(), value=value)
        print(f"[prop {self._id}] requesting acceptance for proposal {prop}")
        responses = await asyncio.gather(
            *[comm.accept(prop) for comm in self._acceptor_comms]
        )
        accepted = [r for r in responses if r.accepted]
        return len(accepted) >= self._majority


class Acceptor:
    _persisted = {"_highest_promise", "_last_proposal"}

    def __init__(
        self, acceptor_id: int, learner_comms: Sequence[LearnerCommunication]
    ) -> None:
        self._highest_promise = 0
        self._last_proposal: Proposal | None = None
        self._id = acceptor_id
        self._learner_comms = learner_comms
        self._persist_id = f"acc_{self._id}"
        self._load()
        self._tasks = set()

    def _load(self) -> None:
        previous_state = load(self._persist_id)
        if previous_state:
            if previous_state["_last_proposal"] is not None:
                previous_state["_last_proposal"] = Proposal.model_validate(
                    previous_state["_last_proposal"]
                )
            self.__dict__ = {**self.__dict__, **previous_state}

    def _persist(self) -> None:
        state = {}
        for k, v in self.__dict__.items():
            if k not in self._persisted:
                continue
            if isinstance(v, BaseModel):
                v = v.model_dump()
            state[k] = v

        persist(self._persist_id, state)

    def receive_prepare(self, number: int) -> PrepareResponse:
        print(f"[acc {self._id}] received prepare request for number {number}")
        if number > self._highest_promise and (
            self._last_proposal is None or number != self._last_proposal.number
        ):
            print(f"[acc {self._id}] promised to number {number}")
            self._highest_promise = number
            self._persist()
            return PrepareResponse(prepared=True, last_proposal=self._last_proposal)

        return PrepareResponse(prepared=False)

    async def receive_accept(self, prop: Proposal) -> AcceptResponse:
        if prop.number < self._highest_promise:
            return AcceptResponse(accepted=False)

        self._last_proposal = prop
        self._persist()
        print(f"[acc {self._id}] accepted proposal {prop}")

        for comm in self._learner_comms:
            task = asyncio.create_task(comm.send_accepted(self._id, prop.value))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)

        return AcceptResponse(accepted=True)


class Learner:
    def __init__(self, learner_id: int, n_acceptors: int) -> None:
        self._id = learner_id
        self._accepted: defaultdict[Value, set[int]] = defaultdict(set)
        self._n_acceptors = n_acceptors
        self._majority = n_acceptors // 2 + 1

    def receive_accepted(self, acceptor_id: int, value: Value) -> None:
        print(
            f"[learner {self._id}] received accepted value {value} from {acceptor_id}"
        )
        self._accepted[value].add(acceptor_id)

    def get_value(self) -> Value | None:
        print(f"[learner {self._id}] finding accepted value")
        if not self._accepted:
            return None

        items = ((k, len(v)) for k, v in self._accepted.items())
        sorted_items = list(sorted(items, key=lambda item: item[1], reverse=True))
        value, count = sorted_items[0]
        if count >= self._majority:
            return value

        return None


class ImperfectAcceptorComms:
    def __init__(
        self,
        acceptor: Acceptor,
        accepted_values: dict[int, Value],
        task_queue: asyncio.Queue,
        failure_rate: float = 0.2,
    ) -> None:
        self.acc = acceptor
        self.accepted_values = accepted_values
        self.failure_rate = failure_rate
        self.task_queue = task_queue

    async def prepare(self, number: int) -> PrepareResponse:
        async def prepare_task():
            return self.acc.receive_prepare(number)

        fut = asyncio.get_event_loop().create_future()
        await self.task_queue.put((False, prepare_task(), fut))

        return await fut

    async def accept(self, prop: Proposal) -> AcceptResponse:
        async def accept_task():
            accepted = await self.acc.receive_accept(prop)
            if accepted:
                self.accepted_values[self.acc._id] = prop.value
            return accepted

        fut = asyncio.get_event_loop().create_future()
        await self.task_queue.put((False, accept_task(), fut))

        return await fut


class ImperfectLearnerComms:
    def __init__(self, learner: Learner, task_queue: asyncio.Queue) -> None:
        self.learner = learner
        self.task_queue = task_queue

    async def send_accepted(self, acceptor_id: int, value: Value):
        async def send_accepted_task():
            self.learner.receive_accepted(acceptor_id, value)

        fut = asyncio.get_event_loop().create_future()
        await self.task_queue.put((False, send_accepted_task(), fut))

        return await fut


async def await_coro(coro, fut):
    result = await coro
    fut.set_result(result)


def get_consensus(accepted_values: dict[int, Value], majority: int) -> Value | None:
    counts = Counter(accepted_values.values())
    majority_values = [k for k, v in counts.items() if v >= majority]
    return majority_values[0] if majority_values else None


async def process_task_queue(
    task_queue: asyncio.Queue, accepted_values: dict[int, Value], majority: int
):
    tasks = set()
    consensus = None

    while True:
        new_consensus = get_consensus(accepted_values, majority)
        if consensus is not None:
            assert consensus == new_consensus, "consensus changed"

        consensus = new_consensus
        try:
            proposer, coro, fut = task_queue.get_nowait()
        except asyncio.QueueEmpty:
            await asyncio.sleep(1e-4)
            continue

        try:
            if proposer:
                bg_task = asyncio.create_task(await_coro(coro, fut))
                tasks.add(bg_task)
                bg_task.add_done_callback(tasks.discard)
            else:
                await await_coro(coro, fut)

        except Exception as e:
            fut.set_exception(e)
        finally:
            task_queue.task_done()


async def main():
    n_nodes = 3
    majority = n_nodes // 2 + 1
    accepted_values = {}
    task_queue = asyncio.Queue()

    learners = [Learner(i, n_nodes) for i in range(n_nodes)]
    learner_comms = [ImperfectLearnerComms(learner, task_queue) for learner in learners]
    acceptors = [Acceptor(i, learner_comms=learner_comms) for i in range(n_nodes)]
    acceptor_comms = [
        ImperfectAcceptorComms(acc, accepted_values, task_queue) for acc in acceptors
    ]
    proposers = [Proposer(i, n_nodes, acceptor_comms) for i in range(n_nodes)]

    consumer_task = asyncio.create_task(
        process_task_queue(task_queue, accepted_values, majority)
    )

    fut = asyncio.get_event_loop().create_future()
    await task_queue.put((True, proposers[0].propose("foo"), fut))
    fut = asyncio.get_event_loop().create_future()
    await task_queue.put((True, proposers[1].propose("bar"), fut))
    fut = asyncio.get_event_loop().create_future()
    await task_queue.put((True, proposers[2].propose("ping"), fut))

    await consumer_task
    print(accepted_values)


if __name__ == "__main__":
    asyncio.run(main())

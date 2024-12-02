import asyncio
from collections import defaultdict
import json
import os
import random
from typing import Protocol, Sequence

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


def persist(id_: str, d):
    os.makedirs(PERSIST_DIR, exist_ok=True)
    tmp_fpath = f"{PERSIST_DIR}/.tmp_{id_}.json"
    fpath = f"{PERSIST_DIR}/{id_}.json"
    with open(tmp_fpath, "w", encoding="utf-8") as f:
        f.write(json.dumps(d))
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_fpath, fpath)


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

    def _load(self):
        previous_state = load(self._persist_id)
        if previous_state:
            self.__dict__ = {**self.__dict__, **previous_state}

    def _persist(self):
        state = {k: v for k, v in self.__dict__.items() if k in self._persisted}
        persist(self._persist_id, state)

    def _increment(self):
        self._n += 1
        self._persist()

    def _proposal_number(self):
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
        print(f"requesting prepare for number {self._proposal_number()}")
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

    async def _request_acceptance(self, value: Value):
        prop = Proposal(number=self._proposal_number(), value=value)
        print(f"requesting acceptance for proposal {prop}")
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

    def _load(self):
        previous_state = load(self._persist_id)
        if previous_state:
            if previous_state["_last_proposal"] is not None:
                previous_state["_last_proposal"] = Proposal.model_validate(
                    previous_state["_last_proposal"]
                )
            self.__dict__ = {**self.__dict__, **previous_state}

    def _persist(self):
        state = {}
        for k, v in self.__dict__.items():
            if k not in self._persisted:
                continue
            if isinstance(v, BaseModel):
                v = v.model_dump()
            state[k] = v

        persist(self._persist_id, state)

    def receive_prepare(self, number: int) -> PrepareResponse:
        print(f"received prepare request for number {number}")
        if number > self._highest_promise and (
            self._last_proposal is None or number != self._last_proposal.number
        ):
            print(f"promised to number {number}")
            self._highest_promise = number
            self._persist()
            return PrepareResponse(prepared=True, last_proposal=self._last_proposal)

        return PrepareResponse(prepared=False)

    async def receive_accept(self, prop: Proposal) -> AcceptResponse:
        if prop.number < self._highest_promise:
            return AcceptResponse(accepted=False)

        self._last_proposal = prop
        self._persist()
        print(f"accepted proposal {prop}")

        await asyncio.gather(
            *[comm.send_accepted(self._id, prop.value) for comm in self._learner_comms]
        )

        return AcceptResponse(accepted=True)


class Learner:
    def __init__(self, n_acceptors: int) -> None:
        self._accepted: defaultdict[Value, set[int]] = defaultdict(set)
        self._n_acceptors = n_acceptors
        self._majority = n_acceptors // 2 + 1

    def receive_accepted(self, acceptor_id: int, value: Value) -> None:
        print(f"received accepted value {value} from {acceptor_id}")
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


class ImperfectAcceptorComms:
    def __init__(self, acceptor: Acceptor, failure_rate: float = 0.5) -> None:
        self.acc = acceptor
        self.failure_rate = failure_rate

    async def prepare(self, number: int) -> PrepareResponse:
        latency = random.random()
        await asyncio.sleep(latency / 2)

        if random.random() < self.failure_rate:
            return PrepareResponse(prepared=False)

        return self.acc.receive_prepare(number)

    async def accept(self, prop: Proposal) -> AcceptResponse:
        latency = random.random()
        await asyncio.sleep(latency / 2)

        if random.random() < self.failure_rate:
            return AcceptResponse(accepted=False)

        return await self.acc.receive_accept(prop)


class ImperfectLearnerComms:
    def __init__(self, learner: Learner, failure_rate: float = 0.5) -> None:
        self.learner = learner
        self.failure_rate = failure_rate

    async def send_accepted(self, acceptor_id: int, value: Value):
        latency = random.random()
        await asyncio.sleep(latency / 2)

        if random.random() < self.failure_rate:
            return

        self.learner.receive_accepted(acceptor_id, value)


async def main():
    learners = [Learner(3) for _ in range(3)]
    learner_comms = [ImperfectLearnerComms(learner) for learner in learners]
    acceptors = [Acceptor(i, learner_comms=learner_comms) for i in range(3)]
    prop = Proposer(
        proposer_id=0,
        n_proposers=3,
        acceptor_comms=[ImperfectAcceptorComms(acc) for acc in acceptors],
    )

    while True:
        accepted, acc_value = await prop.propose("foo")
        if accepted:
            break

        await asyncio.sleep(0.1)

    print(learners[0].get_value())


if __name__ == "__main__":
    asyncio.run(main())

import asyncio
import json
import os
from collections import defaultdict
from dataclasses import asdict, dataclass, is_dataclass
from typing import Any, Protocol, Sequence

PERSIST_DIR = "/tmp/paxos-persist"

type NodeId = int
type Value = str


@dataclass
class Proposal:
    number: int
    value: Value

    def __hash__(self) -> int:
        return hash((self.number, self.value))


@dataclass
class PrepareResponse:
    prepared: bool
    last_proposal: Proposal | None = None


@dataclass
class AcceptResponse:
    accepted: bool


class AcceptorCommunication(Protocol):
    async def prepare(self, proposer_id: NodeId, number: int) -> PrepareResponse: ...

    async def accept(self, proposer_id: NodeId, prop: Proposal) -> AcceptResponse: ...


class LearnerCommunication(Protocol):
    async def send_accepted(self, acceptor_id: NodeId, prop: Proposal) -> None: ...


def persist(id_: str, d: dict[str, Any]):
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
        instance_id: str,
        proposer_id: NodeId,
        n_proposers: int,
        n_acceptors: int,
        acceptor_comms: Sequence[AcceptorCommunication],
    ) -> None:
        self._id = proposer_id
        self._n = -1
        self._n_proposers = n_proposers
        self._acceptor_comms = acceptor_comms
        self._majority = n_acceptors // 2 + 1
        self._persist_id = f"prop_{instance_id}_{self._id}"
        self._load()
        self._persist()

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
            *[comm.prepare(self._id, number) for comm in self._acceptor_comms]
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
            *[comm.accept(self._id, prop) for comm in self._acceptor_comms]
        )
        accepted = [r for r in responses if r.accepted]
        return len(accepted) >= self._majority


class Acceptor:
    _persisted = {"_highest_promise", "_last_proposal"}

    def __init__(
        self,
        instance_id: str,
        acceptor_id: NodeId,
        learner_comms: Sequence[LearnerCommunication],
    ) -> None:
        self._id = acceptor_id
        self._highest_promise = 0
        self._last_proposal: Proposal | None = None
        self._learner_comms = learner_comms
        self._persist_id = f"acc_{instance_id}_{self._id}"
        self._load()
        self._persist()
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
            task = asyncio.create_task(comm.send_accepted(self._id, prop))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)

        return AcceptResponse(accepted=True)


class Learner:
    def __init__(self, learner_id: NodeId, n_acceptors: int) -> None:
        self.id = learner_id
        self._accepted: defaultdict[Proposal, set[NodeId]] = defaultdict(set)
        self._consensus: Proposal | None = None
        self._n_acceptors = n_acceptors
        self._majority = n_acceptors // 2 + 1

    def receive_accepted(self, acceptor_id: NodeId, prop: Proposal) -> None:
        self._accepted[prop].add(acceptor_id)
        self._find_consensus()

    def _find_consensus(self) -> None:
        items = ((k, len(v)) for k, v in self._accepted.items())
        sorted_items = list(sorted(items, key=lambda item: item[1], reverse=True))
        prop, count = sorted_items[0]
        if count >= self._majority:
            self._consensus = prop

    def get_value(self) -> Value | None:
        return self._consensus.value if self._consensus else None

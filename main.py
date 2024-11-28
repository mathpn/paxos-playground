import random
import asyncio

from typing import Protocol, NamedTuple

Value = str


class Proposal(NamedTuple):
    number: int
    value: Value


class PrepareResponse(NamedTuple):
    prepared: bool
    last_proposal: Proposal | None


class AcceptResponse(NamedTuple):
    accepted: bool


class AcceptorCommunication(Protocol):
    async def prepare(self, number: int) -> PrepareResponse: ...

    async def accept(self, prop: Proposal) -> AcceptResponse: ...


class Proposer:
    def __init__(
        self,
        acceptor_comms: list[AcceptorCommunication],
    ) -> None:
        self._n = 0
        self._acceptor_comms = acceptor_comms
        self._majority = len(acceptor_comms) // 2 + 1

    async def propose(self, value):
        while True:
            accepted, value = await self._propose(value)
            if accepted:
                return value

            await asyncio.sleep(0.1)

    async def _propose(self, value) -> tuple[bool, Value | None]:
        self._n += 1
        prepared, prop = await self._prepare()
        if not prepared:
            return False, None

        value = value if prop is None else prop.value
        return await self._request_acceptance(value), value

    async def _prepare(self) -> tuple[bool, Proposal | None]:
        responses = await asyncio.gather(
            *[comm.prepare(self._n) for comm in self._acceptor_comms]
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

    async def _request_acceptance(self, value):
        prop = Proposal(self._n, value)
        responses = await asyncio.gather(
            *[comm.accept(prop) for comm in self._acceptor_comms]
        )
        accepted = [r for r in responses if r.accepted]
        return len(accepted) >= self._majority


class Acceptor:
    def __init__(self) -> None:
        self._highest_promise = 0
        self._last_proposal: Proposal | None = None

    def receive_prepare(self, number: int) -> PrepareResponse:
        print(f"received proposal with number {number}")
        if number > self._highest_promise:
            self._highest_promise = number
            return PrepareResponse(True, self._last_proposal)

        return PrepareResponse(False, None)

    def receive_accept(self, prop: Proposal) -> AcceptResponse:
        if prop.number < self._highest_promise:
            return AcceptResponse(False)

        self._last_proposal = prop
        print(f"accepted proposal {prop}")
        return AcceptResponse(True)


class ImperfectAcceptorComms:
    def __init__(self, acceptor: Acceptor) -> None:
        self.acc = acceptor

    async def prepare(self, number: int) -> PrepareResponse:
        latency = random.random()
        await asyncio.sleep(latency / 2)

        fail = random.random()
        if fail < 0.1:
            return PrepareResponse(False, None)

        return self.acc.receive_prepare(number)

    async def accept(self, prop: Proposal) -> AcceptResponse:
        latency = random.random()
        await asyncio.sleep(latency / 2)

        fail = random.random()
        if fail < 0.5:
            return AcceptResponse(False)

        return self.acc.receive_accept(prop)


async def main():
    prop = Proposer([ImperfectAcceptorComms(Acceptor()) for _ in range(3)])
    value = await prop.propose("foo")
    print(value)


if __name__ == "__main__":
    asyncio.run(main())

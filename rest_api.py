import asyncio
import logging
import os
import random
from contextlib import asynccontextmanager

import aiohttp
from fastapi import FastAPI

from main import (
    Acceptor,
    AcceptResponse,
    Learner,
    PrepareResponse,
    Proposal,
    Proposer,
    Value,
)

FAILURE_RATE = 0.1


class RESTAcceptorComms:
    def __init__(self, acceptor_address: str, timeout: float = 3) -> None:
        self._acceptor_address = acceptor_address
        self._timeout = aiohttp.ClientTimeout(total=timeout)

    async def prepare(self, number: int) -> PrepareResponse:
        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(
                    f"{self._acceptor_address}/prepare",
                    params={"number": number},
                    timeout=self._timeout,
                ) as res:
                    if res.status != 200:
                        return PrepareResponse(prepared=False)

                    return PrepareResponse.model_validate(await res.json())

            except asyncio.TimeoutError:
                return PrepareResponse(prepared=False)

    async def accept(self, prop: Proposal) -> AcceptResponse:
        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(
                    f"{self._acceptor_address}/accept",
                    json=prop.model_dump(),
                    timeout=self._timeout,
                ) as res:
                    if res.status != 200:
                        return AcceptResponse(accepted=False)

                    return AcceptResponse.model_validate(await res.json())

            except asyncio.TimeoutError:
                return AcceptResponse(accepted=False)


class ImperfectAcceptorComms:
    def __init__(
        self,
        web_api_adapter: RESTAcceptorComms,
        tasks: set[asyncio.Task],
        failure_rate: float = FAILURE_RATE,
    ) -> None:
        self._adapter = web_api_adapter
        self.failure_rate = failure_rate
        self.tasks = tasks

    async def _delayed_call(self, callback, *args, **kwargs):
        lat = random.random()
        await asyncio.sleep(lat)
        return await callback(*args, **kwargs)

    async def prepare(self, number: int) -> PrepareResponse:
        lat = random.random()
        await asyncio.sleep(lat)

        if random.random() < self.failure_rate:
            return PrepareResponse(prepared=False)

        task = asyncio.create_task(self._delayed_call(self._adapter.prepare, number))
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)

        if random.random() < self.failure_rate:
            return PrepareResponse(prepared=False)

        return await task

    async def accept(self, prop: Proposal) -> AcceptResponse:
        lat = random.random()
        await asyncio.sleep(lat)

        if random.random() < self.failure_rate:
            return AcceptResponse(accepted=False)

        task = asyncio.create_task(self._delayed_call(self._adapter.accept, prop))
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)

        if random.random() < self.failure_rate:
            return AcceptResponse(accepted=False)

        return await task


class RESTLearnerComms:
    def __init__(self, learner_address: str, timeout: float = 1) -> None:
        self._learner_address = learner_address
        self._timeout = aiohttp.ClientTimeout(total=timeout)

    async def send_accepted(self, acceptor_id: int, value: Value) -> None:
        async with aiohttp.ClientSession() as session:
            try:
                await session.post(
                    f"{self._learner_address}/receive_accepted",
                    params={"acceptor_id": acceptor_id, "value": value},
                    timeout=self._timeout,
                )

            except asyncio.TimeoutError:
                pass

        return


class ImperfectLearnerComms:
    def __init__(
        self,
        rest_adapter: RESTLearnerComms,
        tasks: set[asyncio.Task],
        failure_rate: float = FAILURE_RATE,
    ) -> None:
        self._adapter = rest_adapter
        self.failure_rate = failure_rate
        self.tasks = tasks

    async def _delayed_call(self, callback, *args, **kwargs):
        lat = random.random()
        await asyncio.sleep(lat)
        return await callback(*args, **kwargs)

    async def send_accepted(self, acceptor_id: int, value: Value) -> None:
        lat = random.random()
        await asyncio.sleep(lat)

        if random.random() < self.failure_rate:
            return

        task = asyncio.create_task(
            self._delayed_call(self._adapter.send_accepted, acceptor_id, value)
        )
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)

        if random.random() < self.failure_rate:
            return

        await task


def reset_paxos(app: FastAPI):
    service_addresses = os.getenv("SERVICE_ADDRESSES", "").split(";")

    instance_id = int(os.environ["INSTANCE_ID"])
    n_instances = int(os.environ["N_INSTANCES"])
    app.state.tasks = set()
    app.state.learner = Learner(instance_id, n_instances)
    app.state.acceptor = Acceptor(
        acceptor_id=instance_id,
        learner_comms=[
            ImperfectLearnerComms(RESTLearnerComms(address), app.state.tasks)
            for address in service_addresses
        ],
    )
    app.state.proposer = Proposer(
        proposer_id=instance_id,
        n_proposers=n_instances,
        acceptor_comms=[
            ImperfectAcceptorComms(RESTAcceptorComms(address), app.state.tasks)
            for address in service_addresses
        ],
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    reset_paxos(app)
    yield


uvicorn_logger = logging.getLogger("uvicorn.access")
uvicorn_logger.setLevel(logging.ERROR)
app = FastAPI(lifespan=lifespan)


@app.post("/propose")
async def propose(value: Value):
    while True:
        accepted, accepted_value = await app.state.proposer.propose(value)
        if accepted:
            return accepted_value

        await asyncio.sleep(0.1)


@app.get("/accepted_value")
def get_accepted_value():
    return app.state.learner.get_value()


@app.post("/receive_accepted")
def receive_accepted(acceptor_id: int, value: Value):
    return app.state.learner.receive_accepted(acceptor_id, value)


@app.post("/prepare")
def prepare(number: int):
    return app.state.acceptor.receive_prepare(number)


@app.post("/accept")
async def accept(prop: Proposal):
    return await app.state.acceptor.receive_accept(prop)

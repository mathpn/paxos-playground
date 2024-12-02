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
    PrepareResponse,
    Proposal,
    Proposer,
    Value,
)


class WebAPIAdapter:
    def __init__(self, acceptor_address: str, timeout: float = 1) -> None:
        self._acceptor_address = acceptor_address
        self._timeout = aiohttp.ClientTimeout(total=timeout)

    async def prepare(self, number: int) -> PrepareResponse:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{self._acceptor_address}/prepare",
                params={"number": number},
                timeout=self._timeout,
            ) as res:
                if res.status != 200:
                    return PrepareResponse(prepared=False)

                return PrepareResponse.model_validate(await res.json())

    async def accept(self, prop: Proposal) -> AcceptResponse:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{self._acceptor_address}/accept",
                json=prop.model_dump(),
                timeout=self._timeout,
            ) as res:
                if res.status != 200:
                    return AcceptResponse(accepted=False)

                return AcceptResponse.model_validate(await res.json())


class ImperfectNetworkAdapter:
    def __init__(
        self,
        web_api_adapter: WebAPIAdapter,
        tasks: set[asyncio.Task],
        failure_rate: float = 0.3,
    ) -> None:
        self._adapter = web_api_adapter
        self.failure_rate = failure_rate
        self.tasks = tasks

    async def gather_tasks(self):
        print(self.tasks)
        await asyncio.gather(*self.tasks)

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


def reset_paxos(app: FastAPI):
    service_addresses = os.getenv("SERVICE_ADDRESSES", "").split(";")

    instance_id = int(os.environ["INSTANCE_ID"])
    app.state.tasks = set()
    app.state.acceptor = Acceptor(instance_id)
    app.state.proposer = Proposer(
        proposer_id=instance_id,
        n_proposers=int(os.environ["N_PROPOSERS"]),
        acceptor_comms=[
            ImperfectNetworkAdapter(WebAPIAdapter(address), app.state.tasks)
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
    accepted_value = await app.state.proposer.propose(value)
    return accepted_value


@app.post("/prepare")
def prepare(number: int):
    return app.state.acceptor.receive_prepare(number)


@app.post("/accept")
def accept(prop: Proposal):
    return app.state.acceptor.receive_accept(prop)

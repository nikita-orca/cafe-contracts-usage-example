import asyncio
from asyncio.events import AbstractEventLoop
from typing import Generator

import faust
import pytest
from orca.contracts.mocker_client.v1.mocker_client import (ContractMock,
                                                           MockerClient)

from main import (ORDER_INPUT_CONTRACT_ID,
                  ORDER_INSTRUCTIONS_OUTPUT_CONTRACT_ID, app)

mocker_client = MockerClient(address="localhost", port=8001)


@pytest.fixture()
def order_input_contract_mock() -> Generator[ContractMock, None, None]:
    mock = ContractMock(
        contract=ORDER_INPUT_CONTRACT_ID,
        client=mocker_client,
    )

    mock.create_channel_if_not_exists()
    yield mock
    mock.delete_channel_if_exists()


@pytest.fixture()
def order_instructions_output_mock() -> Generator[ContractMock, None, None]:
    mock = ContractMock(
        contract=ORDER_INSTRUCTIONS_OUTPUT_CONTRACT_ID,
        client=mocker_client,
    )
    mock.create_channel_if_not_exists()
    yield mock
    mock.delete_channel_if_exists()


@pytest.mark.asyncio()
@pytest.fixture()
def event_loop() -> Generator[AbstractEventLoop, None, None]:
    """necessary for testing Faust agents"""
    yield app.loop


@pytest.mark.asyncio()
async def test_contract_coffee_order_service(
    order_input_contract_mock: ContractMock,
    order_instructions_output_mock: ContractMock,
) -> None:
    """
    1. Produce messages to input topic using contract mock
    2. Spin up service and start processing
    3. Use output contract mock to verify messages are valid
    """
    # 1.
    await order_input_contract_mock.async_produce()
    # 2.
    worker = faust.Worker(app, loglevel="INFO")
    try:
        await asyncio.wait_for(worker.start(), 10)
    except asyncio.exceptions.TimeoutError:
        await worker.stop()
    # 3.
    await order_instructions_output_mock.async_consume(number_of_expected_messages=3)


@pytest.mark.asyncio()
async def test_contract_coffee_order_service_only_vip(
    order_input_contract_mock: ContractMock,
    order_instructions_output_mock: ContractMock,
) -> None:
    await order_input_contract_mock.async_produce(included_tags={"is_vip": True})
    worker = faust.Worker(app, loglevel="INFO")
    try:
        await asyncio.wait_for(worker.start(), 10)
    except asyncio.exceptions.TimeoutError:
        await worker.stop()
    await order_instructions_output_mock.async_consume(number_of_expected_messages=1)


@pytest.mark.asyncio()
async def test_contract_coffee_order_service_only_latte(
    order_input_contract_mock: ContractMock,
    order_instructions_output_mock: ContractMock,
) -> None:
    await order_input_contract_mock.async_produce(included_labels={"latte"})
    worker = faust.Worker(app, loglevel="INFO")
    try:
        await asyncio.wait_for(worker.start(), 10)
    except asyncio.exceptions.TimeoutError:
        await worker.stop()
    await order_instructions_output_mock.async_consume(number_of_expected_messages=1)

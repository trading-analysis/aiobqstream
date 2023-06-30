import proto
import pytest
from dotenv import dotenv_values

import aiobqstream


class CustomerRecord(proto.Message):
    customer_name = proto.Field(proto.STRING, number=1)
    row_num = proto.Field(proto.INT64, number=2)


@pytest.fixture
def env():
    values = dotenv_values()

    for key in ("PROJECT_ID", "DATASET_ID", "TABLE_ID"):
        assert key in values

    return values


@pytest.fixture
def project_id(env):
    return env["PROJECT_ID"]


@pytest.fixture
def dataset_id(env):
    return env["DATASET_ID"]


@pytest.fixture
def table_id(env):
    return env["TABLE_ID"]


@pytest.mark.asyncio
async def test_writer(project_id, dataset_id, table_id):
    writer = aiobqstream.StreamWrierAsyncClient()
    result = await writer.append_rows(
        project=project_id,
        dataset=dataset_id,
        table=table_id,
        protobuf=CustomerRecord,
        rows=[
            CustomerRecord(
                customer_name="Alice",
                row_num=1,
            )
        ],
    )

    for response in result:
        assert not response.row_errors


@pytest.mark.asyncio
async def test_queue_writer(project_id, dataset_id, table_id):
    writer = aiobqstream.StreamWrierAsyncClient()
    qt = aiobqstream.asyncio.QueueTask()
    qt.produce(
        writer.append_rows(
            project=project_id,
            dataset=dataset_id,
            table=table_id,
            protobuf=CustomerRecord,
            rows=[
                CustomerRecord(
                    customer_name="Bob",
                    row_num=2,
                )
            ],
        )
    )

    await qt._exit()

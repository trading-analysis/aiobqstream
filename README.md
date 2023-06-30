# aiobqstream

Python async Wrapper on Bigquery Storage Write API.

Implemented with reference to the following issues.

> https://github.com/googleapis/python-bigquery-storage/issues/398

## Usage

1. Define a message class that extends proto.
1. Create an instance of `aiobqstream.StreamWrierAsyncClient`.
1. await the `append_rows` method.

```py
import aiobqstream


class CustomerRecord(proto.Message):
    customer_name = proto.Field(proto.STRING, number=1)
    row_num = proto.Field(proto.INT64, number=2)


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
```
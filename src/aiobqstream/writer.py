import proto
from google.cloud.bigquery_storage import (
    AppendRowsRequest,
    AppendRowsResponse,
    BigQueryWriteAsyncClient,
    ProtoRows,
    ProtoSchema,
)
from google.protobuf.descriptor_pb2 import DescriptorProto


class StreamWrierAsyncClient:
    def __init__(self):
        self._client = BigQueryWriteAsyncClient()

    async def append_rows(
        self,
        project: str,
        dataset: str,
        table: str,
        protobuf: type[proto.Message],
        rows: list[proto.Message],
    ) -> list[AppendRowsResponse]:
        # Get a fully-qualified write_stream string
        write_stream = BigQueryWriteAsyncClient.write_stream_path(
            project, dataset, table, "_default"
        )

        # Create serialized proto rows
        serialized_rows = [proto.Message.serialize(row) for row in rows]

        # Create proto schema
        proto_descriptor = DescriptorProto()
        protobuf.pb().DESCRIPTOR.CopyToProto(proto_descriptor)
        proto_schema = ProtoSchema(proto_descriptor=proto_descriptor)

        # Create ProtoData message
        proto_rows = AppendRowsRequest.ProtoData(
            rows=ProtoRows(serialized_rows=serialized_rows), writer_schema=proto_schema
        )

        # Create AppendRowsRequest message
        request = AppendRowsRequest(write_stream=write_stream, proto_rows=proto_rows)
        requests = [request]

        async def request_generator():
            for request in requests:
                yield request

        # Send request
        result = await self._client.append_rows(request_generator())

        return [x async for x in result]

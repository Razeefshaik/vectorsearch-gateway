import grpc

import embed_pb2
import embed_pb2_grpc


def run():
    channel = grpc.insecure_channel("localhost:50051")
    stub = embed_pb2_grpc.EmbedServiceStub(channel)
    response = stub.Embed(embed_pb2.EmbedRequest(text="hello world"))
    print(f"vector length: {len(response.vector)}")
    print(f"first 5 values: {response.vector[:5]}")


if __name__ == "__main__":
    run()
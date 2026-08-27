import grpc
from concurrent import futures
from sentence_transformers import SentenceTransformer

import embed_pb2
import embed_pb2_grpc

import torch
torch.set_num_threads(1)

class EmbedServicer(embed_pb2_grpc.EmbedServiceServicer):
    def __init__(self):
        self.model= SentenceTransformer("all-MiniLM-L6-v2")
    
    def Embed(self, request, context):
        if not request.text or not request.text.strip():
             context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
             context.set_details("text must not be empty")
             return embed_pb2.EmbedResponse()
    
        vector = self.model.encode(request.text)
        return embed_pb2.EmbedResponse(vector=vector.tolist())


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    embed_pb2_grpc.add_EmbedServiceServicer_to_server(EmbedServicer(), server)
    server.add_insecure_port("[::]:50051")
    server.start()
    print("embed service listening on :50051")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()

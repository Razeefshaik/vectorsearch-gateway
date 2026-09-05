Here are the commands to run yourself (from repo root: s:\StudyResource\TechBoooo\Backend\B projs\vectorsearch-gateway):

1. List gRPC services on gatewayd


& "$HOME\go\bin\grpcurl.exe" -plaintext -import-path ./proto -proto gateway.proto localhost:50053 list
2. Insert via gatewayd (goes through Kafka → consumer → embed-ingest)


& "$HOME\go\bin\grpcurl.exe" -plaintext -import-path ./proto -proto gateway.proto -d '{\"key\":{\"client_id\":1,\"label\":42},\"text\":\"hello world, this is a test document\"}' localhost:50053 vectorsearch.gateway.v1.Gateway/Insert
3. Search via gatewayd (will fail at coordinator — expected)


& "$HOME\go\bin\grpcurl.exe" -plaintext -import-path ./proto -proto gateway.proto -d '{\"text\":\"hello world\",\"k\":5,\"ef\":50,\"allow_partial\":true,\"client_id\":1}' localhost:50053 vectorsearch.gateway.v1.Gateway/Search
4. embed-search directly


& "$HOME\go\bin\grpcurl.exe" -plaintext -import-path ./proto -proto embed.proto -d '{\"text\":\"hello world, this is a test sentence\"}' localhost:50051 embed.EmbedService/Embed
5. embed-ingest directly


& "$HOME\go\bin\grpcurl.exe" -plaintext -import-path ./proto -proto embed.proto -d '{\"text\":\"another test sentence\"}' localhost:50054 embed.EmbedService/Embed
6. embed-search with empty text (should error InvalidArgument)


& "$HOME\go\bin\grpcurl.exe" -plaintext -import-path ./proto -proto embed.proto -d '{\"text\":\"\"}' localhost:50051 embed.EmbedService/Embed
Note: PowerShell needs \" escaping for the JSON inside single quotes as shown above (unlike bash, which I used directly with '...'). If that's finicky, you can instead put the JSON in a file and use -d @ with -d "@" < body.json, but the escaped form above should work fine in PowerShell 5.1.

give commands to stop all
Stop the two Go processes: press Ctrl+C in the gatewayd terminal and the consumer terminal (they're foreground go run processes, so no separate command needed).

Stop the Docker containers:


docker compose down
This stops and removes the kafka-1, embed-search-1, embed-ingest-1 containers (but keeps the built images and any named volumes). If you also want to remove the images built for this project:


docker compose down --rmi local
now give all commands used from start to stop
Here's the full sequence, in order:

1. Activate venv


.\venv\Scripts\Activate.ps1
2. Start the containers (kafka, embed-search, embed-ingest)


docker compose up --build
(run in its own terminal; leave it running)

3. Start gatewayd — new terminal, from repo root


cd "s:\StudyResource\TechBoooo\Backend\B projs\vectorsearch-gateway\go"
go run .
4. Start the consumer — another new terminal


cd "s:\StudyResource\TechBoooo\Backend\B projs\vectorsearch-gateway\go"
go run ./gateway/consumer
5. Test — from a terminal at the repo root (s:\StudyResource\TechBoooo\Backend\B projs\vectorsearch-gateway)


& "$HOME\go\bin\grpcurl.exe" -plaintext -import-path ./proto -proto gateway.proto localhost:50053 list

& "$HOME\go\bin\grpcurl.exe" -plaintext -import-path ./proto -proto gateway.proto -d '{\"key\":{\"client_id\":1,\"label\":42},\"text\":\"hello world, this is a test document\"}' localhost:50053 vectorsearch.gateway.v1.Gateway/Insert

& "$HOME\go\bin\grpcurl.exe" -plaintext -import-path ./proto -proto gateway.proto -d '{\"text\":\"hello world\",\"k\":5,\"ef\":50,\"allow_partial\":true,\"client_id\":1}' localhost:50053 vectorsearch.gateway.v1.Gateway/Search

& "$HOME\go\bin\grpcurl.exe" -plaintext -import-path ./proto -proto embed.proto -d '{\"text\":\"hello world, this is a test sentence\"}' localhost:50051 embed.EmbedService/Embed

& "$HOME\go\bin\grpcurl.exe" -plaintext -import-path ./proto -proto embed.proto -d '{\"text\":\"another test sentence\"}' localhost:50054 embed.EmbedService/Embed

& "$HOME\go\bin\grpcurl.exe" -plaintext -import-path ./proto -proto embed.proto -d '{\"text\":\"\"}' localhost:50051 embed.EmbedService/Embed
6. Stop everything


# In gatewayd terminal: Ctrl+C
# In consumer terminal: Ctrl+C
# In the docker compose terminal: Ctrl+C, then:
docker compose down
One-time setup step you already did (not needed again unless grpcurl is missing):


go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest
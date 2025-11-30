# Distributed Persistent Key-Value Store (NestJS)

A **Persistent Key-Value Store** built using **NestJS**, implementing real database engine principles such as:

- Write-Ahead Log (WAL)
- SSTables
- Memtable
- Compaction
- Range queries
- Batch writes
- Replication (optional)

It is inspired by designs from **LevelDB, RocksDB, DynamoDB, and FoundationDB**, and relies **only on Node.js standard libraries** for storage.

## Features

| Capability                   | Status |
| ---------------------------- | ------ |
| Persistent key/value storage | ✅     |
| Single key PUT/GET/DELETE    | ✅     |
| Batch write support          | ✅     |
| Range query (sorted scan)    | ✅     |
| WAL-based crash recovery     | ✅     |
| SSTable file persistence     | ✅     |

## Architecture Overview

This project mimics real-world storage engines with the following internal workflow:
Client Request → Memtable (RAM) → WAL (disk write) → SSTable flush → Compaction → Read/Range Query

### Components

| Component                 | Description                                       |
| ------------------------- | ------------------------------------------------- |
| **Memtable**              | In-memory sorted key/value map for fast writes    |
| **WAL (Write-Ahead Log)** | Ensures durability before acknowledging a write   |
| **SSTable Files**         | Immutable sorted flat-files storing data on disk  |
| **Compactor Worker**      | Merges SSTables to remove duplicates & tombstones |
| **Replication Layer**     | Syncs writes to other nodes (optional)            |

## Project Structure

```sh
src/
┣ kv/
┃ ┣ controllers/
┃ ┣ dto/
┃ ┣ engine/
┃ ┃ ┣ wal.ts
┃ ┃ ┣ memtable.ts
┃ ┃ ┣ sstable.ts
┃ ┃ ┣ compactor.worker.ts
┃ ┃ ┗ bloom-filter.ts
┃ ┣ replication/
┃ ┃ ┗ sync.service.ts
┃ ┣ kv.service.ts
┃ ┗ kv.module.ts
┣ main.ts
```

## Installation & Running

```sh
git clone <repo-url>
cd moniepoint-kv-store
npm install
npm run start:dev
```

## API Endpoints

1. Create a key: POST /kv/put

```sh
curl -X POST http://localhost:3000/kv/put \
  -H "Content-Type: application/json" \
  -d '{"key":"user:1","value":"hello"}'
```

2. Read a Key: GET /kv/read/:key

```sh
curl http://localhost:3000/kv/read/user:101
```

3. Batch Insert: POST /kv/batch-put

```sh
curl -X POST http://localhost:3000/kv/batch-put \
  -H "Content-Type: application/json" \
  -d '{
        "items":[
          {"key":"order:1", "value":"pending"},
          {"key":"order:2", "value":"paid"},
          {"key":"order:3", "value":"shipped"}
        ]
      }'
```

4. Delete a key: DELETE /kv/delete/:key

```sh
curl -X DELETE http://localhost:3000/kv/delete/user:101
```

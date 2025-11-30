import { Module } from '@nestjs/common';
import { KvController } from './kv.controller';
import { EngineService } from './engine/engine.service';
import { StorageService } from './storage.service';
import { ReplicationService } from './replication/replication.service';

@Module({
  controllers: [KvController],
  providers: [StorageService, EngineService, ReplicationService],
  exports: [StorageService],
})
export class KvModule {}

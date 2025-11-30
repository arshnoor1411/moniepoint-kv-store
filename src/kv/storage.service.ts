////////////////////////////////////////////////////////////////////////////////
import { Injectable, OnModuleInit } from '@nestjs/common';
import { EngineService } from './engine/engine.service';

@Injectable()
export class StorageService implements OnModuleInit {
  constructor(private readonly engine: EngineService) {}

  async onModuleInit() {
    await this.engine.init();
  }

  async put(key: string, value: string) {
    return this.engine.put(key, value);
  }
  async read(key: string) {
    return this.engine.read(key);
  }
  async readRange(start: string, end: string) {
    return this.engine.readRange(start, end);
  }
  async batchPut(items: { key: string; value: string }[]) {
    return this.engine.batchPut(items);
  }
  async delete(key: string) {
    return this.engine.delete(key);
  }
}

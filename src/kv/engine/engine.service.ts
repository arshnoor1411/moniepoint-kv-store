import { Injectable, Logger } from '@nestjs/common';
import * as fs from 'fs';
import * as path from 'path';
import { WAL } from './wal';
import { Memtable } from './memtable';
import { SSTable } from './ss-table';
import { ReplicationService } from '../replication/replication.service';

@Injectable()
export class EngineService {
  private readonly logger = new Logger(EngineService.name);
  private dataDir = path.resolve(process.cwd(), 'data');
  private walDir = path.join(this.dataDir, 'wal');
  private sstDir = path.join(this.dataDir, 'sstables');

  private wal: WAL;
  private mem: Memtable;
  private sstables: Array<{ path: string; index: any; bloom: any }> = [];
  private flushing = false;

  private MEMTABLE_MAX_BYTES =
    Number(process.env.MEMTABLE_MAX_BYTES) || 4 * 1024 * 1024;
  private MAX_INFLIGHT_BYTES =
    Number(process.env.MAX_INFLIGHT_BYTES) || 32 * 1024 * 1024;

  constructor(private readonly repl: ReplicationService) {}

  async init() {
    if (!fs.existsSync(this.dataDir)) fs.mkdirSync(this.dataDir);
    if (!fs.existsSync(this.walDir)) fs.mkdirSync(this.walDir);
    if (!fs.existsSync(this.sstDir)) fs.mkdirSync(this.sstDir);

    this.wal = new WAL(this.walDir);
    this.mem = new Memtable(this.MEMTABLE_MAX_BYTES);

    await this.loadSSTables();
    await this.wal.replayAll(this.mem);

    // setInterval(() => this.backgroundWork(), 30000);
  }

  private async loadSSTables() {
    const files = fs.readdirSync(this.sstDir).filter((f) => f.endsWith('.sst'));
    files.sort().reverse();
    this.sstables = [];
    for (const f of files) {
      const full = path.join(this.sstDir, f);
      try {
        const meta = SSTable.readMeta(full);
        this.sstables.push({
          path: full,
          index: meta.index,
          bloom: meta.bloom,
        });
      } catch (e) {
        this.logger.warn(`Failed to read sstable meta ${full}: ${e.message}`);
      }
    }
    this.logger.log(`Loaded ${this.sstables.length} sstables.`);
  }

  //   private async backgroundWork() {
  //     try {
  //       if (this.mem.sizeBytes > 0 && !this.flushing) await this.flushMemtable();
  //       if (this.sstables.length > 4) await this.compactOnce();
  //     } catch (e) {
  //       this.logger.error(`backgroundWork error: ${e.message}`);
  //     }
  //   }

  async put(key: string, value: string) {
    if (key == null || value == null) {
      throw new Error(`Invalid KV entry: key=${key}, value=${value}`);
    }
    if (this.mem.sizeBytes + this.wal.bufferedBytes() > this.MAX_INFLIGHT_BYTES)
      throw new Error('STORE_OVERLOADED');

    const rec = { op: 'put', k: key, v: value, ts: Date.now() };
    await this.repl.maybeReplicate(rec);
    await this.wal.append(rec);
    this.mem.set(key, value, false);
    if (this.mem.sizeBytes >= this.MEMTABLE_MAX_BYTES)
      await this.flushMemtable();
  }

  async read(key: string): Promise<string | null> {
    const m = this.mem.get(key);
    console.log('M', m);
    if (m) return m.tombstone ? null : m.value;

    for (const s of this.sstables) {
      if (!s.bloom.mightContain(key)) continue;
      const res = SSTable.get(s.path, key, s.index);
      if (res) return res.tombstone ? null : res.value;
    }
    return null;
  }

  async readRange(start: string, end: string) {
    const results: Map<string, string | null> = new Map();

    for (let i = this.sstables.length - 1; i >= 0; i--) {
      const s = this.sstables[i];
      const items = SSTable.range(s.path, start, end, s.index);
      for (const it of items)
        results.set(it.key, it.tombstone ? null : it.value);
    }

    for (const it of this.mem.range(start, end))
      results.set(it.key, it.tombstone ? null : it.value);
    return Array.from(results.entries())
      .filter(([, v]) => v !== null)
      .map(([k, v]) => ({ key: k, value: v as string }))
      .sort((a, b) => a.key.localeCompare(b.key));
  }

  async batchPut(items: { key: string; value: string }[]) {
    const totalSize = items.reduce(
      (s, it) => s + Buffer.byteLength(it.key) + Buffer.byteLength(it.value),
      0,
    );
    if (
      this.mem.sizeBytes + this.wal.bufferedBytes() + totalSize >
      this.MAX_INFLIGHT_BYTES
    )
      throw new Error('STORE_OVERLOADED');

    const recs = items.map((it) => ({
      op: 'put',
      k: it.key,
      v: it.value,
      ts: Date.now(),
    }));
    await this.repl.maybeReplicateBatch(recs);
    await this.wal.appendBatch(recs);
    for (const it of items) this.mem.set(it.key, it.value, false);
    if (this.mem.sizeBytes >= this.MEMTABLE_MAX_BYTES)
      await this.flushMemtable();
  }

  async delete(key: string) {
    if (this.mem.sizeBytes + this.wal.bufferedBytes() > this.MAX_INFLIGHT_BYTES)
      throw new Error('STORE_OVERLOADED');
    const rec = { op: 'del', k: key, ts: Date.now() };
    await this.repl.maybeReplicate(rec);
    await this.wal.append(rec);
    this.mem.set(key, null, true);
    if (this.mem.sizeBytes >= this.MEMTABLE_MAX_BYTES)
      await this.flushMemtable();
  }

  private async flushMemtable() {
    if (this.flushing) return;
    this.flushing = true;
    try {
      const entries = this.mem.entriesSorted();
      if (entries.length === 0) return;
      const fname = `sst-${Date.now()}.sst`;
      const tmp = path.join(this.sstDir, `.tmp-${fname}`);
      const final = path.join(this.sstDir, fname);
      await SSTable.write(tmp, entries);
      fs.renameSync(tmp, final);

      await this.wal.rotate();
      this.mem = new Memtable(this.MEMTABLE_MAX_BYTES);

      const meta = SSTable.readMeta(final);
      this.sstables.unshift({
        path: final,
        index: meta.index,
        bloom: meta.bloom,
      });
      this.logger.log(`Flushed memtable -> ${final}`);
    } finally {
      this.flushing = false;
    }
  }

  private async compactOnce() {
    if (this.sstables.length < 2) return;
    const a = this.sstables.pop();
    const b = this.sstables.pop();
    if (!a || !b) return;
    const mergedTmp = path.join(this.sstDir, `.tmp-sst-${Date.now()}.sst`);
    const mergedFinal = path.join(this.sstDir, `sst-${Date.now()}-cmp.sst`);
    await SSTable.merge([a.path, b.path], mergedTmp);
    fs.renameSync(mergedTmp, mergedFinal);

    try {
      fs.unlinkSync(a.path);
      fs.unlinkSync(b.path);
    } catch (e) {}
    const meta = SSTable.readMeta(mergedFinal);
    this.sstables.unshift({
      path: mergedFinal,
      index: meta.index,
      bloom: meta.bloom,
    });
    this.logger.log(`Compacted into ${mergedFinal}`);
  }

  async applyReplicaRecord(obj: any) {
    await this.wal.append(obj);
    if (obj.op === 'put') this.mem.set(obj.k, obj.v, false);
    else if (obj.op === 'del') this.mem.set(obj.k, null, true);
    if (this.mem.sizeBytes >= this.MEMTABLE_MAX_BYTES)
      await this.flushMemtable();
  }

  async applyReplicaBatch(objs: any[]) {
    await this.wal.appendBatch(objs);
    for (const obj of objs) {
      if (obj.op === 'put') this.mem.set(obj.k, obj.v, false);
      else if (obj.op === 'del') this.mem.set(obj.k, null, true);
    }
    if (this.mem.sizeBytes >= this.MEMTABLE_MAX_BYTES)
      await this.flushMemtable();
  }
}

import * as fs from 'fs';
import * as path from 'path';

export class WAL {
  private dir: string;
  private currentFd: number | null = null;
  private currentPath: string | null = null;
  private buffer: string[] = [];
  private flushedBytesCount = 0;

  private BATCH_SIZE = Number(process.env.WAL_BATCH_SIZE) || 1000;
  private FLUSH_INTERVAL_MS = Number(process.env.WAL_FLUSH_INTERVAL_MS) || 50;
  private FSYNC_PER_APPEND = process.env.WAL_FSYNC_PER_APPEND === 'true';

  constructor(dir: string) {
    this.dir = dir;
    this.rotate();
    setInterval(() => this.maybeFlush(), this.FLUSH_INTERVAL_MS);
  }

  bufferedBytes() {
    return this.buffer.reduce((s, b) => s + Buffer.byteLength(b), 0);
  }

  async append(obj: any) {
    const line = JSON.stringify(obj) + '\n';
    this.buffer.push(line);
    if (this.buffer.length >= this.BATCH_SIZE) await this.flush();
    if (this.FSYNC_PER_APPEND) await this.fsyncCurrent();
  }

  async appendBatch(objs: any[]) {
    for (const o of objs) this.buffer.push(JSON.stringify(o) + '\n');
    await this.flush();
  }

  async maybeFlush() {
    if (this.buffer.length > 0) await this.flush();
  }

  async flush() {
    if (!this.currentFd) this.rotate();
    const toWrite = this.buffer.join('');
    this.buffer = [];
    fs.writeSync(this.currentFd!, toWrite);
    await this.fsyncCurrent();
    this.flushedBytesCount += Buffer.byteLength(toWrite);
  }

  async fsyncCurrent() {
    if (this.currentFd) fs.fsyncSync(this.currentFd);
  }

  async rotateAndClose() {
    if (this.currentFd) {
      fs.fsyncSync(this.currentFd);
      fs.closeSync(this.currentFd);
      this.currentFd = null;
    }
    this.rotate();
  }

  async rotate() {
    if (this.currentFd) {
      fs.fsyncSync(this.currentFd);
      fs.closeSync(this.currentFd);
    }
    const fname = `wal-${Date.now()}.wal`;
    const full = path.join(this.dir, fname);
    this.currentFd = fs.openSync(full, 'a');
    this.currentPath = full;
  }

  async replayAll(memtable: any) {
    const files = fs
      .readdirSync(this.dir)
      .filter((f) => f.endsWith('.wal'))
      .sort();
    for (const f of files) {
      const full = path.join(this.dir, f);
      const data = fs.readFileSync(full, 'utf8');
      if (!data) continue;
      const lines = data.trim().split('\n');
      for (const ln of lines) {
        if (!ln) continue;
        const obj = JSON.parse(ln);
        if (obj.op === 'put') memtable.set(obj.k, obj.v, false);
        else if (obj.op === 'del') memtable.set(obj.k, null, true);
      }
    }
  }
}

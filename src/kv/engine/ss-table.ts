import * as fs from 'fs';
import * as path from 'path';
import { BloomFilter } from './bloom';

export class SSTable {
  static async write(
    tmpPath: string,
    entries: Array<[string, any]>,
    indexInterval = 32,
  ) {
    const fd = fs.openSync(tmpPath, 'w');
    const index: Array<{ key: string; offset: number }> = [];
    let offset = 0;
    for (let i = 0; i < entries.length; i++) {
      const [k, v] = entries[i];
      if (i % indexInterval === 0) index.push({ key: k, offset });
      const line = JSON.stringify({ k, v }) + '\n';
      fs.writeSync(fd, line);
      offset += Buffer.byteLength(line);
    }
    const indexJson = JSON.stringify(index) + '\n';
    const indexOff = offset;
    fs.writeSync(fd, indexJson);
    offset += Buffer.byteLength(indexJson);

    const bloom = new BloomFilter(1024 * 8, 3);
    for (const [k] of entries) bloom.add(k);
    const bloomJson =
      JSON.stringify({ bloom: bloom.serialize(), m: 1024 * 8, k: 3 }) + '\n';
    fs.writeSync(fd, bloomJson);
    offset += Buffer.byteLength(bloomJson);

    const footer =
      JSON.stringify({ magic: 'SSTABLEv1', indexOffset: indexOff }) + '\n';
    fs.writeSync(fd, footer);
    fs.fsyncSync(fd);
    fs.closeSync(fd);
  }

  static readMeta(filePath: string) {
    const data = fs.readFileSync(filePath, 'utf8');
    const parts = data.trimEnd().split('\n');
    if (parts.length < 2) throw new Error('Invalid sstable');
    const footer = JSON.parse(parts[parts.length - 1]);
    const indexJson = parts[parts.length - 2];
    const index = JSON.parse(indexJson);
    const bloomPart = parts[parts.length - 3];
    const bloomObj = JSON.parse(bloomPart);
    const bloom = BloomFilter.deserialize(
      bloomObj.bloom,
      bloomObj.m,
      bloomObj.k,
    );
    return { index, bloom };
  }

  static get(filePath: string, key: string, index: any) {
    const fd = fs.openSync(filePath, 'r');

    let lo = 0,
      hi = index.length - 1,
      pos = 0;
    while (lo <= hi) {
      const mid = Math.floor((lo + hi) / 2);
      if (index[mid].key === key) {
        pos = index[mid].offset;
        break;
      }
      if (index[mid].key < key) {
        pos = index[mid].offset;
        lo = mid + 1;
      } else {
        hi = mid - 1;
      }
    }

    const stat = fs.fstatSync(fd);
    const toRead = Math.min(64 * 1024, stat.size - pos);
    const buf = Buffer.alloc(toRead);
    fs.readSync(fd, buf, 0, toRead, pos);
    const lines = buf.toString('utf8').split('\n');
    for (const ln of lines)
      if (ln) {
        const o = JSON.parse(ln);
        if (o.k === key) return o.v;
      }
    fs.closeSync(fd);
    return null;
  }

  static range(filePath: string, start: string, end: string, index: any) {
    const out: { key: string; value: string | null; tombstone: boolean }[] = [];
    const data = fs.readFileSync(filePath, 'utf8').trim().split('\n');
    for (const ln of data) {
      if (!ln) continue;
      const o = JSON.parse(ln);
      if (o.k >= start && o.k <= end)
        out.push({ key: o.k, value: o.v.value, tombstone: o.v.tombstone });
    }
    return out.sort((a, b) => a.key.localeCompare(b.key));
  }

  static async merge(inputs: string[], tmpOut: string) {
    const map = new Map<string, any>();
    for (const f of inputs) {
      const lines = fs.readFileSync(f, 'utf8').trim().split('\n');
      for (const ln of lines) {
        if (!ln) continue;
        const o = JSON.parse(ln);
        map.set(o.k, o.v);
      }
    }
    const entries = Array.from(map.entries()).sort((a, b) =>
      a[0].localeCompare(b[0]),
    );
    await SSTable.write(tmpOut, entries);
  }
}

export class Memtable {
  private map: Map<string, { value: string | null; tombstone: boolean }>;
  sizeBytes: number = 0;
  maxBytes: number;

  constructor(maxBytes = 4 * 1024 * 1024) {
    this.map = new Map();
    this.maxBytes = maxBytes;
  }

  set(key: string, value: string | null, tombstone = false) {
    const prev = this.map.get(key);
    if (prev)
      this.sizeBytes -=
        Buffer.byteLength(JSON.stringify(prev)) + Buffer.byteLength(key);
    const rec = { value, tombstone };
    this.map.set(key, rec);
    this.sizeBytes +=
      Buffer.byteLength(JSON.stringify(rec)) + Buffer.byteLength(key);
  }

  get(key: string) {
    return this.map.get(key) ?? null;
  }

  entriesSorted() {
    const keys = Array.from(this.map.keys()).sort();
    return keys.map((k) => [k, this.map.get(k)!] as [string, any]);
  }

  range(start: string, end: string) {
    const out: { key: string; value: string | null; tombstone: boolean }[] = [];
    for (const [k, v] of this.map.entries())
      if (k >= start && k <= end)
        out.push({ key: k, value: v.value, tombstone: v.tombstone });
    out.sort((a, b) => a.key.localeCompare(b.key));
    return out;
  }
}

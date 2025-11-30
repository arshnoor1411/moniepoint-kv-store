export class BloomFilter {
  private m: number; // bits
  private k: number; // hashes
  private bits: Uint8Array;

  constructor(m = 1024 * 8, k = 3) {
    this.m = m;
    this.k = k;
    this.bits = new Uint8Array(Math.ceil(m / 8));
  }

  private hash1(s: string) {
    let h = 2166136261;
    for (let i = 0; i < s.length; i++)
      h = Math.imul(h ^ s.charCodeAt(i), 16777619);
    return Math.abs(h);
  }
  private hash2(s: string) {
    let h = 5381;
    for (let i = 0; i < s.length; i++) h = (h << 5) + h + s.charCodeAt(i);
    return Math.abs(h);
  }

  add(s: string) {
    for (let i = 0; i < this.k; i++) {
      const pos = (this.hash1(s) + i * this.hash2(s)) % this.m;
      this.bits[Math.floor(pos / 8)] |= 1 << (pos % 8);
    }
  }
  mightContain(s: string) {
    for (let i = 0; i < this.k; i++) {
      const pos = (this.hash1(s) + i * this.hash2(s)) % this.m;
      if ((this.bits[Math.floor(pos / 8)] & (1 << (pos % 8))) === 0)
        return false;
    }
    return true;
  }
  serialize() {
    return Buffer.from(this.bits).toString('base64');
  }
  static deserialize(b64: string, m = 1024 * 8, k = 3) {
    const buf = Buffer.from(b64, 'base64');
    const bf = new BloomFilter(m, k);
    bf.bits = Uint8Array.from(buf);
    return bf;
  }
}

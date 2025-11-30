import { Injectable, Logger } from '@nestjs/common';
import * as http from 'http';

@Injectable()
export class ReplicationService {
  private readonly logger = new Logger(ReplicationService.name);
  private peers: string[] = (process.env.REPLICATION_PEERS || '')
    .split(',')
    .filter(Boolean);
  private ackMode: 'ASYNC' | 'MAJORITY' = (process.env.REPLICATION_ACK ||
    'ASYNC') as any;

  async maybeReplicate(rec: any) {
    if (this.peers.length === 0) return;
    // fire-and-forget for ASYNC
    if (this.ackMode === 'ASYNC') {
      for (const p of this.peers) this.sendToPeer(p, '/replicate/append', rec);
      return;
    }
    // MAJORITY: wait for majority acks
    const promises = this.peers.map((p) =>
      this.sendToPeer(p, '/replicate/append', rec),
    );
    const results = await Promise.allSettled(promises);
    const success = results.filter((r) => r.status === 'fulfilled').length + 1; // +1 leader
    const need = Math.floor(this.peers.length / 2) + 1;
    if (success < need) this.logger.warn('Replication majority not reached');
  }

  async maybeReplicateBatch(recs: any[]) {
    if (this.peers.length === 0) return;
    if (this.ackMode === 'ASYNC') {
      for (const p of this.peers)
        this.sendToPeer(p, '/replicate/appendBatch', { recs });
      return;
    }
    const promises = this.peers.map((p) =>
      this.sendToPeer(p, '/replicate/appendBatch', { recs }),
    );
    await Promise.allSettled(promises);
  }

  private sendToPeer(peer: string, path: string, body: any) {
    return new Promise((resolve, reject) => {
      try {
        const u = new URL(peer + path);
        const data = JSON.stringify(body);
        const opts = {
          hostname: u.hostname,
          port: u.port,
          path: u.pathname,
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Content-Length': Buffer.byteLength(data),
          },
        };
        const req = http.request(opts, (res) => {
          res.on('data', () => {});
          res.on('end', () => resolve(true));
        });
        req.on('error', (err) => reject(err));
        req.write(data);
        req.end();
      } catch (e) {
        reject(e);
      }
    });
  }
}

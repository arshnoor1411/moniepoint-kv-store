import { Controller, Post, Body, Logger } from '@nestjs/common';
import { EngineService } from '../engine/engine.service';

@Controller('replicate')
export class ReplicationController {
  private readonly logger = new Logger(ReplicationController.name);
  constructor(private readonly engine: EngineService) {}

  @Post('append')
  async append(@Body() body) {
    // body is a single WAL record
    await this.engine.applyReplicaRecord(body);
    return { status: 'ok' };
  }

  @Post('appendBatch')
  async appendBatch(@Body() body) {
    const recs = body.recs || [];
    await this.engine.applyReplicaBatch(recs);
    return { status: 'ok' };
  }
}

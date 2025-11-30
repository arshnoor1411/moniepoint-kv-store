import {
  Controller,
  Post,
  Body,
  Get,
  Param,
  Query,
  Delete,
  Res,
} from '@nestjs/common';
import { StorageService } from './storage.service';
import { PutDto } from './dto/put.dto';
import { BatchPutDto } from './dto/batch-put.dto';

@Controller('kv')
export class KvController {
  constructor(private readonly storage: StorageService) {}

  @Post('put')
  async put(@Body() body: PutDto, @Res() res) {
    try {
      await this.storage.put(body.key, body.value);
      return res.status(200).json({ status: 'ok' });
    } catch (e) {
      if (e.message === 'STORE_OVERLOADED')
        return res.status(503).json({ error: e.message });
      throw e;
    }
  }

  @Get('read/:key')
  async read(@Param('key') key: string) {
    const v = await this.storage.read(key);
    console.log(v);
    if (v === null) return { error: 'NOT_FOUND' };
    return { key, value: v };
  }

  @Get('read-range')
  async readRange(@Query('start') start: string, @Query('end') end: string) {
    const data = await this.storage.readRange(start, end);
    return { data };
  }

  @Post('batch-put')
  async batchPut(@Body() body: BatchPutDto, @Res() res) {
    try {
      await this.storage.batchPut(body.items);
      return res.status(200).json({ status: 'ok', count: body.items.length });
    } catch (e) {
      if (e.message === 'STORE_OVERLOADED')
        return res.status(503).json({ error: e.message });
      throw e;
    }
  }

  @Delete('delete/:key')
  async delete(@Param('key') key: string) {
    await this.storage.delete(key);
    return { status: 'deleted' };
  }
}

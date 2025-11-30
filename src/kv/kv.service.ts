import { Injectable } from '@nestjs/common';
import { CreateKvDto } from './dto/create-kv.dto';
import { UpdateKvDto } from './dto/update-kv.dto';

@Injectable()
export class KvService {
  create(createKvDto: CreateKvDto) {
    return 'This action adds a new kv';
  }

  findAll() {
    return `This action returns all kv`;
  }

  findOne(id: number) {
    return `This action returns a #${id} kv`;
  }

  update(id: number, updateKvDto: UpdateKvDto) {
    return `This action updates a #${id} kv`;
  }

  remove(id: number) {
    return `This action removes a #${id} kv`;
  }
}

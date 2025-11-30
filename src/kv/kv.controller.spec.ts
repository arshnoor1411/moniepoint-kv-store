import { Test, TestingModule } from '@nestjs/testing';
import { KvController } from './kv.controller';
import { KvService } from './kv.service';

describe('KvController', () => {
  let controller: KvController;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      controllers: [KvController],
      providers: [KvService],
    }).compile();

    controller = module.get<KvController>(KvController);
  });

  it('should be defined', () => {
    expect(controller).toBeDefined();
  });
});

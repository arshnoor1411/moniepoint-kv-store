import { Test, TestingModule } from '@nestjs/testing';
import { KvService } from './kv.service';

describe('KvService', () => {
  let service: KvService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [KvService],
    }).compile();

    service = module.get<KvService>(KvService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });
});

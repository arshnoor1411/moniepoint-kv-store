import { PartialType } from '@nestjs/mapped-types';
import { CreateKvDto } from './create-kv.dto';

export class UpdateKvDto extends PartialType(CreateKvDto) {}

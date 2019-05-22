import { DatabaseBaseClient } from 'objio-object/base/database/database';
import { DatabaseBase } from '../base/database';

export class Database extends DatabaseBaseClient {
  static TYPE_ID = DatabaseBase.TYPE_ID;
}

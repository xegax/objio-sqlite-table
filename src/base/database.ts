import { DatabaseBase as Base } from 'objio-object/base/database/database';

export abstract class DatabaseBase extends Base {
  static TYPE_ID = 'SQLITE3-DATABASE';
}

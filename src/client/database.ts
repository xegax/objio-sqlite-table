import { SERIALIZER } from 'objio';
import { Database as Base } from 'objio-object/client/database';

export class Database extends Base {
  static TYPE_ID = 'SQLite3Database';
  static SERIALIZE: SERIALIZER = () => ({
  })
}

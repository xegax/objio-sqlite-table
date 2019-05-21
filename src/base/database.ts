import { DatabaseBase2 } from 'objio-object/base/database/database-holder';
import { DatabaseServerBase as ServerBase, GuidMapData } from 'objio-object/base/database/database-server';

export { GuidMapData };

export abstract class DatabaseBase extends DatabaseBase2 {
  static TYPE_ID = 'SQLT3Database';
}

export abstract class DatabaseServerBase extends ServerBase {
  static TYPE_ID = 'SQLT3Database';
}

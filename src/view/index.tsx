import { DatabaseHolder } from 'objio-object/client/database/database-holder';
import { Database } from '../client/database';
import 'ts-react-ui/typings';
import { ObjectToCreate }  from 'objio-object/common/interfaces';
import { OBJIOItemClassViewable } from 'objio-object/view/config';

export function getObjectsToCreate(): Array<ObjectToCreate> {
  return [
    {
      name: 'SQLite3',
      desc: 'SQLite3 database',
      icon: 'database-icon',
      create: () => new DatabaseHolder({ impl: new Database() })
    }
  ];
}

export function getViews(): Array<OBJIOItemClassViewable> {
  return [];
}

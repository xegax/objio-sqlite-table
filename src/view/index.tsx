import * as React from 'react';
import { OBJIOItemClassViewable, registerViews } from 'objio-object/view/config';
import { DatabaseHolder } from 'objio-object/client/database/database-holder';
import { Database2 } from '../client/database2';
import { Database, DatabaseView, Props } from './database-view';
import { Icon } from 'ts-react-ui/icon';
import 'ts-react-ui/typings';
import * as sqliteIcon from '../images/sqlite-icon.png';
import { ObjectToCreate }  from 'objio-object/common/interfaces';

export function getObjectsToCreate(): Array<ObjectToCreate> {
  return [
    {
      name: 'sqlite3',
      desc: 'sqlite3 database',
      create: () => new DatabaseHolder({ impl: new Database2() })
    }
  ];
}

export function getViews(): Array<OBJIOItemClassViewable> {
  registerViews({
    classObj: Database,
    icons: { item: <Icon src={sqliteIcon}/> },
    views: [{
      view: (props: Props) => <DatabaseView {...props}/>
    }],
    flags: [ 'create-wizard' ],
    desc: 'SQLITE3 Database'
  });

  return [
    Database
  ];
}

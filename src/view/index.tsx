import * as React from 'react';
import { OBJIOItemClassViewable, registerViews } from 'objio-object/view/config';
import { Database, DatabaseView, Props } from './database-view';
import { Icon } from 'ts-react-ui/icon';

export function getViews(): Array<OBJIOItemClassViewable> {
  registerViews({
    classObj: Database,
    icons: { item: <Icon src='sqlite-db.png'/> },
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

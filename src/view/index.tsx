import * as React from 'react';
import { OBJIOItemClassViewable, registerViews } from 'objio-object/view/config';
import { Database, DatabaseView, Props } from './database-view';

export function getViews(): Array<OBJIOItemClassViewable> {
  registerViews({
    classObj: Database,
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

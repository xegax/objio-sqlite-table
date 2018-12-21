import * as React from 'react';
import { OBJIOItemClassViewable, registerViews } from 'objio-object/view/config';
import { Database, DatabaseView, Props } from './database-view';
import { Icon } from 'ts-react-ui/icon';
import 'ts-react-ui/typings';
import * as sqliteIcon from '../images/sqlite-icon.png';

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

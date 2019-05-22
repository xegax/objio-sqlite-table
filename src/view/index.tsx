import * as React from 'react';
import { DatabaseHolder } from 'objio-object/client/database/database-holder';
import { Database } from '../client/database';
import { Icon } from 'ts-react-ui/icon';
import 'ts-react-ui/typings';
import * as SQLITEIcon from '../images/sqlite.svg';
import { ObjectToCreate }  from 'objio-object/common/interfaces';
import { OBJIOItemClassViewable } from 'objio-object/view/config';

export function getObjectsToCreate(): Array<ObjectToCreate> {
  return [
    {
      name: 'sqlite3',
      desc: 'sqlite3 database',
      icon: <Icon src={SQLITEIcon}/>,
      create: () => new DatabaseHolder({ impl: new Database() })
    }
  ];
}

export function getViews(): Array<OBJIOItemClassViewable> {
  return [];
}

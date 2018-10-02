import * as React from 'react';
import { Database } from '../client/database';

export { Database };

export interface Props {
  model: Database;
}

export class DatabaseView extends React.Component<Props> {
  render() {
    return (
      <div>SQLITE3 database</div>
    );
  }
}

import { Database as DB } from 'sqlite3';
import { Columns, createTable, deleteTable, insert, all } from './sqlite3';

export class SQLite {
  private db: DB;

  private constructor(db: DB) {
    this.db = db;
  }

  static open(file: string): Promise<SQLite> {
    return new Promise((resolve, reject) => {
      const db = new DB(file, err => {
        if (err)
          return reject(err);

        resolve(new SQLite(db));
      });
    });
  }

  createTable(args: { table: string, columns: Columns }) {
    return createTable(this.db, args.table, args.columns);
  }

  deleteTable(table: string) {
    return deleteTable(this.db, table);
  }

  insert(args: { table: string; values: Array<{[key: string]: string}> }) {
    return insert({
      db: this.db,
      table: args.table,
      values: args.values
    });
  }

  getRows(args: { table: string; from: number; count: number, cols?: Array<string> }) {
    let cols = args.cols ? args.cols.join(', ') : '*';
    return all(this.db, `select ${cols} from ${args.table} limit ${args.count} offset ${args.from}`);
  }

  close(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.db.close(err => {
        if (err)
          return reject(err);

        resolve();
      });
    });
  }
}

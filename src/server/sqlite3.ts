import { Database as SQLite3 } from 'sqlite3';

export interface PushRowArgs {
  columns?: Array<string>;
  values: Array<{[key: string]: string}>;
}

export interface ColumnAttr {
  name: string;
  type: string;
  notNull?: boolean;
  primary?: boolean;
  autoInc?: boolean;
  unique?: boolean;
}

type Columns = Array<ColumnAttr>;

export function sqlInt(int: number | string) {
  return +int;
}

export function sqlColumn(column: string) {
  return column;
}

export function sqlTable(table: string) {
  return table;
}

export function quoteValue(value: string | number) {
  value = ('' + value).replace(/"/g, '\\\"');
  return `"${value}"`;
}

export function srPromise(db: SQLite3, callback: (resolve, reject) => void): Promise<any> {
  return new Promise((resolve, reject) => {
    db.serialize(() => {
      callback(resolve, reject);
    });
  });
}

export function exec(db: SQLite3, sql: string): Promise<any> {
  return srPromise(db, (resolve, reject) => {
    db.exec(sql, err => {
      if (!err) {
        resolve();
      } else {
        console.log('error at', sql);
        reject(err);
      }
    });
  });
}

export function run(db: SQLite3, sql: string, params: Array<any>): Promise<any> {
  return srPromise(db, (resolve, reject) => {
    db.run(sql, params, err => {
      if (!err) {
        resolve();
      } else {
        console.log('error at', sql);
        reject(err);
      }
    });
  });
}

export function all<T = Object>(db: SQLite3, sql: string, params?: Array<any>): Promise<Array<T>> {
  return srPromise(db, (resolve, reject) => {
    db.all(sql, params || [], (err, rows: Array<T>) => {
      if (err)
        return reject(err);
      resolve(rows);
    });
  });
}

export function get<T = Object>(db: SQLite3, sql: string): Promise<T> {
  return srPromise(db, (resolve, reject) => {
    db.get(sql, (err, row: T) => {
      if (err)
        return reject(err);
      resolve(row);
    });
  });
}

export function createTable(db: SQLite3, table: string, columns: Columns): Promise<any> {
  const sql = columns.map(column => {
    let value = `${column.name} ${column.type}`;
    if (column.notNull)
      value += ' NOT NULL';
    if (column.primary)
      value += ' PRIMARY KEY';
    if (column.autoInc)
      value += ' AUTOINCREMENT';
    if (column.unique)
      value += ' UNIQUE';
    return value;
  }).join(', ');
  return exec(db, `create table ${table} (${sql})`);
}

export function deleteTable(db: SQLite3, table: string): Promise<void> {
  return exec(db, `drop table if exists ${table}`);
}

export function deleteData(args: {db: SQLite3, table: string, where?: string}): Promise<void> {
  let where = args.where || '';
  if (where)
    where = `where ${where}`;
  const sql = `delete from ${args.table} ${where}`;
  
  console.log(sql);
  return exec(args.db, sql);
}

export function loadTableInfo(db: SQLite3, table: string): Promise<Columns> {
  return (
    all<ColumnAttr>(db, `pragma table_info(${table})`)
    .then(res => {
      return res.map(row => ({name: row['name'], type: row['type']}));
    })
  );
}

export function loadTableList(db: SQLite3): Promise<Array<string>> {
  return (
    all(db, `select * from sqlite_master where type = 'table'`)
    .then(res => {
      return res.map(row => row['name']);
    })
  );
}

export function loadRowsNum(db: SQLite3, table: string): Promise<number> {
  return (
    get<{count: number}>(db, `select count(*) as count from ${table}`)
    .then(res => {
      return +res.count;
    })
  );
}

export function insert(args: PushRowArgs & { table: string; db: SQLite3 }): Promise<any> {
  const cols: {[name: string]: number} = {};
  const valuesArr = Array<string>();
  const holderArr = Array<string>();
  const values = args.values;
  if (!args.columns) {
    for (let n = 0; n < values.length; n++) {
      const keys = Object.keys(values[n]);
      for (let c = 0; c < keys.length; c++) {
        cols[ keys[c] ] = ( cols[ keys[c] ] || 0 ) + 1;
      }
    }
  }

  const colsArr = args.columns || Object.keys(cols);
  for (let n = 0; n < values.length; n++) {
    for (let c = 0; c < colsArr.length; c++) {
      valuesArr.push(values[n][ colsArr[c] ] as string || null);
    }
    holderArr.push( '(' + colsArr.map(() => '?').join(',') + ')' );
  }

  const allCols = colsArr.join(',');
  const sql = `insert into ${args.table}(${allCols}) values ${holderArr.join(',')};`;
  return run(args.db, sql, valuesArr);
}

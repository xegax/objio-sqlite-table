import { Database as SQLite3 } from 'sqlite3';
import {
  ColumnAttr,
  Columns,
  PushRowArgs,
  Condition,
  CompoundCond,
  ValueCond
} from 'objio-object/base/database';

export function getCompSqlCondition(cond: CompoundCond, col?: string): string {
  let sql = '';
  if (cond.values.length == 1) {
    sql = getSqlCondition(cond.values[0]);
  } else {
    sql = cond.values.map(cond => {
      return `( ${getSqlCondition(cond)} )`;
    }).join(` ${cond.op} `);
  }

  if (cond.table && col)
    sql = `select ${col} from ${cond.table} where ${sql}`;

  return sql;
}

export function getSqlCondition(cond: Condition): string {
  const comp = cond as CompoundCond;

  if (comp.op && comp.values)
    return getCompSqlCondition(comp);

  const valueCond = cond as ValueCond;

  if (Array.isArray(valueCond.value) && valueCond.value.length == 2) {
    return `${valueCond.column} >= ${valueCond.value[0]} and ${valueCond.column} <= ${valueCond.value[1]}`;
  } else if (typeof valueCond.value == 'object') {
    const val = valueCond.value as CompoundCond;
    return `${valueCond.column} in (select ${valueCond.column} from ${val.table} where ${getCompSqlCondition(val)})`;
  }

  let value = valueCond.value;
  let op: string;
  if (valueCond.like) {
    op = valueCond.inverse ? ' not like ' : ' like ';
    if (value.indexOf('%') == -1 && value.indexOf('_') == -1)
      value = '%' + value + '%';
  } else {
    op = valueCond.inverse ? '!=' : '=';
  }

  return `${valueCond.column}${op}"${value}"`;
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

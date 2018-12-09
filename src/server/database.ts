import { Database as SQLite3 } from 'sqlite3';
import { Database as Base } from '../client/database';
import {
  TableNameArgs,
  TableColsArgs,
  ColumnAttr,
  Columns,
  NumStatsArgs,
  NumStats,
  LoadCellsArgs,
  PushRowArgs,
  Cells,
  Condition,
  CompoundCond,
  ValueCond,
  SubtableAttrs,
  CreateSubtableResult
} from 'objio-object/client/table';
import { SERIALIZER } from 'objio';

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

function getSqlCondition(cond: Condition): string {
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

function srPromise(db: SQLite3, callback: (resolve, reject) => void): Promise<any> {
  return new Promise((resolve, reject) => {
    db.serialize(() => {
      callback(resolve, reject);
    });
  });
}

function exec(db: SQLite3, sql: string): Promise<any> {
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

function run(db: SQLite3, sql: string, params: Array<any>): Promise<any> {
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

function all<T = Object>(db: SQLite3, sql: string, params?: Array<any>): Promise<Array<T>> {
  return srPromise(db, (resolve, reject) => {
    db.all(sql, params || [], (err, rows: Array<T>) => {
      if (err)
        return reject(err);
      resolve(rows);
    });
  });
}

function get<T = Object>(db: SQLite3, sql: string): Promise<T> {
  return srPromise(db, (resolve, reject) => {
    db.get(sql, (err, row: T) => {
      if (err)
        return reject(err);
      resolve(row);
    });
  });
}

function createTable(db: SQLite3, table: string, columns: Columns): Promise<any> {
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

function deleteTable(db: SQLite3, table: string): Promise<void> {
  return exec(db, `drop table if exists ${table}`);
}

function loadTableInfo(db: SQLite3, table: string): Promise<Columns> {
  return all<ColumnAttr>(db, `pragma table_info(${table})`).then(res => {
    return res.map(row => ({name: row['name'], type: row['type']}));
  });
}

function insert(args: PushRowArgs & { table: string; db: SQLite3 }): Promise<any> {
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
      valuesArr.push(values[n][ colsArr[c] ] || null);
    }
    holderArr.push( '(' + colsArr.map(() => '?').join(',') + ')' );
  }

  const allCols = colsArr.join(',');
  const sql = `insert into ${args.table}(${allCols}) values ${holderArr.join(',')};`;
  return run(args.db, sql, valuesArr);
}

let subtableCounter: number = 0;
export class Database extends Base {
  private db: SQLite3;
  private subtableMap: {[key: string]: { subtable: string, columns: Array<ColumnAttr> }} = {};

  constructor() {
    super();

    this.holder.addEventHandler({
      onCreate: () => {
        console.log('sqlite3 db create');
        return this.openDB(this.getPath());
      },
      onLoad: () => {
        console.log('sqlite3 db load');
        return this.openDB(this.getPath());
      }
    });

    this.holder.setMethodsToInvoke({
      loadTableInfo:  { method: this.loadTableInfo,   rights: 'read'  },
      loadRowsCount:  { method: this.loadRowsCount,   rights: 'read'  },
      deleteTable:    { method: this.deleteTable,     rights: 'write' },
      createTable:    { method: this.createTable,     rights: 'write' },
      loadCells:      { method: this.loadCells,       rights: 'read'  },
      getNumStats:    { method: this.getNumStats,     rights: 'read'  },
      createSubtable: { method: this.createSubtable,  rights: 'read'  },
      pushCells:      { method: this.pushCells,       rights: 'read'  }
    });
  }

  loadTableInfo = (args: TableNameArgs) => {
    return loadTableInfo(this.db, args.table);
  }

  getFile(): string {
    return `db_${this.holder.getID()}.sqlite3`;
  }

  getPath() {
    return this.holder.getPrivatePath(this.getFile());
  }

  openDB(file: string): Promise<SQLite3> {
    if (this.db)
      return Promise.resolve(this.db);

    return new Promise((resolve, reject) => {
      this.db = new SQLite3(file, (err => {
        if (!err) {
          resolve(this.db);
        } else {
          reject(this.db);
        }
      }));
    });
  }

  createTable = (args: TableColsArgs): Promise<void> => {
    return createTable(this.db, args.table, args.columns);
  }

  deleteTable = (args: TableNameArgs): Promise<void> => {
    return deleteTable(this.db, args.table);
  }

  loadCells = (args: LoadCellsArgs): Promise<Cells> => {
    const { table, filter, first, count } = args;
    let where = filter ? getSqlCondition(filter) : '';
    if (where)
      where = `where ${where}`;

    const sql = `select * from ${table} ${where} limit ? offset ?`;
    return (
      all<Object>(this.db, sql, [count, first])
      .then(rows => {
        return rows.map(row => Object.keys(row).map(key => row[key]));
      })
    );
  }

  pushCells = (args: PushRowArgs & { table: string }): Promise<number> => {
    return insert({ ...args, db: this.db });
  }

  loadRowsCount = (args: TableNameArgs): Promise<number> => {
    return (
      get<{count: number}>(this.db, `select count(*) as count from ${args.table}`)
      .then(res => res.count)
    );
  }

  getNumStats = (args: NumStatsArgs): Promise<NumStats> => {
    const { table, column } = args;
    const sql = `select min(${column}) as min, max(${column}) as max from ${table} where ${column}!=""`;
    return get<NumStats>(this.db, sql);
  }

  getColumns(table: string): Promise<Columns> {
    return this.loadTableInfo({ table });
  }

  createSubtable = (args: SubtableAttrs & { table: string }): Promise<CreateSubtableResult> => {
    return (
      this.getColumns(args.table)
      .then(columns => {
        return this.createSubtableImpl({...args, columns});
      })
    );
  }

  createSubtableImpl(args: SubtableAttrs & { table: string, columns: Columns }): Promise<CreateSubtableResult> {
    let tableKey = JSON.stringify(args);
    const subtable = this.subtableMap[tableKey];
    if (subtable) {
      return this.loadRowsCount({table: subtable.subtable})
      .then(rowsNum => ({
        ...subtable,
        rowsNum
      }));
    }

    let newTable = 'tmp_table_' + subtableCounter++;
    let cols = (args.cols && args.cols.length) ? args.cols.join(', ') : '*';

    let cond: string = null;
    if (typeof args.filter == 'string')
      cond = args.filter;
    else if (args.filter)
      cond = getSqlCondition(args.filter);

    const where = cond ? ` where ${cond}` : '';
    let orderBy: string = '';
    if (args.sort && args.sort.length)
      orderBy = `order by ${args.sort[0].column} ${args.sort[0].dir}`;

    this.subtableMap[tableKey] = {
      subtable: newTable,
      columns: !args.cols || args.cols.length == 0 ? args.columns : args.columns.filter(col => {
        return args.cols.indexOf(col.name) != -1;
      })
    };

    let groupBy = '';
    if (args.distinct) {
      groupBy = `group by ${args.distinct.column}`;
      this.subtableMap[tableKey].columns = [
        { name: args.distinct.column, type: args.columns.find(c => c.name == args.distinct.column).type },
        { name: 'count', type: 'INTEGER' }
      ];
      cols = [args.distinct.column, `count(${args.distinct.column}) as count`].join(', ');
    }

    const sql = `create temp table ${newTable} as select ${cols} from ${args.table} ${where} ${groupBy} ${orderBy}`;
    console.log(sql);

    return (
      exec(this.db, sql)
      .then(() => this.loadRowsCount({table: newTable}))
      .then(rowsNum => {
        return { ...this.subtableMap[tableKey], rowsNum };
      })
    );
  }

  static SERIALIZE: SERIALIZER = () => ({
    ...Base.SERIALIZE()
  })
}

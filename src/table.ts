import {
  Table as TableBase,
  ExecuteArgs,
  Cells,
  Columns,
  ColumnAttr,
  UpdateRowArgs,
  PushRowArgs,
  RemoveRowsArgs,
  SubtableAttrs,
  LoadCellsArgs,
  Condition,
  CompoundCond,
  ValueCond,
  CreateSubtableResult
} from 'objio-object/table';
import { Database } from 'sqlite3';
import { SERIALIZER, EXTEND } from 'objio';
import { CSVReader, CSVBunch } from 'objio/server';
import { FileObject } from 'objio-object/server/file-object';

let db: Database;

function srPromise(db: Database, callback: (resolve, reject) => void): Promise<any> {
  return new Promise((resolve, reject) => {
    db.serialize(() => {
      callback(resolve, reject);
    });
  });
}

function openDB(file: string): Promise<Database> {
  if (db)
    return Promise.resolve(db);

  return new Promise((resolve, reject) => {
    db = new Database(file, (err => {
      if (!err) {
        resolve(db);
      } else {
        reject(db);
      }
    }));
  });
}

function exec(db: Database, sql: string): Promise<any> {
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

function run(db: Database, sql: string, params: Array<any>): Promise<any> {
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

function all<T = Object>(db: Database, sql: string, params?: Array<any>): Promise<Array<T>> {
  return srPromise(db, (resolve, reject) => {
    db.all(sql, params || [], (err, rows: Array<T>) => {
      if (err)
        return reject(err);
      resolve(rows);
    });
  });
}

function get<T = Object>(db: Database, sql: string): Promise<T> {
  return srPromise(db, (resolve, reject) => {
    db.get(sql, (err, row: T) => {
      if (err)
        return reject(err);
      resolve(row);
    });
  });
}

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

  const value = cond as ValueCond;

  if (typeof value.value == 'object') {
    return `${value.column} in (select ${value.column} from ${value.value.table} where ${getCompSqlCondition(value.value)})`;
  }

  const op = value.inverse ? '!=' : '=';
  return `${value.column}${op}"${value.value}"`;
}

function createTable(db: Database, table: string, columns: Columns): Promise<any> {
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

function deleteTable(db: Database, table: string): Promise<Database> {
  return exec(db, `drop table if exists ${table}`).then(() => db);
}

function loadTableInfo(db: Database, table: string): Promise<Columns> {
  return all<ColumnAttr>(db, `pragma table_info(${table})`).then(res => {
    return res.map(row => ({name: row['name'], type: row['type']}));
  });
}

function loadRowsCount(db: Database, table: string): Promise<number> {
  return get<{count: number}>(db, `select count(*) as count from ${table}`)
    .then(res => res.count);
}

function insert(db: Database, table: string, values: {[col: string]: Array<string>}): Promise<any> {
  const cols = Object.keys(values);
  const valsHolder = cols.map(() => '?').join(', ');
  const allValsHolder = values[cols[0]].map(() => `( ${valsHolder} )`).join(', ');

  const valuesArr = [];
  const rowsNum = values[cols[0]].length;
  for (let n = 0;  n < rowsNum; n++) {
    cols.forEach(col => {
      valuesArr.push(values[col][n]);
    });
  }

  const sql = `insert into ${table}(${cols.join(',')}) values ${allValsHolder};`;
  return run(db, sql, valuesArr);
}

function createIdColumn(cols: Array<ColumnAttr>, idColName?: string): ColumnAttr {
  let idCol: ColumnAttr;
  if (!idColName) { // create column
    idColName = idColName || 'row_uid';
    while (cols.find(col => col.name == idColName)) {
      idColName = 'row_uid_' + Math.round(Math.random() * 100000).toString(16);
    }
  } else {  //  find
    idCol = cols.find(col => col.name == idColName);
  }

  if (!idCol) {
    idCol = {
      name: idColName,
      type: 'INTEGER',
      autoInc: true,
      notNull: true,
      primary: true,
      unique: true
    };
    cols.splice(0, 0, idCol);
  }

  return idCol;
}

let subtableCounter: number = 0;
export class Table extends TableBase {
  private subtableMap: {[key: string]: { subtable: string, columns: Array<ColumnAttr> }} = {};

  constructor() {
    super();

    this.holder.setMethodsToInvoke({
      createSubtable: (args: SubtableAttrs) => this.createSubtable(args),
      loadCells: (args: LoadCellsArgs) => this.loadCells(args),
      pushCells: (args: PushRowArgs) => this.pushCells(args),
      updateCells: (args: UpdateRowArgs) => this.updateCells(args),
      removeRows: (args: RemoveRowsArgs) => this.removeRows(args),
      execute: (args: ExecuteArgs) => this.execute(args)
    });

    this.holder.addEventHandler({
      onLoad: () => {
        // not configured
        if (!this.table)
          return;

        return (
          this.openDB()
          .then(db => {
            return Promise.all([
              loadTableInfo(db, this.table),
              loadRowsCount(db, this.table) as any
            ]);
          })
          .then(res => {
            this.columns = res[0];
            this.totalRowsNum = res[1];
          })
        );
      }
    });
  }

  private readColumns(csv: FileObject): Promise<Array<ColumnAttr>> {
    let cols: Array<ColumnAttr>;

    const onNextBunch = (bunch: CSVBunch) => {
      cols = bunch.rows[0].map(col => ({
        name: col,
        type: 'TEXT'
      }));
      bunch.done();
    };

    return CSVReader.read({file: csv.getPath(), onNextBunch, linesPerBunch: 1}).then(() => cols);
  }

  private readRows(csv: FileObject, columns: Columns, startRow: number, flushPerRows: number): Promise<any> {
    const onNextBunch = (bunch: CSVBunch) => {
      const rows = bunch.firstLineIdx == 0 ? bunch.rows.slice(1) : bunch.rows;
      const values: {[col: string]: Array<string>} = {};

      rows.forEach(row => {
        row.forEach((v, i) => {
          v = v.trim();

          const colAttr = columns[i];
          if (colAttr.discard)
            return;

          const col = values[colAttr.name] || (values[colAttr.name] = []);
          if (colAttr.removeQuotes != false) {
            if (v.length > 1 && v[0] == '"' && v[v.length - 1] == '"')
              v = v.substr(1, v.length - 2);
          }
          col.push(v);
        });
      });

      return this.pushCells({values, updRowCounter: false}).then(() => {
        this.state.setProgress(bunch.progress);
        this.totalRowsNum += rows.length;
      });
    };

    return (
      CSVReader.read({file: csv.getPath(), onNextBunch})
    );
  }

  execute(args: ExecuteArgs): Promise<any> {
    let startTime: number;
    let columns: Columns = [];
    let readRowCols: Columns = [];
    let idCol: ColumnAttr;

    let prepCols: Promise<Array<ColumnAttr>>;
    if (args.fileObjId) {
      prepCols = this.holder.getObject<FileObject>(args.fileObjId)
      .then(csv => this.readColumns(csv))
      .then(cols => {
        if (!args.columns || args.columns.length == 0)
          return cols;

        args.columns.forEach(argsCol => {
          const i = cols.findIndex(col => col.name == argsCol.name);
          if (i == -1)
            return;
          cols[i] = { ...cols[i], ...argsCol };
        });

        return cols;
      });
    } else {
      prepCols = Promise.resolve( (args.columns || []).map(col => ({...col})) );
    }

    const task  = prepCols.then(cols => {
      // append idColumn if need
      readRowCols = cols.slice();
      columns = cols.filter(col => !col.discard);
      idCol = createIdColumn(columns, args.idColumn);
      return this.openDB();
    })
    .then(db => deleteTable(db, args.table))
    .then(db => createTable(db, args.table, columns))
    .then(() => {
      this.totalRowsNum = 0;
      this.columns = columns;
      this.table = args.table;
      this.idColumn = idCol.name;
      this.holder.save();
    });

    if (!args.fileObjId)
      return task;

    return task.then(() => {
      this.holder.getObject<FileObject>(args.fileObjId)
      .then(obj => {
        this.state.setStateType('in progress').save();
        this.holder.save();
        startTime = Date.now();
        return this.readRows(obj, readRowCols, 1, 50);
      })
      .then(() => this.openDB())
      .then(db => this.updateRowNum(db))
      .then(() => {
        this.fileObjId = args.fileObjId;
        this.lastExecuteTime = Date.now() - startTime;
        this.holder.save();
        this.state.setProgress(1);
        this.state.setStateType('valid').save();
      }).catch(err => {
        this.state.addError(err.toString()).save();
      });
    });
  }

  loadCells(args: LoadCellsArgs): Promise<Cells> {
    const table = args.table || this.table;
    let where = args.filter ? getSqlCondition(args.filter) : '';
    if (where)
      where = `where ${where}`;

    return (
      this.openDB()
      .then(db => {
        return all<Object>(db, `select * from ${table} ${where} limit ? offset ?`, [args.count, args.first]);
      }).then(rows => {
        return rows.map(row => Object.keys(row).map(key => row[key]));
      })
    );
  }

  pushCells(args: PushRowArgs): Promise<number> {
    if (!this.table)
      throw 'table not defined';

    const values = {...args.values};
    delete values[this.getIdColumn()];

    let db: Database;
    return (
      this.openDB()
      .then(dbobj => db = dbobj)
      .then(() => insert(db, this.table, values))
      .then(() => args.updRowCounter != false && this.updateRowNum(db))
    );
  }

  removeRows(args: RemoveRowsArgs): Promise<number> {
    const holders = args.rowIds.map(() => `${this.getIdColumn()} = ?`).join(' or ');
    let db: Database;
    return (
      this.openDB()
      .then(dbTmp => (db = dbTmp) && run(dbTmp, `delete from ${this.table} where ${holders}`, args.rowIds))
      .then(() => this.updateRowNum(db))
    );
  }

  createSubtable(args: SubtableAttrs): Promise<CreateSubtableResult> {
    let tableKey = JSON.stringify(args);
    const subtable = this.subtableMap[tableKey];
    if (subtable) {
      return this.openDB()
      .then(db => loadRowsCount(db, subtable.subtable))
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
      columns: !args.cols || args.cols.length == 0 ? this.columns : this.columns.filter(col => {
        return args.cols.indexOf(col.name) != -1;
      })
    };

    let groupBy = '';
    if (args.distinct) {
      groupBy = `group by ${args.distinct.column}`;
      this.subtableMap[tableKey].columns = [
        { name: args.distinct.column, type: this.columns.find(c => c.name == args.distinct.column).type },
        { name: 'count', type: 'INTEGER' }
      ];
      cols = [args.distinct.column, `count(${args.distinct.column}) as count`].join(', ');
    }

    const sql = `create temp table ${newTable} as select ${cols} from ${this.table} ${where} ${groupBy} ${orderBy}`;
    console.log(sql);
    let db: Database;
    return (
      this.openDB()
      .then(d => {
        db = d;
        return exec(db, sql);
      })
      .then(() => loadRowsCount(db, newTable))
      .then(rowsNum => {
        return { ...this.subtableMap[tableKey], rowsNum };
      })
    );
  }

  private openDB(): Promise<Database> {
    return openDB(this.holder.getDBPath());
  }

  private updateRowNum(db: Database): Promise<number> {
    return (
      loadRowsCount(db, this.table)
      .then(rows => {
        this.totalRowsNum = rows;
        this.holder.save();
        return rows;
      })
    );
  }

  updateCells(args: UpdateRowArgs): Promise<void> {
    return (
      this.openDB()
      .then(db => {
        const set = Object.keys(args.values).map(col => {
          return `${col}=?`;
        }).join(', ');
        const values = Object.keys(args.values).map(col => args.values[col]);
        return run(db, `update ${this.table} set ${set} where ${this.getIdColumn()}`, [...values, args.rowId]);
      })
    );
  }

  static TYPE_ID = 'Table';
  static SERIALIZE: SERIALIZER = () => ({
    ...TableBase.SERIALIZE(),
    ...EXTEND({
    }, { tags: ['sr'] })
  })
}

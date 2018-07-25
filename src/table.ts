import {
  Table as TableBase,
  TableArgs,
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
  ValueCond
} from 'objio-object/table';
import { Database } from 'sqlite3';
import { SERIALIZER, EXTEND, CSVReader, CSVBunch } from 'objio';
import { FileObject } from 'objio-object/server/file-object';

let db: Database;

export interface CreateSubtableArgs {
  table: string;
  attrs: SubtableAttrs;
}

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

export function getCompSqlCondition(cond: CompoundCond): string {
  return cond.values.map(cond => {
    return `( ${getSqlCondition(cond)} )`;
  }).join(` ${cond.op} `);
}

export function getSqlCondition(cond: Condition): string {
  const comp = cond as CompoundCond;

  if (comp.op && comp.values && comp.values.length)
    return getCompSqlCondition(comp);

  const value = cond as ValueCond;
  return `${value.column}="${value.value}"`;
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

export class Table extends TableBase {
  constructor() {
    super();

    this.holder.setMethodsToInvoke({
      loadCells: (args: LoadCellsArgs) => this.loadCells(args),
      pushCells: (args: PushRowArgs) => this.pushCells(args),
      updateCells: (args: UpdateRowArgs) => this.updateCells(args),
      removeRows: (args: RemoveRowsArgs) => this.removeRows(args),
      execute: (args: TableArgs) => this.execute(args)
    });

    this.holder.addEventHandler({
      onLoad: () => {
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
          const col = values[columns[i].name] || (values[columns[i].name] = []);
          col.push(v);
        });
      });

      return this.pushCells({values}).then(() => {
        this.state.setProgress(bunch.progress).save();
      });
    };

    return (
      CSVReader.read({file: csv.getPath(), onNextBunch})
    );
  }

  execute(args: TableArgs): Promise<any> {
    this.table = args.table;
    this.columns = (args.columns || []).map(column => ({...column}));

    let idCol: ColumnAttr;
    if (!args.idColumn) {
      while (idCol = this.findColumn(this.idColumn)) {
        this.idColumn = 'row_uid_' + Math.round(Math.random() * 100000).toString(16);
      }
    } else {
      this.idColumn = args.idColumn;
      idCol = this.findColumn(args.idColumn);
    }

    idCol = {
      name: !idCol ? this.idColumn : idCol.name,
      type: 'INTEGER',
      autoInc: true,
      notNull: true,
      primary: true,
      unique: true
    };

    let columns: Columns = [];
    let task: Promise<any>;
    if (args.srcId) {
      task = this.holder.getObject<FileObject>(args.srcId)
      .then(csv => this.readColumns(csv))
      .then(cols => {
        columns = cols;
        this.columns = [idCol, ...cols];
      });
    } else {
      this.columns.splice(0, 0, idCol);
      task = Promise.resolve();
    }

    return task.then(() => this.openDB())
    .then(db => createTable(db, this.table, this.columns))
    .then(() => {
      this.state.setStateType('inProgress').save();
      this.holder.save();

      if (args.srcId) {
        this.holder.getObject<FileObject>(args.srcId)
        .then((obj) => this.readRows(obj, columns, 1, 50))
        .then(() => {
          this.state.setProgress(1).setStateType('valid').save();
        });
      }
    });
  }

  loadCells(args: LoadCellsArgs): Promise<Cells> {
    const table = args.table || this.table;
    return (
      this.openDB()
      .then(db => {
        return all<Object>(db, `select * from ${table} limit ? offset ?`, [args.count, args.first]);
      }).then(rows => {
        return rows.map(row => Object.keys(row).map(key => row[key]));
      })
    );
  }

  pushCells(args: PushRowArgs): Promise<number> {
    const values = {...args.values};
    delete values[this.getIdColumn()];

    let db: Database;
    return (
      this.openDB()
      .then(dbobj => db = dbobj)
      .then(() => insert(db, this.table, values))
      .then(() => this.updateRowNum(db))
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

  createSubtable(args: CreateSubtableArgs): Promise<any> {
    const cols = (args.attrs.cols && args.attrs.cols.length) ? args.attrs.cols.join(', ') : '*';
    const cond = args.attrs.filter ? getSqlCondition(args.attrs.filter) : null;
    const where = cond ? ` where ${cond}` : '';
    let orderBy: string = '';
    if (args.attrs.sort && args.attrs.sort.length)
      orderBy = `order by ${args.attrs.sort[0].column} ${args.attrs.sort[0].dir}`;

    return (
      this.openDB().
      then(db => exec(db, `create temp table ${args.table} as select ${cols} from ${this.table} ${where} ${orderBy}`))
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

  static SERIALIZE: SERIALIZER = () => ({
    ...TableBase.SERIALIZE(),
    ...EXTEND({
    }, { tags: ['sr'] })
  })
}

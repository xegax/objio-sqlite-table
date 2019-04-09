import { Database as SQLite3 } from 'sqlite3';
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
  SubtableAttrs,
  CreateSubtableResult,
  DatabaseBase
} from 'objio-object/base/database';
import {
  loadTableInfo,
  createTable,
  deleteTable,
  getSqlCondition,
  all,
  insert,
  get,
  exec
} from './sqlite3';
import { SERIALIZER } from 'objio';

let subtableCounter: number = 0;
export class Database extends DatabaseBase {
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
      loadTableInfo: {
        method: (args: TableNameArgs) => {
          return this.loadTableInfo(args);
        },
        rights: 'read'
      },
      loadRowsCount: {
        method: (args: TableNameArgs) => {
          return this.loadRowsCount(args);
        },
        rights: 'read'
      },
      deleteTable: {
        method: (args: TableNameArgs) => {
          return this.deleteTable(args);
        },
        rights: 'write'
      },
      createTable: {
        method: (args: TableColsArgs) => {
          return this.createTable(args);
        },
        rights: 'write'
      },
      loadCells: {
        method: (args: LoadCellsArgs) => {
          return this.loadCells(args);
        },
        rights: 'read'
      },
      getNumStats: {
        method: (args: NumStatsArgs) => {
          return this.getNumStats(args);
        },
        rights: 'read'
      },
      createSubtable: {
        method: (args: SubtableAttrs & { table: string }) => {
          return this.createSubtable(args);
        },
        rights: 'read'
      },
      pushCells: {
        method: (args: PushRowArgs & {table: string}) => {
          return this.pushCells(args);
        },
        rights: 'read'
      }
    });
  }

  loadTableInfo(args: TableNameArgs) {
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

  createTable(args: TableColsArgs): Promise<void> {
    return createTable(this.db, args.table, args.columns);
  }

  deleteTable(args: TableNameArgs): Promise<void> {
    return deleteTable(this.db, args.table);
  }

  loadCells(args: LoadCellsArgs): Promise<Cells> {
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

  pushCells(args: PushRowArgs & { table: string }): Promise<number> {
    return insert({ ...args, db: this.db });
  }

  loadRowsCount(args: TableNameArgs): Promise<number> {
    return (
      get<{count: number}>(this.db, `select count(*) as count from ${args.table}`)
      .then(res => res.count)
    );
  }

  getNumStats(args: NumStatsArgs): Promise<NumStats> {
    const { table, column } = args;
    const sql = `select min(${column}) as min, max(${column}) as max from ${table} where ${column}!=""`;
    return get<NumStats>(this.db, sql);
  }

  getColumns(table: string): Promise<Columns> {
    return this.loadTableInfo({ table });
  }

  createSubtable(args: SubtableAttrs & { table: string }): Promise<CreateSubtableResult> {
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

  static TYPE_ID = 'SQLite3Database';
  static SERIALIZE: SERIALIZER = () => ({
    ...DatabaseBase.SERIALIZE()
  })
}

import {
  TableInfo,
  TableArgs,
  TmpTableArgs,
  TableData,
  TableDataArgs,
  CreateTableArgs,
  DeleteTableArgs,
  PushDataArgs,
  PushDataResult
} from 'objio-object/base/database-holder';
import { DatabaseBase } from '../base/database';
import { Database as SQLite3 } from 'sqlite3';
import {
  loadTableList,
  loadTableInfo,
  loadRowsNum,
  insert,
  all,
  exec,
  createTable,
  deleteTable
} from './sqlite3';
import { StrMap } from 'objio-object/common/interfaces';

export class Database2 extends DatabaseBase {
  private db: SQLite3;
  private tempTables: {[key: string]: { tableName: string, columns: Array<string> }} = {};
  private tmpTableCounter = 0;

  constructor() {
    super();

    this.holder.setMethodsToInvoke({
      loadTableList: {
        method: () => this.loadTableList(),
        rights: 'read'
      },
      loadTableInfo: {
        method: (args: TableArgs) => this.loadTableInfo(args),
        rights: 'read'
      },
      loadTableRowsNum: {
        method: (args: TableArgs) => this.loadTableRowsNum(args),
        rights: 'read'
      },
      loadTableData: {
        method: (args: TableDataArgs) => this.loadTableData(args),
        rights: 'read'
      },
      createTempTable: {
        method: (args: TableArgs) => this.createTempTable(args),
        rights: 'create'
      },
      createTable: {
        method: (args: CreateTableArgs) => this.createTable(args),
        rights: 'create'
      },
      deleteTable: {
        method: (args: DeleteTableArgs) => this.deleteTable(args),
        rights: 'write'
      }
    });

    this.holder.addEventHandler({
      onCreate: () => {
        return this.openDB(this.getPath());
      },
      onLoad: () => {
        return this.openDB(this.getPath());
      }
    });
  }

  loadTableList(): Promise<Array<TableInfo>> {
    return (
      this.openDB()
      .then(db => loadTableList(db))
      .then(arr => {
        return Promise.all(arr.map(table => this.loadTableInfo({ tableName: table })));
      })
    );
  }

  loadTableInfo(args: TableArgs): Promise<TableInfo> {
    return (
       Promise.all([
         loadTableInfo(this.db, args.tableName),
         loadRowsNum(this.db, args.tableName)
       ])
      .then(arr => {
        const res: TableInfo = {
          tableName: args.tableName,
          columns: arr[0].map(c => ({ colName: c.name, colType: c.type })),
          rowsNum: arr[1]
        };
        return res;
      })
    );
  }

  loadTableRowsNum(args: TableArgs): Promise<number> {
    return (
      this.openDB()
      .then(() => loadRowsNum(this.db, args.tableName))
    );
  }

  loadTableData(args: TableDataArgs): Promise<TableData> {
    const { tableName, fromRow, rowsNum } = args;
    const where = '';
    const sql = `select * from ${tableName} ${where} limit ? offset ?`;
    return (
      this.openDB()
      .then(() => all<Object>(this.db, sql, [rowsNum, fromRow]))
      .then((rows: Array<StrMap>) => {
        return {
          rows,
          fromRow,
          rowsNum
        };
      })
    );
  }

  getTempTableKey(args: TmpTableArgs): string {
    const keyObj: { n: string, cn?: number, cols?: Array<string> } = {
      n: args.tableName
    };

    if (args.columns) {
      keyObj.cn = args.columns.length;
      keyObj.cols = args.columns;
    }

    return JSON.stringify(keyObj);
  }

  createTempTable(args: TmpTableArgs): Promise<TableInfo> {
    const key = this.getTempTableKey(args);
    const table = this.tempTables[key];
    if (table)
      return this.loadTableInfo({ tableName: table.tableName });

    this.tmpTableCounter++;
    const where = '';
    const groupBy = '';
    const orderBy = '';
    const cols = args.columns ? args.columns.join(', ') : '*'; 
    const tmpTableName = `tmp_table_${this.tmpTableCounter}`;
    const sql = `create temp table ${tmpTableName} as select ${cols} from ${args.tableName} ${where} ${groupBy} ${orderBy}`;
    return (
      this.openDB()
      .then(() => exec(this.db, sql))
      .then(() => {
        this.tempTables[key] = {
          tableName: tmpTableName,
          columns: args.columns
        };

        return this.loadTableInfo({ tableName: tmpTableName });
      })
    );
  }

  createTable(args: CreateTableArgs): Promise<TableInfo> {
    return (
      this.openDB()
      .then(() => {
        return (
          createTable(
            this.db,
            args.tableName,
            args.columns.map(col => {
              return {
                name: col.colName,
                type: col.colType,
                ...col
              };
            })
          )
        );
      })
    );
  }

  deleteTable(args: DeleteTableArgs): Promise<void> {
    return (
      this.openDB()
      .then(() => {
        return deleteTable(this.db, args.tableName);
      })
    );
  }

  pushData(args: PushDataArgs): Promise<PushDataResult> {
    return (
      insert({
        db: this.db,
        table: args.tableName,
        values: args.rows
      })
      .then(pushRows => ({ pushRows }))
    );
  }

  isRemote() {
    return false;
  }

  getConnClasses() {
    return [];
  }

  getDatabaseList() {
    return Promise.reject('not implemented');
  }

  setDatabase() {
    return Promise.reject('not implemented');
  }

  setConnection() {
    return Promise.reject('not implemented');
  }

  ////////////////////////////////////////////////////////////

  getFile(): string {
    return `database-${this.holder.getID()}.sqlite3`;
  }

  getPath() {
    return this.holder.getPrivatePath(this.getFile());
  }

  private openDB(file?: string): Promise<SQLite3> {
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
}
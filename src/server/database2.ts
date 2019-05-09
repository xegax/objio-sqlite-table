import {
  TableDataArgs,
  CreateTableArgs,
  DeleteTableArgs,
  PushDataArgs,
  PushDataResult,
  DeleteDataArgs,
  CompoundCond,
  ValueCond,
  TableDesc,
  LoadTableGuidArgs,
  TableGuid,
  LoadTableDataArgs,
  LoadTableDataResult,
  LoadTableGuidResult,
  TableDescShort,
  LoadAggrDataArgs,
  LoadAggrDataResult,
  AggregationFunc,
  UpdateDataArgs
} from 'objio-object/base/database-holder-decl';
import { DatabaseBase } from '../base/database';
import { Database as SQLite3 } from 'sqlite3';
import {
  loadTableList,
  loadTableInfo,
  loadRowsNum,
  insert,
  all,
  exec,
  get,
  createTable,
  deleteTable,
  deleteData,
  quoteValue,
  sqlColumn,
  sqlTable
} from './sqlite3';
import { StrMap } from 'objio-object/common/interfaces';

export function aggConv(agg: AggregationFunc, column: string) {
  return `${agg}(${column})`;
}

export function getCompoundSQLCond(cond: CompoundCond, col?: string): string {
  let sql = '';
  if (cond.values.length == 1) {
    sql = getSQLCond(cond.values[0]);
  } else {
    sql = cond.values.map(cond => {
      return `( ${getSQLCond(cond)} )`;
    }).join(` ${cond.op} `);
  }

  if (cond.table && col)
    sql = `select ${col} from ${cond.table} where ${sql}`;

  return sql;
}

export function getSQLCond(cond: ValueCond | CompoundCond): string {
  const comp = cond as CompoundCond;

  if (comp.op && comp.values)
    return getCompoundSQLCond(comp);

  const valueCond = cond as ValueCond;

  if (Array.isArray(valueCond.value) && valueCond.value.length == 2) {
    return `${valueCond.column} >= ${valueCond.value[0]} and ${valueCond.column} <= ${valueCond.value[1]}`;
  } else if (typeof valueCond.value == 'object') {
    const val = valueCond.value as CompoundCond;
    return `${valueCond.column} in (select ${valueCond.column} from ${val.table} where ${getCompoundSQLCond(val)})`;
  }

  let value = valueCond.value;
  let op: string;
  if (valueCond.like) {
    op = valueCond.inverse ? ' not like ' : ' like ';
    if (value.indexOf('%') == -1 && value.indexOf('_') == -1)
      value = '%' + value + '%';
  } else if (value == '' || value == null) {
    op = valueCond.inverse ? 'is not' : 'is';
    return `${valueCond.column} ${op} NULL`;
  } else {
    op = valueCond.inverse ? '!=' : '=';
  }

  return `${valueCond.column}${op}"${value}"`;
}

interface GuidMapData {
  args: LoadTableGuidArgs;
  desc: TableDesc;
  tmpTable: string;
  invalid: boolean;
  createTask: Promise<TableDescShort>;
}

function getArgsKey(args: LoadTableGuidArgs): string {
  return [
    args.tableName,
    args.cond
  ].map(k => JSON.stringify(k)).join('-');
}

export class Database2 extends DatabaseBase {
  private db: SQLite3;

  private guidMap: {[guid: string]: GuidMapData} = {};
  private argsToGuid: {[argsKey: string]: string} = {};

  private tmpTableCounter = 0;
  private tableList: Array<TableDesc>;

  constructor() {
    super();

    this.holder.setMethodsToInvoke({
      loadTableList: {
        method: () => this.loadTableList(),
        rights: 'read'
      },
      loadTableGuid: {
        method: (args: LoadTableGuidArgs) => this.loadTableGuid(args),
        rights: 'read'
      },
      loadTableRowsNum: {
        method: (args: TableGuid) => this.loadTableRowsNum(args),
        rights: 'read'
      },
      loadTableData: {
        method: (args: LoadTableDataArgs) => this.loadTableData(args),
        rights: 'read'
      },
      createTable: {
        method: (args: CreateTableArgs) => this.createTable(args),
        rights: 'create'
      },
      deleteTable: {
        method: (args: DeleteTableArgs) => this.deleteTable(args),
        rights: 'write'
      },
      pushData: {
        method: (args: PushDataArgs) => this.pushData(args),
        rights: 'write'
      },
      deleteData: {
        method: (args: DeleteDataArgs) => this.deleteData(args),
        rights: 'write'
      },
      loadAggrData: {
        method: (args: LoadAggrDataArgs) => this.loadAggrData(args),
        rights: 'read'
      }
    });

    this.holder.addEventHandler({
      onCreate: () => {
        return this.openDB(this.getPath());
      },
      onLoad: () => {
        return this.loadTableList();
      }
    });
  }

  loadTableList(): Promise<Array<TableDesc>> {
    if (this.tableList)
      return Promise.resolve(this.tableList);

    return (
      this.openDB()
      .then(db => loadTableList(db))
      .then(arr => {
        return (
          Promise.all(
            arr.map(table => 
              this.loadTableGuid({ tableName: table, desc: true })
              .then(res => {
                return { ...res.desc, tableName: table };
              })
            )
          )
        );
      }).then(list => {
        return this.tableList = list;
      })
    );
  }

  createTempTable(guid: string): Promise<TableDescShort> {
    const data = this.guidMap[guid];
    if (data.createTask)
      return data.createTask;

    let cols = '*';
    const tmpTable = data.tmpTable;
    const table = data.desc.tableName;
    const where = data.args.cond ? 'where ' + getCompoundSQLCond(data.args.cond) : '';
    const groupBy = '';
    const orderBy = '';
    const sql = `create temp table ${tmpTable} as select ${cols} from ${table} ${where} ${groupBy} ${orderBy}`;

    console.log(sql);
    return (
      data.createTask = this.openDB()
      .then(() => deleteTable(this.db, tmpTable))
      .then(() => exec(this.db, sql))
      .then(() => 
        Promise.all([
          loadTableInfo(this.db, tmpTable),
          loadRowsNum(this.db, tmpTable)
        ])
      )
      .then(arr => {
        data.invalid = false;
        data.createTask = null;
        return {
          columns: arr[0].map(c => ({ colName: c.name, colType: c.type })),
          rowsNum: arr[1]
        };
      })
    );
  }

  createTableGuid(args: LoadTableGuidArgs, argsKey?: string): Promise<{ guid: string }> {
    argsKey = argsKey || getArgsKey(args);
    const { desc, ...other } = args;
    const id = this.tmpTableCounter++;
    const tmpTable = 'tmpt_' + id;
    const guid = 'guid_' + id;

    this.guidMap[guid] = {
      args: other,
      desc: { tableName: args.tableName, columns: [], rowsNum: 0 },
      tmpTable,
      invalid: false,
      createTask: null
    };
    this.argsToGuid[argsKey] = guid;

    return (
      this.createTempTable(guid)
      .then(res => {
        this.guidMap[guid].desc = {
          tableName: args.tableName,
          columns: res.columns,
          rowsNum: res.rowsNum
        };

        return { guid };
      })
    );
  }

  loadTableGuid(args: LoadTableGuidArgs): Promise<LoadTableGuidResult> {
    const argsKey = getArgsKey(args);
    const guid = this.argsToGuid[argsKey];
    let guidData = guid ? this.guidMap[guid] : null;

    let p: Promise<{ guid: string }> = Promise.resolve({ guid });
    if (!guidData)
      p = this.createTableGuid(args, argsKey);
    else if (guidData.invalid)
      p = this.createTempTable(guid)
      .then(res => {
        guidData.desc.rowsNum = res.rowsNum;
        guidData.desc.columns = res.columns;
        return { guid };
      });

    return (
      p.then(res => {
        if (!args.desc)
          return res;

        return {
          guid: res.guid,
          desc: this.guidMap[res.guid].desc
        };
      })
    );
  }

  getGuidData(guid: string): Promise<GuidMapData> {
    const data = this.guidMap[guid];
    if (!data)
      return Promise.reject(`guid = ${guid} not found`);

    if (!data.invalid)
      return Promise.resolve(data);
    
    return (
      this.createTempTable(guid)
      .then(res => {
        data.desc.columns = res.columns;
        data.desc.rowsNum = res.rowsNum;

        return data;
      })
    );
  }

  loadTableRowsNum(args: TableGuid): Promise<number> {
    return (
      this.openDB()
      .then(() => this.getGuidData(args.guid))
      .then(data => loadRowsNum(this.db, data.tmpTable))
    );
  }

  loadTableData(args: LoadTableDataArgs): Promise<LoadTableDataResult> {
    return (
      this.openDB()
      .then(() => this.getGuidData(args.guid))
      .then(data => {
        const where = '';
        const sql = `select * from ${data.tmpTable} ${where} limit ? offset ?`;
        return all<Object>(this.db, sql, [args.count, args.from]);
      })
      .then((rows: Array<StrMap>) => {
        return {
          rows,
          fromRow: args.from,
          rowsNum: args.count
        };
      })
    );
  }

  createTable(args: CreateTableArgs): Promise<TableDesc> {
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
      .then(res => {
        this.tableList = null;
        return this.loadTableList().then(() => res);
      })
    );
  }

  deleteTable(args: DeleteTableArgs): Promise<void> {
    return (
      this.openDB()
      .then(() => deleteTable(this.db, args.tableName))
      .then(() => {
        this.tableList = null;
        return this.loadTableList().then(() => {});
      })
    );
  }

  invalidateGuids(table: string) {
    Object.keys(this.guidMap)
    .forEach(key => {
      const data = this.guidMap[key];
      if (data.desc.tableName != table || data.invalid)
        return;

      data.invalid = true;
    });
  }

  pushData(args: PushDataArgs): Promise<PushDataResult> {
    return (
      insert({
        db: this.db,
        table: args.tableName,
        values: args.rows
      })
      .then(() => {
        this.invalidateGuids(args.tableName);
        this.tableList = null;
        return { pushRows: args.rows.length };
      })
    );
  }

  updateData(args: UpdateDataArgs): Promise<void> {
    const vals = args.values.map(value => `${sqlColumn(value.column)}=${quoteValue(value.value)}`).join(',');
    const where = args.cond ? 'where ' + getCompoundSQLCond(args.cond) : '';
    // const limit = args.limit != null ? `limit ${args.limit}` : '';
    const sql = `update ${sqlTable(args.tableName)} set ${vals} ${where}`;
    return (
      exec(this.db, sql)
      .then(() => this.invalidateGuids(args.tableName))
    );
  }

  deleteData(args: DeleteDataArgs): Promise<void> {
    return (
      deleteData({
        db: this.db,
        table: args.tableName,
        where: getCompoundSQLCond(args.cond)
      })
      .then(() => this.invalidateGuids(args.tableName))
    );
  }

  loadAggrData(args: LoadAggrDataArgs): Promise<LoadAggrDataResult> {
    return (
      this.getGuidData(args.guid)
      .then(data => {
        let sqlArr = args.values.map((v, i) => aggConv(v.aggs, v.column) + ` as col${i}`);
        return get(this.db, `select ${sqlArr.join(', ')} from ${data.tmpTable}`);
      })
      .then(res => {
        return {
          values: args.values.map((v, i) => {
            return {
              column: v.column,
              aggs: v.aggs,
              value: res['col'+i]
            };
          })
        };
      })
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

    file = file || this.getPath();
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

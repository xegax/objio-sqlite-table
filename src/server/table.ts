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
  CreateSubtableResult,
  NumStatsArgs,
  NumStats
} from 'objio-object/client/table';
// import { Database } from 'sqlite3';
import { SERIALIZER, EXTEND } from 'objio';
import { CSVReader, CSVBunch } from 'objio/server';
import { FileObject } from 'objio-object/server/file-object';
import { Database } from '../server/database';

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

  if (Array.isArray(value.value) && value.value.length == 2) {
    return `${value.column} >= ${value.value[0]} and ${value.column} <= ${value.value[1]}`;
  } else if (typeof value.value == 'object') {
    const val = value.value as CompoundCond;
    return `${value.column} in (select ${value.column} from ${val.table} where ${getCompSqlCondition(val)})`;
  }

  const op = value.inverse ? '!=' : '=';
  return `${value.column}${op}"${value.value}"`;
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

export class Table extends TableBase {
  private db: Database;
  
  constructor() {
    super();

    this.holder.setMethodsToInvoke({
      createSubtable: (args: SubtableAttrs) => this.createSubtable(args),
      loadCells: (args: LoadCellsArgs) => this.loadCells(args),
      pushCells: (args: PushRowArgs) => this.pushCells(args),
      updateCells: (args: UpdateRowArgs) => this.updateCells(args),
      removeRows: (args: RemoveRowsArgs) => this.removeRows(args),
      execute: (args: ExecuteArgs) => this.execute(args),
      getNumStats: (args: NumStatsArgs) => this.getNumStats(args)
    });

    this.holder.addEventHandler({
      onLoad: () => {
        // not configured
        if (!this.table)
          return;

        return (
          Promise.all([
            this.db.loadTableInfo(this.table),
            this.db.loadRowsCount(this.table)
          ])
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
    })
    .then(() => this.db.deleteTable(args.table))
    .then(() => this.db.createTable(args.table, columns))
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
      .then(() => this.updateRowNum())
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

  private updateRowNum(): Promise<number> {
    return (
      this.db.loadRowsCount(this.table)
      .then(rows => {
        this.totalRowsNum = rows;
        this.holder.save();
        return rows;
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

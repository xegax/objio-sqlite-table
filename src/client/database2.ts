import {
  TmpTableArgs,
  TableInfo,
  TableArgs,
  TableData,
  TableDataArgs,
  CreateTableArgs,
  DeleteTableArgs,
  PushDataArgs,
  PushDataResult,
  DeleteDataArgs
} from 'objio-object/base/database-holder';
import { DatabaseBase } from '../base/database';

export class Database2 extends DatabaseBase {
  loadTableList(): Promise<Array<TableInfo>> {
    return this.holder.invokeMethod({ method: 'loadTableList', args: {} });
  }

  loadTableInfo(args: TableArgs): Promise<TableInfo> {
    return this.holder.invokeMethod({ method: 'loadTableInfo', args });
  }

  loadTableRowsNum(args: TableArgs): Promise<number> {
    return this.holder.invokeMethod({ method: 'loadTableRowsNum', args });
  }

  loadTableData(args: TableDataArgs): Promise<TableData> {
    return this.holder.invokeMethod({ method: 'loadTableData', args });
  }

  createTempTable(args: TmpTableArgs): Promise<TableInfo> {
    return this.holder.invokeMethod({ method: 'createTempTable', args });
  }

  createTable(args: CreateTableArgs): Promise<TableInfo> {
    return this.holder.invokeMethod({ method: 'createTable', args });
  }

  deleteTable(args: DeleteTableArgs): Promise<void> {
    return this.holder.invokeMethod({ method: 'deleteTable', args });
  }

  pushData(args: PushDataArgs): Promise<PushDataResult> {
    return this.holder.invokeMethod({ method: 'pushData', args });
  }

  deleteData(args: DeleteDataArgs): Promise<void> {
    return this.holder.invokeMethod({ method: 'deleteData', args });
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
}

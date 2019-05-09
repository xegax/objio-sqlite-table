import {
  TableDataArgs,
  CreateTableArgs,
  DeleteTableArgs,
  PushDataArgs,
  PushDataResult,
  DeleteDataArgs,
  TableDesc,
  LoadTableGuidArgs,
  LoadTableGuidResult,
  TableGuid,
  LoadTableDataArgs,
  LoadTableDataResult,
  LoadAggrDataArgs,
  LoadAggrDataResult,
  UpdateDataArgs
} from 'objio-object/base/database-holder-decl';
// import { Datab} from 'objio-object/base/database-holder';
import { DatabaseBase } from '../base/database';

export class Database2 extends DatabaseBase {
  loadTableList(): Promise<Array<TableDesc>> {
    return this.holder.invokeMethod({ method: 'loadTableList', args: {} });
  }

  loadTableGuid(args: LoadTableGuidArgs): Promise<LoadTableGuidResult> {
    return this.holder.invokeMethod({ method: 'loadTableGuid', args });
  }

  loadTableRowsNum(args: TableGuid): Promise<number> {
    return this.holder.invokeMethod({ method: 'loadTableRowsNum', args });
  }

  loadTableData(args: LoadTableDataArgs): Promise<LoadTableDataResult> {
    return this.holder.invokeMethod({ method: 'loadTableData', args });
  }

  createTable(args: CreateTableArgs): Promise<TableDesc> {
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

  updateData(args: UpdateDataArgs): Promise<void> {
    return this.holder.invokeMethod({ method: 'updateData', args });
  }

  loadAggrData(args: LoadAggrDataArgs): Promise<LoadAggrDataResult> {
    return this.holder.invokeMethod({ method: 'loadAggrData', args });
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

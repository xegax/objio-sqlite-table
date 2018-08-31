import { OBJIOItem } from 'objio';
import {
  Columns,
  LoadCellsArgs,
  Cells,
  PushRowArgs,
  NumStats,
  NumStatsArgs,
  CreateSubtableResult,
  SubtableAttrs
} from 'objio-object/client/table';

export class Database extends OBJIOItem {
  loadTableInfo(table: string): Promise<Columns> {
    return this.holder.invokeMethod('loadTableInfo', { table });
  }

  loadRowsCount(table: string): Promise<number> {
    return this.holder.invokeMethod('loadRowsCount', { table });
  }

  deleteTable(table: string): Promise<void> {
    return this.holder.invokeMethod('deleteTable', { table });
  }

  createTable(table: string, columns: Columns): Promise<void> {
    return this.holder.invokeMethod('createTable', { table, columns });
  }

  loadCells(args: LoadCellsArgs): Promise<Cells> {
    return this.holder.invokeMethod('loadCells', args);
  }

  pushCells(args: PushRowArgs): Promise<number> {
    return this.holder.invokeMethod('pushCells', args);
  }

  getNumStats(args: NumStatsArgs): Promise<NumStats> {
    return this.holder.invokeMethod('getNumStats', args);
  }

  createSubtable(args: SubtableAttrs): Promise<CreateSubtableResult> {
    return this.holder.invokeMethod('createSubtable', args);
  }
}

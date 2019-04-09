import { OBJIOItemClass } from 'objio';
import { Database } from './database';
import { Database2 } from './database2';

export function getClasses(): Array<OBJIOItemClass> {
  return [
    Database,
    Database2
  ];
}

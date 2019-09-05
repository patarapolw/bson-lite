import { EventEmitter } from "events";
import uuid4 from "uuid/v4";
import { Db, isObjectNotNull } from "./db";
import _filter from "lodash.filter";
import _map from "lodash.map";

interface IMeta<T> {
  unique: Partial<Record<keyof T, boolean>>;
  indexes: Partial<Record<keyof T, Record<string, string[]>>>; 
}

interface ICollectionOptions<T> {
  unique?: Array<keyof T>;
  indexes?: Array<keyof T>;
}

export class Collection<T> {
  private db: Db;
  private name: string;
  
  public events: EventEmitter;

  constructor(db: Db, name: string, options: Partial<ICollectionOptions<T>> = {}) {
    this.db = db;
    this.name = name;
    this.events = new EventEmitter();

    if (!this.db.get(this.name)) {
      this.db.set(this.name, {
        __meta: {
          unique: (() => {
            const output: any = {};
            if (options.unique) {
              for (const u of options.unique) {
                output[u] = {};
              }
            }

            return output;
          })(),
          indexes: (() => {
            const output: any = {};
            if (options.indexes) {
              for (const i of options.indexes) {
                output[i] = {};
              }
            }

            return output;
          })()
        },
        data: {}
      });
    }
  }

  get __meta(): IMeta<T> {
    return this.db.get(`${this.name}.__meta`);
  }

  public create(entry: T): string | null {
    this.events.emit("pre-create", entry);

    if (this.isDuplicate(entry)) {
      return null;
    }

    this.addDuplicate(entry);

    let { _id } = entry as any;
    if (!_id) {
      _id = uuid4();
    }
  
    this.db.set(`${this.name}.data.${_id}`, {
      _id,
      ...entry
    });

    this.addIndex(entry, _id);

    this.events.emit("create", entry);
    return _id;
  }

  public find(cond: any): T[] {
    this.events.emit("pre-read", cond);
    let data: T[] | null = [];

    if (isObjectNotNull(cond) && Object.keys(cond).length === 1) {
      const k = Object.keys(cond)[0];
      if (k === "_id") {
        data = this.getByIndex(cond[k]);
      }
      const indexes = this.__meta.indexes;
      if (Object.keys(indexes).includes(k)) {
        data = this.getByIndex(cond[k], k);
      }
    }

    if (!data) {
      data = _filter(Object.values<T>(this.db.get(`${this.name}.data`, {})), cond);
    }

    this.events.emit("read", cond, data);

    return data;
  }

  public get(cond: any): T | null {
    return this.find(cond)[0] || null;
  }

  public getByIndex(_id: string, indexName?: string): T[] {
    if (indexName) {
      const indexes = this.__meta.indexes;
      if (!Object.keys(indexes).includes(indexName)) {
        throw new Error("Invalid index name");
      }

      return this.getIndex(indexName as any, _id).map((el) => this.getByIndex(el)[0]);
    }

    return [this.db.get(`${this.name}.data.${_id}`)]
  }

  public update(
    cond: any,
    transform: any
  ): boolean {
    this.events.emit("pre-update", cond, transform);

    const found = this.find(cond);
    const changes = found.map<T>(transform);
    if (changes.some(this.isDuplicate)) {
      return false;
    }
    found.forEach(this.removeDuplicate);
    changes.forEach(this.addDuplicate);

    for (const c of changes) {
      this.db.set(`${this.name}.data.${(c as any)._id}`, c);
    }
    this.events.emit("update", cond, transform, changes);

    return true;
  }

  public async delete(
    cond: any
  ) {
    this.events.emit("pre-delete", cond);

    const changes = this.find(cond);
    changes.forEach(this.removeDuplicate);

    for (const c of changes) {
      this.db.set(`${this.name}.data.${(c as any)._id}`, undefined);
      this.removeIndex(c, (c as any)._id);
    }  

    this.events.emit("delete", cond, changes);
  }

  private isDuplicate(entry: T): boolean {
    for (const [k, v] of Object.entries<any>(this.__meta.unique)) {
      if (v[(entry as any)[k]]) {
        return true;
      }
    }

    return false;
  }

  private addDuplicate(entry: T) {
    Object.keys(this.__meta.unique).map((u) => {
      return this.db.set(`${this.name}.__meta.unique.${u}.${(entry as any)[u]}`, true);
    });
  }

  private removeDuplicate(entry: T) {
    Object.keys(this.__meta.unique).map((u) => {
      return this.db.set(`${this.name}.__meta.unique.${u}.${(entry as any)[u]}`, undefined);
    });
  }

  private getIndex(k: keyof T, v: string): string[] {
    return this.db.get(`${this.name}.__meta.indexes.${k}.${v}`, []);
  }

  private addIndex(entry: T, _id: string) {
    const indexes = this.__meta.indexes;
    for (const [k, v] of Object.entries(entry)) {
      if (Object.keys(indexes).includes(k)) {
        const ids: string[] = this.getIndex(k as any, v);
        if (!ids.includes(_id)) {
          ids.push(_id);
          this.db.set(`${this.name}.__meta.indexes.${k}.${v}`, ids);
        }
      }
    }
  }

  private removeIndex(entry: T, _id: string) {
    const indexes = this.__meta.indexes;
    for (const [k, v] of Object.entries(entry)) {
      if (Object.keys(indexes).includes(k)) {
        const ids: string[] = this.getIndex(k as any, v);
        ids.splice(ids.indexOf(_id), 1);
        this.db.set(`${this.name}.__meta.indexes.${k}.${v}`, ids);
      }
    }
  }
}

interface IJoiner<T> {
  col: T[],
  key: keyof T;
  null?: boolean;
}

export function joinCollection<T, U>(
  left: IJoiner<T>, right: IJoiner<U>,
  mapFn?: (left?: T, right?: U) => T & U
): Array<T & U> {
  const joinMap: Record<string | number, {
    left?: T,
    right?: U
  }> = {};

  for (const l of left.col) {
    let key: any;
    if (l[left.key]) {
      key = l[left.key];
    } else if (left.null) {
      key = uuid4();
    }

    joinMap[key] = joinMap[key] || {};
    joinMap[key].left = l;
  }

  for (const r of right.col) {
    let key: any;
    if (r[right.key]) {
      key = r[right.key];
    } else if (right.null) {
      key = uuid4();
    }

    joinMap[key] = joinMap[key] || {};
    joinMap[key].right = r;
  }

  return Object.values(joinMap)
  .filter((el) => el.left || el.right)
  .map((el) => {
    if (mapFn) {
      return mapFn(el.left, el.right);
    } else {
      return Object.assign(el.right || {}, el.left || {}) as T & U;
    }
  });
}
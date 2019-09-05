import { EventEmitter } from "events";
import uuid4 from "uuid/v4";
import { Db, dotGetter } from "./db";
import _filter from "lodash.filter";
import _map from "lodash.map";

interface IMeta<T> {
  unique: Set<keyof T>;
}

interface ICollectionOptions<T> {
  unique?: Array<keyof T>;
}

export class Collection<T> {
  private __meta: IMeta<T> = {
    unique: new Set()
  };

  private db: Db;
  private name: string;
  
  public events: EventEmitter;

  constructor(db: Db, name: string, options: Partial<ICollectionOptions<T>> = {}) {
    this.db = db;
    this.name = name;
    this.events = new EventEmitter();

    if (options.unique) {
      for (const u of options.unique) {
        this.__meta.unique.add(u);
      }
    }

    this.build();
  }

  public build() {
    if (!this.db.get(this.name)) {
      this.db.set(this.name, {
        __meta: {
          unique: (() => {
            const output: any = {};
            for (const u of this.__meta.unique) {
              output[u] = {};
            }

            return output;
          })()
        },
        data: {}
      });
    }
  }

  public create(entry: T): string | null {
    this.events.emit("pre-create", entry);

    if (this.isDuplicate(entry)) {
      return null;
    }

    this.addDuplicate(entry);
    
    const _id = uuid4();
    this.db.set(`${this.name}.data.${_id}`, {
      _id,
      ...entry
    });

    this.events.emit("create", entry);
    return _id;
  }

  public find(cond: any): T[] {
    this.events.emit("pre-read", cond);

    const data = _filter(Object.values<T>(this.db.get(`${this.name}.data`, {})), cond);
    this.events.emit("read", cond, data);

    return data;
  }

  public get(cond: any): T | null {
    return this.find(cond)[0] || null;
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
    }  

    this.events.emit("delete", cond, changes);
  }

  private isDuplicate(entry: T): boolean {
    const unique = this.db.get(`${this.name}.__meta.unique`, {});
    for (const u of this.__meta.unique) {
      if (dotGetter(unique, [u, (entry as any)[u]])) {
        return true;
      }
    }

    return false;
  }

  private addDuplicate(entry: T) {
    Array.from(this.__meta.unique).map((u) => {
      return this.db.set(`${this.name}.__meta.unique.${u}.${(entry as any)[u]}`, true);
    });
  }

  private removeDuplicate(entry: T) {
    Array.from(this.__meta.unique).map((u) => {
      return this.db.set(`${this.name}.__meta.unique.${u}.${(entry as any)[u]}`, undefined);
    });
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
    if (l[left.key]) {
      joinMap[left.key as any].left = l;
    } else if (left.null) {
      joinMap.__.left = l;
    }
  }

  for (const r of right.col) {
    if (r[right.key]) {
      joinMap[right.key as any].right = r;
    } else if (right.null) {
      joinMap.__.right = r;
    }
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
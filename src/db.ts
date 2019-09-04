import bson from "bson";
import fs from "fs";

export class Db {
  public filename: string;
  private data: any = {};

  constructor(filename: string) {
    this.filename = filename;
    if (fs.existsSync(this.filename)) {
      this.data = bson.deserialize(fs.readFileSync(this.filename));
    }
  }

  public commit() {
    fs.writeFileSync(this.filename, bson.serialize(this.data));
  }

  public get(p: string | string[]) {
    return dotGetter(this.data, p);
  }

  public set(p: string | string[], value: any) {
    return dotSetter(this.data, p, value);
  }
}

export function dotGetter(data: any, p: string | string[]) {
  if (typeof p === "string") {
    p = p.split(".");
  }
  
  p.forEach((pn, i) => {
    if (Array.isArray(data) && /\d+/.test(pn)) {
      data = data[parseInt(pn)];
    } else if (isObjectNotNull(data)) {
      data = data[pn];
    } else if (i < p.length - 1) {
      data = {};
    }
  });

  if (isObjectNotNull(data) && Object.keys(data).length === 0) {
    return undefined;
  }

  return data;
}

export function dotSetter(data: any, p: string | string[], value: any) {
  if (typeof p === "string") {
    p = p.split(".");
  }
  
  p.slice(0, p.length - 1).forEach((pn, i) => {
    if (Array.isArray(data) && /\d+/.test(pn)) {
      data = data[parseInt(pn)];
    } else if (isObjectNotNull(data)) {
      data = data[pn];
    } else if (i < p.length - 1) {
      data = {};
    }
  });

  if (isObjectNotNull(data)) {
    data[p[p.length - 1]] = value;
  }
}

export function isObjectNotNull(data: any) {
  return data && typeof data === "object" && data.constructor === {}.constructor;
}
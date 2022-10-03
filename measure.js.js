// import { writeSync } from "fs";
import { createHook } from "async_hooks";

class Stack {
  constructor() {
    this._array = [];
  }
  push(x) {
    return this._array.push(x);
  }
  peek() {
    return this._array[this._array.length - 1];
  }
  pop() {
    return this._array.pop();
  }
  get is_not_empty() {
    return this._array.length > 0;
  }
}

class Timer {
  constructor() {
    this._records = new Map(); /* of {start:number, end:number} */
  }
  starts(scope) {
    const detail = this._records.set(scope, {
      start: this.timestamp(),
      end: -1,
    });
  }
  ends(scope) {
    this._records.get(scope).end = this.timestamp();
  }
  timestamp() {
    return Date.now();
  }
  timediff(t0, t1) {
    return Math.abs(t0 - t1);
  }
  report(scopes, detail) {
    let tSyncOnly = 0;
    let tSyncAsync = 0;
    for (const [scope, { start, end }] of this._records)
      if (scopes.has(scope))
        if (~end) {
          tSyncOnly += end - start;
          tSyncAsync += end - start;
          const { type, offset } = detail.get(scope);
          if (type === "Timeout") tSyncAsync += offset;
          // writeSync(1, `async scope ${scope} \t... ${end - start}ms \n`);
        }
    return { tSyncOnly, tSyncAsync };
  }
}

export async function measure(asyncFn) {
  const stack = new Stack();
  const scopes = new Set();
  const timer = new Timer();
  const detail = new Map();
  const hook = createHook({
    init(scope, type, parent, resource) {
      if (type === "TIMERWRAP") return;
      scopes.add(scope);
      detail.set(scope, {
        type: type,
        offset: type === "Timeout" ? resource._idleTimeout : 0,
      });
    },
    before(scope) {
      if (stack.is_not_empty) timer.ends(stack.peek());
      stack.push(scope);
      timer.starts(scope);
    },
    after() {
      timer.ends(stack.pop());
    },
  });

  // Force to create a new async scope by wrapping asyncFn in setTimeout,
  // st sync part of asyncFn() is a async op from async_hooks POV.
  // The extra async scope also take time to run which should not be count
  return await new Promise((r) => {
    hook.enable();
    setTimeout(() => {
      asyncFn()
        .then(() => hook.disable())
        .then(() => r(timer.report(scopes, detail)))
        .catch(console.error);
    }, 1);
  });
}

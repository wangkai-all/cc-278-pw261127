// Minimal DOM shim to exercise the real static/app.js against a live server.
// Not a browser substitute: it verifies our own rendering code paths, element
// wiring, and every API call the page makes.
import fs from "node:fs";
import vm from "node:vm";

class ClassList {
  constructor(node) { this.node = node; }
  get value() { return this.node.className.split(/\s+/).filter(Boolean); }
}
class Node {
  constructor(tag) {
    this.tagName = tag.toUpperCase();
    this.children = [];
    this.attributes = {};
    this.listeners = {};
    this._text = "";
    this.className = "";
    this.id = "";
    this.hidden = false;
    this.disabled = false;
    this.value = "";
    this.checked = false;
    this.onclick = null;
  }
  append(...nodes) {
    for (const n of nodes) {
      if (typeof n === "string") {
        const t = new Text(n);
        t.parent = this;
        this.children.push(t);
      } else if (n instanceof Text) {
        n.parent = this;
        this.children.push(n);
      } else if (n && n.isFragment) {
        for (const c of n.children) c.parent = this;
        this.children.push(...n.children);
      } else if (n) {
        n.parent = this;
        this.children.push(n);
      }
    }
  }
  prepend(...nodes) { this.children.unshift(...nodes); }
  replaceChildren(...nodes) { this.children = []; this.append(...nodes); }
  replaceWith(node) {
    const i = this.parent ? this.parent.children.indexOf(this) : -1;
    if (i >= 0) { node.parent = this.parent; this.parent.children[i] = node; }
  }
  remove() { if (this.parent) this.parent.children = this.parent.children.filter((c) => c !== this); }
  get firstChild() { return this.children[0] || null; }
  setAttribute(k, v) {
    this.attributes[k] = String(v);
    if (k === "id") this.id = String(v);
    if (k === "class") this.className = String(v);
  }
  getAttribute(k) { return this.attributes[k] ?? null; }
  addEventListener(type, fn) { (this.listeners[type] ||= []).push(fn); }
  async dispatch(type, event = {}) {
    const ev = { preventDefault() {}, target: this, ...event };
    for (const fn of this.listeners[type] || []) await fn(ev);
    if (type === "click" && this.onclick) await this.onclick(ev);
  }
  set textContent(v) { this._text = String(v); this.children = []; }
  get textContent() {
    return this._text + this.children.map((c) => (c instanceof Text ? c._text : c.textContent)).join("");
  }
  get innerText() { return this.textContent; }
  set innerHTML(v) { throw new Error("innerHTML is not allowed in this UI"); }
  get rows() { return this.children.filter((c) => c.tagName === "TR"); }
  get tBodies() { return this.children.filter((c) => c.tagName === "TBODY"); }
  querySelectorAll(sel) {
    const out = [];
    const walk = (node) => {
      for (const c of node.children) {
        if (c.tagName && sel.startsWith(".") && c.className.split(/\s+/).includes(sel.slice(1))) out.push(c);
        if (c.tagName === sel.toUpperCase()) out.push(c);
        if (c.children) walk(c);
      }
    };
    walk(this);
    return out;
  }
  querySelector(sel) { return this.querySelectorAll(sel)[0] || null; }
  getContext() { return null; }
}
class Text {
  constructor(text) { this._text = text; }
  get textContent() { return this._text; }
  get className() { return ""; }
}
class Fragment extends Node {
  constructor() { super("#fragment"); this.isFragment = true; }
}

function buildDocument() {
  const byId = new Map();
  const root = new Node("html");
  const body = new Node("body");
  root.append(body);
  const html = fs.readFileSync(new URL("../static/index.html", import.meta.url), "utf8");
  // Build only the elements the script looks up; ids are all we need, plus
  // nesting for panels/tables is irrelevant to the code under test.
  for (const match of html.matchAll(/<(\w+)([^>]*\bid="([^"]+)"[^>]*)>/g)) {
    const node = new Node(match[1]);
    node.id = match[3];
    const hidden = /hidden/.test(match[2]);
    node.hidden = hidden;
    byId.set(match[3], node);
    body.append(node);
  }
  const document = {
    getElementById: (id) => {
      // Prefer the live tree: replaceTable swaps table nodes out, so a static
      // id map would hand back a detached node.
      const live = findById(root, id);
      return live || byId.get(id) || null;
    },
    createElement: (tag) => new Node(tag),
    createTextNode: (text) => new Text(text),
    createDocumentFragment: () => new Fragment(),
    querySelector: (sel) => root.querySelector(sel),
    body,
  };
  return document;
}

function findById(node, id) {
  if (node.id === id) return node;
  for (const child of node.children || []) {
    const found = findById(child, id);
    if (found) return found;
  }
  return null;
}

export function runPage(base) {
  const document = buildDocument();
  const calls = [];
  const errors = [];
  const sandbox = {
    document,
    console: {
      log: (...a) => calls.push(["log", ...a]),
      error: (...a) => errors.push(a.join(" ")),
      warn: (...a) => errors.push(a.join(" ")),
    },
    fetch: async (path, options) => {
      calls.push([options?.method || "GET", path]);
      const res = await fetch(base + path, options);
      const text = await res.text();
      return {
        ok: res.ok,
        status: res.status,
        statusText: res.statusText,
        text: async () => text,
      };
    },
    setTimeout,
    clearInterval,
    setInterval,
    clearTimeout,
    Text,
  };
  sandbox.window = sandbox;
  sandbox.globalThis = sandbox;
  const code = fs.readFileSync(new URL("../static/app.js", import.meta.url), "utf8");
  vm.createContext(sandbox);
  vm.runInContext(code, sandbox, { filename: "app.js" });
  return { document, sandbox, calls, errors };
}

export async function settle(ms = 1200) {
  await new Promise((r) => setTimeout(r, ms));
}

export function textOf(node) { return node ? node.textContent : ""; }
export function tableRows(document, id) {
  const table = document.getElementById(id);
  if (!table) return [];
  const tbody = table.children.find((c) => c.tagName === "TBODY") || table;
  return tbody.children.filter((c) => c.tagName === "TR").map((tr) => tr.children.map((td) => td.textContent));
}

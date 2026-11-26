const GC = (() => {
  // ---------------------------------------------------------------- vocabulary
  const BANDS = ["2200m", "630m", "160m", "80m", "60m", "40m", "30m", "20m", "17m", "15m", "12m", "10m", "6m", "4m", "2m", "1.25m", "70cm", "33cm", "23cm", "13cm"];
  const FILTER_MODES = ["CW", "FT2", "FT4", "FT8", "JS8", "LSB", "USB", "RTTY", "MSK144", "PSK", "SSTV", "UNKNOWN"];
  const MODE_VARIANTS = { PSK31: "PSK", PSK63: "PSK", PSK125: "PSK" };
  const CONF_EXEMPT = { MSK144: true, PSK: true };
  const EVENTS = ["LLOTA", "IOTA", "POTA", "SOTA", "WWFF"];
  const SOURCES = ["HUMAN", "SKIMMER"];
  const CONTINENTS = ["AF", "AN", "AS", "EU", "NA", "OC", "SA"];
  const CONF_SYMBOLS = ["?", "S", "C", "P", "V", "B"];
  const PATH_CLASSES = ["HIGH", "MEDIUM", "LOW", "UNLIKELY", "CLOSED", "INSUFFICIENT"];
  const STATES = ["AA", "AB", "AE", "AK", "AL", "AP", "AR", "AS", "AZ", "BC", "CA", "CO", "CT", "DC", "DE", "FL", "GA", "GU", "HI", "IA",
    "ID", "IL", "IN", "KS", "KY", "LA", "MA", "MB", "MD", "ME", "MI", "MN", "MO", "MP", "MS", "MT", "NB", "NC", "ND", "NE",
    "NH", "NJ", "NL", "NM", "NS", "NT", "NU", "NV", "NY", "OH", "OK", "ON", "OR", "PA", "PE", "PR", "QC", "RI", "SC", "SD",
    "SK", "TN", "TX", "UM", "UT", "VA", "VI", "VT", "WA", "WI", "WV", "WY", "YT"];
  const STATE_SET = new Set(STATES);
  // runtime.yaml filter.default_modes / default_sources (shipped).
  const DEFAULT_MODES = ["CW", "LSB", "USB", "UNKNOWN", "RTTY", "MSK144", "PSK31", "SSTV"];
  const DEFAULT_SOURCES = ["HUMAN", "SKIMMER"];

  const trimSpace = s => s.replace(/^[ \t\n\v\f\r]+|[ \t\n\v\f\r]+$/g, "");
  const upper = s => s.replace(/[a-z]/g, c => c.toUpperCase());
  const lower = s => s.replace(/[A-Z]/g, c => c.toLowerCase());
  const normalizeUpper = s => upper(trimSpace(s));
  const goQuote = s => '"' + s.replace(/\\/g, "\\\\").replace(/"/g, '\\"') + '"';

  function normalizeBand(label) {
    let c = lower(trimSpace(label));
    if (c === "") return "";
    for (const [o, n] of [["meters", "m"], ["meter", "m"], ["metres", "m"], ["metre", "m"],
      ["centimeters", "cm"], ["centimeter", "cm"], ["centimetres", "cm"], ["centimetre", "cm"]]) c = c.split(o).join(n);
    c = c.split(" ").join("");
    if (c === "") return "";
    const last = c[c.length - 1];
    if (last >= "0" && last <= "9") c += "m";
    return c;
  }
  const BAND_SET = new Set(BANDS.map(normalizeBand));
  // spot/meta_bands.go: fixed command conveniences over the physical catalog.
  const META_BANDS = [["WARC", ["30m", "17m", "12m"]], ["HFCONTEST", ["160m", "80m", "40m", "20m", "15m", "10m"]],
    ["HF", ["160m", "80m", "60m", "40m", "30m", "20m", "17m", "15m", "12m", "10m"]], ["VHF", ["6m", "4m", "2m", "1.25m"]],
    ["UHF", ["70cm", "33cm", "23cm", "13cm"]], ["VHFUP", ["6m", "4m", "2m", "1.25m", "70cm", "33cm", "23cm", "13cm"]]];
  function metaBandMembers(name) { const u = upper(name); for (const [n, m] of META_BANDS) if (n === u) return m.slice(); return null; }
  const isValidBand = l => { const n = normalizeBand(l); return n !== "" && BAND_SET.has(n); };
  function canonicalMode(mode) {
    const u = upper(trimSpace(mode));
    if (u === "") return "";
    return MODE_VARIANTS[u] || u;
  }
  const canonicalModeForFilter = m => { let u = upper(trimSpace(m)); if (u === "") u = "UNKNOWN"; return canonicalMode(u); };
  const isSupportedMode = m => FILTER_MODES.includes(canonicalModeForFilter(m));
  const eventMask = n => { const i = EVENTS.indexOf(upper(trimSpace(n))); return i < 0 ? 0 : (1 << i); };
  const isSupportedEvent = n => eventMask(n) !== 0;
  const isSupportedZone = z => z >= 1 && z <= 40;
  const isState = c => STATE_SET.has(c);
  const isSupportedContinent = c => CONTINENTS.includes(normalizeUpper(c));
  const isSupportedSource = s => SOURCES.includes(normalizeUpper(s));
  const isSupportedConf = s => { const n = normalizeUpper(s); return n !== "" && CONF_SYMBOLS.includes(n); };
  const normalizePathClass = l => { const n = normalizeUpper(l); return PATH_CLASSES.includes(n) ? n : ""; };
  const isSupportedPathClass = c => normalizePathClass(c) !== "";
  const validGrid2 = v => v.length === 2 && v[0] >= "A" && v[0] <= "R" && v[1] >= "A" && v[1] <= "R";
  function normalizeGrid2Token(g) {
    g = normalizeUpper(g);
    if (g === "") return "";
    if (g.length > 2) g = g.slice(0, 2);
    return g.length === 2 ? g : "";
  }
  function validCallsignPattern(v) {
    if (v === "" || v !== trimSpace(v)) return false;
    const stars = (v.match(/\*/g) || []).length;
    if (stars > 1 || (stars === 1 && !v.startsWith("*") && !v.endsWith("*"))) return false;
    return /^[A-Za-z0-9/\-*]+$/.test(v);
  }
  function validCommentPhrase(p) {
    if (p.length === 0 || p.length > 64) return false;
    let nonSpace = false;
    for (let i = 0; i < p.length; i++) {
      const c = p.charCodeAt(i);
      if (c < 32 || c > 126) return false;
      if (c !== 32) nonSpace = true;
    }
    return nonSpace;
  }
  function matchCommentPhrase(comment, phrase) {
    if (!validCommentPhrase(phrase) || phrase.length > comment.length) return false;
    const fold = c => (c >= 65 && c <= 90 ? c + 32 : c);
    const p = [];
    for (let i = 0; i < phrase.length; i++) p.push(fold(phrase.charCodeAt(i)));
    outer: for (let i = 0; i + p.length <= comment.length; i++) {
      for (let j = 0; j < p.length; j++) {
        const c = fold(comment.charCodeAt(i + j));
        if (c >= 128 || c !== p[j]) continue outer;
      }
      return true;
    }
    return false;
  }
  function normalizeConfidenceSymbol(label) {
    const v = trimSpace(label || "");
    if (v === "") return "?";
    let u = upper(v);
    if (CONF_SYMBOLS.includes(u)) return u;
    if (u.endsWith("%")) u = u.slice(0, -1);
    const n = goAtoi(u);
    if (n.err) return "";
    return n.value <= 25 ? "?" : n.value <= 75 ? "P" : "V";
  }

  // Go strconv.Atoi (64-bit int) and ParseFloat validity + FormatFloat('f', -1).
  function goAtoi(s) {
    const syntax = { err: `strconv.Atoi: parsing ${goQuote(s)}: invalid syntax` };
    if (!/^[+-]?[0-9]+$/.test(s)) return syntax;
    const big = BigInt(s);
    if (big > 9223372036854775807n || big < -9223372036854775808n) return { err: `strconv.Atoi: parsing ${goQuote(s)}: value out of range` };
    return { value: Number(big) };
  }
  function goParseFloat(s) {
    if (/^[+-]?(inf|infinity)$/i.test(s)) return { value: s[0] === "-" ? -Infinity : Infinity };
    if (/^[+-]?nan$/i.test(s)) return { value: NaN };
    if (/^[+-]?([0-9]+\.?[0-9]*|\.[0-9]+)([eE][+-]?[0-9]+)?$/.test(s)) {
      const v = Number(s);
      if (!isFinite(v)) return { err: true }; // ErrRange
      return { value: v };
    }
    const hex = /^([+-]?)0[xX]([0-9a-fA-F]*)\.?([0-9a-fA-F]*)[pP]([+-]?[0-9]+)$/.exec(s);
    if (hex && (hex[2] + hex[3]).length > 0 && /^[+-]?0[xX]([0-9a-fA-F]+\.?[0-9a-fA-F]*|\.[0-9a-fA-F]+)[pP]/.test(s)) {
      let mant = 0;
      for (const ch of hex[2] + hex[3]) mant = mant * 16 + parseInt(ch, 16);
      const v = mant * Math.pow(2, Number(hex[4]) - 4 * hex[3].length);
      if (!isFinite(v)) return { err: true };
      return { value: hex[1] === "-" ? -v : v };
    }
    return { err: true };
  }
  function formatFloat(x) {
    let s = String(x);
    if (!/e/i.test(s)) return s;
    const neg = s[0] === "-"; if (neg) s = s.slice(1);
    let [m, e] = s.split(/e/i); e = Number(e);
    let [ip, fp = ""] = m.split(".");
    let digits = ip + fp, point = ip.length + e;
    if (point <= 0) s = "0." + "0".repeat(-point) + digits;
    else if (point >= digits.length) s = digits + "0".repeat(point - digits.length);
    else s = digits.slice(0, point) + "." + digits.slice(point);
    return (neg ? "-" : "") + s;
  }

  // ---------------------------------------------------------------- filter/expr
  const LIM = { MaxBytes: 1024, MaxAtoms: 32, MaxNodes: 128, MaxDepth: 8, MaxValuesPerAtom: 32, Slots: 10, TotalAtoms: 128, TotalValues: 512 };
  const K = { Atom: 0, Not: 1, And: 2, Or: 3, Pred: 4 };
  class SyntaxError_ extends Error {
    constructor(col, msg) { super(`col ${col < 1 ? 1 : col}: ${msg}`); this.col = col < 1 ? 1 : col; this.msg = msg; }
  }
  const synErr = (col, msg) => new SyntaxError_(col, msg);
  const bareByte = b => /[A-Za-z0-9_\-./*?#@]/.test(b);
  const VALID_OPS = ["=", "<>", "<", "<=", ">", ">="];

  function parseExpr(source) {
    if (source.length > LIM.MaxBytes) throw synErr(LIM.MaxBytes + 1, "expression exceeds byte limit");
    for (let i = 0; i < source.length; i++) {
      const c = source.charCodeAt(i);
      if (c < 32 || c > 126) throw synErr(i + 1, "expected printable ASCII");
    }
    const p = { source, pos: 0, ex: { nodes: [], root: -1, atoms: 0 } };
    const spaces = () => { while (p.pos < source.length && source[p.pos] === " ") p.pos++; };
    const take = b => { spaces(); if (p.pos < source.length && source[p.pos] === b) { p.pos++; return true; } return false; };
    const word = () => { spaces(); let end = p.pos; while (end < source.length && bareByte(source[end])) end++; return source.slice(p.pos, end); };
    const keyword = w => upper(word()) === w;
    const add = (node, col) => {
      if (p.ex.nodes.length >= LIM.MaxNodes) throw synErr(col, "expression exceeds node limit");
      p.ex.nodes.push(node);
      return p.ex.nodes.length - 1;
    };
    function parseOr(depth) {
      let left = parseAnd(depth);
      while (keyword("OR")) {
        const col = p.pos + 1; p.pos += 2;
        const right = parseAnd(depth);
        left = add({ kind: K.Or, left, right }, col);
      }
      return left;
    }
    function parseAnd(depth) {
      let left = parseTerm(depth);
      while (keyword("AND")) {
        const col = p.pos + 1; p.pos += 3;
        const right = parseTerm(depth);
        left = add({ kind: K.And, left, right }, col);
      }
      return left;
    }
    function parseTerm(depth) {
      spaces();
      const col = p.pos + 1;
      if (keyword("NOT")) {
        if (depth >= LIM.MaxDepth) throw synErr(col, "expression exceeds nesting limit");
        p.pos += 3;
        const child = parseTerm(depth + 1);
        return add({ kind: K.Not, left: child, right: -1 }, col);
      }
      if (take("(")) {
        if (depth >= LIM.MaxDepth) throw synErr(col, "expression exceeds nesting limit");
        const child = parseOr(depth + 1);
        if (!take(")")) throw synErr(p.pos + 1, "expected closing parenthesis");
        return child;
      }
      return parseAtom();
    }
    function parseAtom() {
      const field = word();
      const col = p.pos + 1;
      if (field === "" || upper(field) === "AND" || upper(field) === "OR") throw synErr(col, "expected field or Boolean term");
      if (p.ex.atoms >= LIM.MaxAtoms) throw synErr(col, "expression exceeds atom limit");
      p.pos += field.length;
      if (upper(field) === "NEARBY") {
        spaces();
        if (p.pos < source.length && "=<>".includes(source[p.pos])) throw synErr(p.pos + 1, "NEARBY takes no operator or values. Use NEARBY on its own");
        const idx = add({ kind: K.Pred, left: -1, right: -1, atom: { field: "NEARBY", op: "", values: [], col } }, col);
        p.ex.atoms++;
        return idx;
      }
      const op = operator();
      const values = valueList();
      const idx = add({ kind: K.Atom, left: -1, right: -1, atom: { field: upper(field), op, values, col } }, col);
      p.ex.atoms++;
      return idx;
    }
    function operator() {
      spaces();
      const start = p.pos;
      if (start < source.length) {
        const c = source[p.pos];
        if (c === "=") p.pos++;
        else if (c === "<" || c === ">") {
          p.pos++;
          if (p.pos < source.length && (source[p.pos] === "=" || (source[start] === "<" && source[p.pos] === ">"))) p.pos++;
        }
      }
      if (start === p.pos) throw synErr(start + 1, "expected comparison operator");
      return source.slice(start, p.pos);
    }
    function valueList() {
      const bracketed = take("[");
      const values = [];
      for (;;) {
        spaces();
        if (values.length >= LIM.MaxValuesPerAtom) throw synErr(p.pos + 1, `This comparison accepts at most ${LIM.MaxValuesPerAtom} values`);
        values.push(value());
        if (!take(",")) break;
      }
      if (bracketed && !take("]")) throw synErr(p.pos + 1, "expected comma or closing bracket");
      return values;
    }
    function value() {
      spaces();
      const col = p.pos + 1;
      if (take('"')) {
        let text = "";
        while (p.pos < source.length) {
          const b = source[p.pos++];
          if (b !== '"') text += b;
          else if (p.pos < source.length && source[p.pos] === '"') { text += '"'; p.pos++; }
          else return { text, quoted: true, col };
        }
        throw synErr(col, "unterminated quoted value");
      }
      const text = word();
      if (text === "") throw synErr(col, "expected value");
      p.pos += text.length;
      return { text: upper(text), quoted: false, col };
    }
    const root = parseOr(0);
    spaces();
    if (p.pos !== source.length) throw synErr(p.pos + 1, "unexpected token after expression");
    p.ex.root = root;
    canonical(p.ex);
    return p.ex;
  }

  const precedence = k => ({ [K.Atom]: 4, [K.Pred]: 4, [K.Not]: 3, [K.And]: 2, [K.Or]: 1 })[k] || 0;
  function canonical(ex) {
    if (!ex || ex.nodes.length === 0) throw synErr(1, "empty expression tree");
    if (ex.nodes.length > LIM.MaxNodes) throw synErr(1, "expression exceeds node limit");
    if (ex.atoms < 1 || ex.atoms > LIM.MaxAtoms) throw synErr(1, "invalid expression atom count");
    let out = "", visited = 0, atoms = 0;
    const seen = new Array(ex.nodes.length).fill(false);
    const write = (t, col) => { if (t.length > LIM.MaxBytes - out.length) throw synErr(col, "canonical expression exceeds byte limit"); out += t; };
    const colOf = n => (n.atom ? n.atom.col : 0);
    function node(index, parentPrec, depth) {
      if (index < 0 || index >= ex.nodes.length) throw synErr(1, "child or root index out of bounds");
      const n = ex.nodes[index];
      if (seen[index]) throw synErr(colOf(n), "node is reused or forms a cycle");
      seen[index] = true; visited++;
      const prec = precedence(n.kind);
      if (prec === 0) throw synErr(colOf(n), "invalid node kind");
      const wrap = prec < parentPrec;
      if (wrap) depth++;
      if (depth > LIM.MaxDepth) throw synErr(colOf(n), "expression exceeds nesting limit");
      if (wrap) write("(", colOf(n));
      body(n, prec, depth);
      if (wrap) write(")", colOf(n));
    }
    function body(n, prec, depth) {
      if (n.kind === K.Atom || n.kind === K.Pred) {
        atoms++;
        if (atoms > LIM.MaxAtoms) throw synErr(n.atom.col, "expression exceeds atom limit");
        if (n.kind === K.Pred) {
          if (upper(n.atom.field) !== "NEARBY" || n.atom.op !== "" || n.atom.values.length) throw synErr(n.atom.col, "invalid operand-free predicate");
          return write("NEARBY", n.atom.col);
        }
        return atom(n.atom);
      }
      if (n.kind === K.Not) {
        if (depth >= LIM.MaxDepth) throw synErr(1, "expression exceeds nesting limit");
        write("NOT ", 1);
        return node(n.left, prec, depth + 1);
      }
      node(n.left, prec, depth);
      write(n.kind === K.Or ? " OR " : " AND ", 1);
      node(n.right, prec, depth);
    }
    function checkText(text, quoted, col) {
      if (text.length > LIM.MaxBytes) throw synErr(col, "field or value exceeds byte limit");
      if (!quoted && text === "") throw synErr(col, "empty field or unquoted value");
      for (let i = 0; i < text.length; i++) {
        const c = text.charCodeAt(i);
        if (c < 32 || c > 126 || (!quoted && !bareByte(text[i]))) throw synErr(col, "invalid field or value byte");
      }
    }
    function atom(a) {
      checkText(a.field, false, a.col);
      const f = upper(a.field);
      if (f === "NOT" || f === "AND" || f === "OR") throw synErr(a.col, "Boolean keyword cannot be a field");
      if (f === "NEARBY") throw synErr(a.col, "NEARBY must be an operand-free predicate");
      if (!VALID_OPS.includes(a.op)) throw synErr(a.col, "invalid comparison operator");
      if (a.values.length === 0 || a.values.length > LIM.MaxValuesPerAtom) throw synErr(a.col, "invalid atom value count");
      write(f, a.col); write(a.op, a.col);
      a.values.forEach((v, i) => {
        checkText(v.text, v.quoted, v.col);
        if (i > 0) write(",", v.col);
        if (!v.quoted) write(upper(v.text), v.col);
        else { write('"', v.col); for (const ch of v.text) write(ch === '"' ? '""' : ch, v.col); write('"', v.col); }
      });
    }
    node(ex.root, 0, 0);
    if (visited !== ex.nodes.length) throw synErr(1, "expression has unreachable nodes");
    if (atoms !== ex.atoms) throw synErr(1, "expression atom count does not match tree");
    return out;
  }

  // ---------------------------------------------------------------- compile
  const FIELDS = ["BAND", "MODE", "SOURCE", "EVENT", "DXCALL", "DECALL", "DXCONT", "DECONT", "DXZONE", "DEZONE", "DXDXCC", "DEDXCC",
    "DXSTATE", "DESTATE", "DXGRID2", "DEGRID2", "FREQ", "SNR", "CONF", "PATH", "COMMENT"];
  const GEO = new Set(["DXCONT", "DECONT", "DXZONE", "DEZONE", "DXDXCC", "DEDXCC", "DXSTATE", "DESTATE", "DXGRID2", "DEGRID2"]);
  const ORDERED = new Set(["FREQ", "SNR", "DXZONE", "DEZONE"]);
  const INTEGRAL = new Set(["SNR", "DXZONE", "DEZONE", "DXDXCC", "DEDXCC"]);
  const ruleErr = (col, msg) => synErr(col, msg);

  // resolve: canonical CTY label -> ADIF (telnet edits); null = numeric only (defaults).
  function compileRule(source, resolve = null) {
    const syntax = parseExpr(source);
    const prog = { syntax, nodes: [], root: syntax.root, canonical: "", atomCount: syntax.atoms, valueCount: 0, rawValueCount: 0, needsPath: false, needsNearby: false };
    for (const node of syntax.nodes) {
      const c = { kind: node.kind, left: node.left, right: node.right, pred: null };
      prog.nodes.push(c);
      if (node.kind !== K.Atom && node.kind !== K.Pred) continue;
      const a = node.atom;
      if (a.field === "COMMENT") {
        for (const v of a.values) if (!v.quoted) { const start = v.col - 1; v.text = source.slice(start, start + v.text.length); }
      }
      prog.rawValueCount += a.values.length;
      c.pred = compilePredicate(a, resolve);
      prog.valueCount += a.values.length;
      prog.needsPath = prog.needsPath || c.pred.field === "PATH";
      prog.needsNearby = prog.needsNearby || c.pred.field === "NEARBY";
    }
    prog.canonical = canonical(syntax);
    return prog;
  }

  function compilePredicate(a, resolve) {
    if (!FIELDS.includes(a.field) && a.field !== "NEARBY") throw ruleErr(a.col, `unsupported field ${goQuote(a.field)}`);
    const p = { field: a.field, op: a.op, text: [], ints: [], freqs: [], calls: [], events: 0, eventNone: false };
    if (a.field === "NEARBY") {
      if (a.op !== "" || a.values.length) throw ruleErr(a.col, "NEARBY takes no operator or values");
      return p;
    }
    const ordered = a.op !== "=" && a.op !== "<>";
    if (ordered) {
      if (!ORDERED.has(a.field)) throw ruleErr(a.col, `${a.field} does not support operator ${a.op}`);
      if (a.values.length !== 1) throw ruleErr(a.col, `${a.field} ordered comparisons require one value`);
    }
    if (a.field === "BAND") expandMetaBands(a);
    // filter/rule_predicate.go supportsAllValues: every field before NEARBY except calls and COMMENT.
    const acceptsAll = !["DXCALL", "DECALL", "COMMENT"].includes(a.field);
    const sessionOperand = a.field === "DXCONT" || a.field === "DECONT" ? "MYCONT" : a.field === "DXDXCC" || a.field === "DEDXCC" ? "MYDXCC" : "";
    let hasAll = false;
    for (const v of a.values) {
      if (acceptsAll && !v.quoted && v.text === "ALL") {
        if (ordered) throw ruleErr(v.col, `${a.field}${a.op}ALL needs a number after ${a.op}. Use ${a.field}=ALL to match any value`);
        hasAll = true;
        continue;
      }
      if (sessionOperand !== "" && !v.quoted && v.text === sessionOperand) { p.operating = true; continue; }
      v.text = compileValue(p, v, ordered, resolve);
      v.quoted = p.field === "COMMENT" || (acceptsAll && v.text === "ALL");
    }
    if (hasAll) return { field: "ALLVALUES", op: a.op };
    return p;
  }
  // Only BAND atoms containing a fixed meta name are expanded and deduplicated.
  function expandMetaBands(a) {
    if (!a.values.some(v => metaBandMembers(v.text))) return;
    const out = [];
    for (const v of a.values) {
      let members = metaBandMembers(v.text);
      if (!members) {
        let member = v.text;
        if (v.quoted || v.text !== "ALL") {
          member = normalizeBand(v.text);
          if (!isValidBand(member)) throw ruleErr(v.col, `unsupported BAND value ${goQuote(v.text)}`);
        }
        members = [member];
      }
      for (const m of members) {
        if (out.some(e => e.text === m)) continue;
        if (out.length >= LIM.MaxValuesPerAtom) throw ruleErr(v.col, `expanded BAND selection exceeds value limit ${LIM.MaxValuesPerAtom}`);
        out.push({ text: m, quoted: false, col: v.col });
      }
    }
    a.values = out;
  }

  function freqValue(text) {
    const r = goParseFloat(text);
    let n = r.value;
    if (n === 0) n = 0; // normalize -0
    return { n, ok: !r.err && n >= 0 && !Number.isNaN(n) && isFinite(n) };
  }
  function compileFrequency(text, allowRange) {
    const one = freqValue(text);
    if (one.ok) return { low: one.n, high: one.n };
    if (allowRange) {
      for (let i = 1; i < text.length - 1; i++) {
        if (text[i] !== "-") continue;
        const lo = freqValue(text.slice(0, i)), hi = freqValue(text.slice(i + 1));
        if (lo.ok && hi.ok && lo.n <= hi.n) return { low: lo.n, high: hi.n };
      }
    }
    return { err: "expected a finite nonnegative number" + (allowRange ? " or inclusive low-high range" : "") };
  }

  function compileDXCC(v, resolve) {
    let r = goAtoi(v.text), number = r.value;
    if (r.err) {
      if (!resolve || /^[+-]?[0-9]+$/.test(v.text)) throw ruleErr(v.col, `expected a strictly integral value for ${goQuote(v.text)}: ${r.err}`);
      const res = resolve(v.text);
      if (res.err) throw ruleErr(v.col, `invalid DXCC value ${goQuote(v.text)}: ${res.err}`);
      number = res.value;
    }
    if (number <= 0) throw ruleErr(v.col, "DXCC must be a positive ADIF integer");
    return number;
  }
  function compileValue(p, v, ordered, resolve) {
    if (p.field === "DXDXCC" || p.field === "DEDXCC") {
      const n = compileDXCC(v, resolve);
      p.ints.push(n);
      return String(n);
    }
    if (p.field === "FREQ") {
      const f = compileFrequency(v.text, !ordered);
      if (f.err) throw ruleErr(v.col, `invalid FREQ value ${goQuote(v.text)}: ${f.err}`);
      p.freqs.push(f);
      const low = formatFloat(f.low);
      return f.low === f.high ? low : low + "-" + formatFloat(f.high);
    }
    if (INTEGRAL.has(p.field)) {
      const r = goAtoi(v.text);
      if (r.err) throw ruleErr(v.col, `expected a strictly integral value for ${goQuote(v.text)}: ${r.err}`);
      if ((p.field === "DXZONE" || p.field === "DEZONE") && !isSupportedZone(r.value)) throw ruleErr(v.col, "CQ zone must be 1..40");
      p.ints.push(r.value);
      return String(r.value);
    }
    let text = v.text;
    if (p.field !== "COMMENT") text = upper(text);
    if (p.field !== "COMMENT" && p.field !== "BAND" && p.field !== "MODE" && trimSpace(text) !== text) {
      throw ruleErr(v.col, `unsupported ${p.field} value ${goQuote(v.text)}`);
    }
    let valid = false;
    switch (p.field) {
      case "BAND": text = normalizeBand(text); valid = isValidBand(text); break;
      case "MODE": if (trimSpace(text) !== "") { text = canonicalModeForFilter(text); valid = isSupportedMode(text); } break;
      case "SOURCE": valid = isSupportedSource(text); break;
      case "EVENT":
        valid = text === "NONE" || isSupportedEvent(text);
        if (valid) { if (text === "NONE") p.eventNone = true; else p.events |= eventMask(text); }
        break;
      case "DXCALL": case "DECALL": valid = validCallsignPattern(text); break;
      case "DXCONT": case "DECONT": valid = isSupportedContinent(text); break;
      case "DXSTATE": case "DESTATE": valid = isState(text); break;
      case "DXGRID2": case "DEGRID2": valid = validGrid2(text); break;
      case "CONF": valid = isSupportedConf(text); break;
      case "PATH": valid = isSupportedPathClass(text); break;
      case "COMMENT": text = v.text; valid = validCommentPhrase(text); break;
    }
    if (!valid) throw ruleErr(v.col, `unsupported ${p.field} value ${goQuote(v.text)}`);
    if (p.field === "DXCALL" || p.field === "DECALL") {
      p.calls.push(text.endsWith("*") ? { k: 1, t: text.slice(0, -1) } : text.startsWith("*") ? { k: 2, t: text.slice(1) } : { k: 0, t: text });
    } else if (p.field !== "EVENT") p.text.push(text);
    return text;
  }

  // ---------------------------------------------------------------- spot view + evaluate
  // ctx: { pathClass, nearby(spot) -> bool }. NEARBY is computed lazily once per view.
  function newView(spot, ctx) { return { spot, pathClass: (ctx && ctx.pathClass) || "", ctx: ctx || {}, cache: {} }; }
  function nearbyValue(v) {
    if (!("nearby" in v)) v.nearby = !!(v.spot && v.ctx.nearby && v.ctx.nearby(v.spot));
    return v.nearby;
  }
  function textValue(v, field) {
    if (!(field in v.cache)) v.cache[field] = v.spot ? loadText(v, field) : "";
    return v.cache[field];
  }
  function loadText(v, field) {
    const s = v.spot;
    switch (field) {
      case "BAND": { let b = normalizeBand(s.bandNorm || ""); if (b === "") b = normalizeBand(s.band || ""); return isValidBand(b) ? b : ""; }
      case "MODE": { let m = s.modeNorm || ""; if (m === "") m = canonicalModeForFilter(s.mode || ""); if (m === "") m = "UNKNOWN"; m = canonicalModeForFilter(m); return isSupportedMode(m) ? m : ""; }
      case "SOURCE": return s.human ? "HUMAN" : "SKIMMER";
      case "DXCALL": return upper(s.dxCall || "");
      case "DECALL": return upper(s.deCallStripped || s.deCall || "");
      case "DXCONT": return contValue(s.dxCont);
      case "DECONT": return contValue(s.deCont);
      case "DXSTATE": return isState(s.dxState || "") ? s.dxState : "";
      case "DESTATE": return isState(s.deState || "") ? s.deState : "";
      case "DXGRID2": { const g = normalizeGrid2Token(s.dxGrid || ""); return validGrid2(g) ? g : ""; }
      case "DEGRID2": { const g = normalizeGrid2Token(s.deGrid || ""); return validGrid2(g) ? g : ""; }
      case "CONF": return CONF_EXEMPT[canonicalMode(textValue(v, "MODE"))] ? "" : normalizeConfidenceSymbol(s.conf || "");
      case "PATH": return normalizePathClass(v.pathClass || "") || "INSUFFICIENT";
      case "COMMENT": return s.comment || "";
    }
    return "";
  }
  const contValue = raw => { const n = normalizeUpper(raw || ""); return isSupportedContinent(n) ? n : ""; };
  function intValue(v, field) {
    const s = v.spot;
    if (!s) return { known: false };
    switch (field) {
      case "SNR": return { n: s.snr, known: s.snr !== undefined && s.snr !== null };
      case "DXZONE": return { n: s.dxZone, known: isSupportedZone(s.dxZone) };
      case "DEZONE": return { n: s.deZone, known: isSupportedZone(s.deZone) };
      case "DXDXCC": return { n: s.dxAdif, known: s.dxAdif > 0 };
      case "DEDXCC": return { n: s.deAdif, known: s.deAdif > 0 };
    }
    return { known: false };
  }
  const cmpInt = (val, opd, op) => op === "=" || op === "<>" ? val === opd : op === "<" ? val < opd : op === "<=" ? val <= opd : op === ">" ? val > opd : op === ">=" ? val >= opd : false;
  const cmpFreq = (val, r, op) => op === "=" || op === "<>" ? val >= r.low && val <= r.high : op === "<" ? val < r.low : op === "<=" ? val <= r.low : op === ">" ? val > r.low : op === ">=" ? val >= r.low : false;
  const spotEvents = s => (s.events || []).reduce((m, e) => m | eventMask(e), 0);

  function predMatches(p, v) {
    if (p.field === "ALLVALUES") return !!v.spot && p.op === "=";
    if (p.field === "NEARBY") return nearbyValue(v);
    const ne = p.op === "<>";
    const geo = (v.ctx && v.ctx.operating) || {};
    if (INTEGRAL.has(p.field)) {
      const { n, known } = intValue(v, p.field);
      if (!known) return false;
      if (p.operating) {
        const entity = geo.dxcc || 0;
        if (entity > 0 && n === entity) return p.op === "=";
        if (entity <= 0 && ne) return false;
      }
      for (const o of p.ints) if (cmpInt(n, o, p.op)) return !ne;
      return ne;
    }
    if (p.field === "FREQ") {
      const f = v.spot ? v.spot.freq : 0;
      if (!(f > 0 && isFinite(f))) return false;
      for (const o of p.freqs) if (cmpFreq(f, o, p.op)) return !ne;
      return ne;
    }
    if (p.field === "EVENT") {
      if (!v.spot) return false;
      const ev = spotEvents(v.spot);
      const matched = (ev & p.events) !== 0 || (p.eventNone && ev === 0);
      return ne ? !matched : matched;
    }
    const value = textValue(v, p.field);
    if (value === "") return false;
    if (p.operating) {
      const cont = geo.cont || "";
      if (value === cont) return p.op === "=";
      if (cont === "" && ne) return false;
    }
    if (p.field === "DXCALL" || p.field === "DECALL") {
      for (const c of p.calls) if (c.k === 1 ? value.startsWith(c.t) : c.k === 2 ? value.endsWith(c.t) : value === c.t) return !ne;
    } else if (p.field === "COMMENT") {
      for (const o of p.text) if (matchCommentPhrase(value, o)) return !ne;
    } else {
      for (const o of p.text) if (value === o) return !ne;
    }
    return ne;
  }
  function evaluate(prog, v, root) {
    const n = prog.nodes[root];
    switch (n.kind) {
      case K.Atom: case K.Pred: return predMatches(n.pred, v);
      case K.Not: return !evaluate(prog, v, n.left);
      case K.And: return evaluate(prog, v, n.left) && evaluate(prog, v, n.right);
      default: return evaluate(prog, v, n.left) || evaluate(prog, v, n.right);
    }
  }

  // ---------------------------------------------------------------- prepared rules
  function prepareRule(def, resolve) {
    let program;
    try { program = compileRule(def.expr, resolve); }
    catch (e) { throw new Error(`rule slot ${def.slot}: ${e.message}`); }
    // ADR-0320: an omitted SOURCE imposes no restriction (no implicit HUMAN guard).
    return { def: { slot: def.slot, action: def.action, expr: program.canonical }, program };
  }

  // A rule set is an immutable sorted array (null = empty/unrestricted).
  function validateTotals(atoms, values) {
    if (atoms > LIM.TotalAtoms) throw new Error(`rules exceed total atom limit ${LIM.TotalAtoms}`);
    if (values > LIM.TotalValues) throw new Error(`rules exceed total value limit ${LIM.TotalValues}`);
  }
  function withRule(set, def, resolve = null) {
    if (def.slot < 0 || def.slot >= LIM.Slots) throw new Error(`rule slot ${def.slot} must be 0..${LIM.Slots - 1}`);
    if (def.action !== "pass" && def.action !== "reject") throw new Error(`rule slot ${def.slot} action must be pass or reject`);
    const rule = prepareRule(def, resolve);
    const prev = set || [];
    let atoms = rule.program.atomCount, values = rule.program.valueCount, raw = rule.program.rawValueCount;
    for (const r of prev) if (r.def.slot !== def.slot) { atoms += r.program.atomCount; values += r.program.valueCount; raw += r.program.valueCount; }
    validateTotals(atoms, values);
    validateTotals(atoms, raw);
    const next = prev.filter(r => r.def.slot !== def.slot).concat([rule]).sort((a, b) => a.def.slot - b.def.slot);
    return next.length ? next : null;
  }
  function withoutRule(set, slot) {
    if (!set) return null;
    if (!set.some(r => r.def.slot === slot)) return set;
    const next = set.filter(r => r.def.slot !== slot);
    return next.length ? next : null;
  }
  // Returns {pass, by}: `by` names the deciding slot for the simulator's reason column.
  function decideRules(set, spot, ctx) {
    if (!set) return { pass: true, by: "no rules" };
    const v = newView(spot, ctx);
    for (const r of set) if (r.def.action === "reject" && evaluate(r.program, v, r.program.root)) return { pass: false, by: `REJECT ${r.def.slot}` };
    let hasPass = false;
    for (const r of set) if (r.def.action === "pass") {
      hasPass = true;
      if (evaluate(r.program, v, r.program.root)) return { pass: true, by: `PASS ${r.def.slot}` };
    }
    return hasPass ? { pass: false, by: "no PASS matched" } : { pass: true, by: "no PASS rules" };
  }
  const matches = (set, spot, ctx) => decideRules(set, spot, ctx).pass;

  // Only band predicates and constants are known without an actual spot.
  function bandTruth(prog, index, view) {
    const node = prog.nodes[index];
    if (node.pred) {
      const field = node.pred.field;
      return field === "BAND" || field === "ALLVALUES" ? predMatches(node.pred, view) : null;
    }
    const left = bandTruth(prog, node.left, view);
    if (node.kind === K.Not) return left === null ? null : !left;
    const shortCircuit = node.kind === K.Or;
    if (left === shortCircuit) return left;
    const right = bandTruth(prog, node.right, view);
    return right === shortCircuit || left !== null ? right : null;
  }

  function mayPassOnBand(set, band) {
    if (!set) return true;
    const view = newView({ band }, null);
    let possible = false, hasPass = false;
    for (const { def, program } of set) {
      const truth = bandTruth(program, program.root, view);
      if (def.action === "reject") {
        if (truth === true) return false;
      } else {
        hasPass = true;
        if (truth !== false) possible = true;
      }
    }
    return possible || !hasPass;
  }
  const needsPath = set => !!set && set.some(r => r.program.needsPath);
  const needsNearby = set => !!set && set.some(r => r.program.needsNearby);

  const readback = set => (set || []).map(r => ({ slot: r.def.slot, action: r.def.action, expr: r.def.expr }));
  function counts(set) {
    const c = { rules: 0, program: { nodes: 0, atoms: 0, values: 0 } };
    for (const r of set || []) { c.rules++; c.program.nodes += r.program.nodes.length; c.program.atoms += r.program.atomCount; c.program.values += r.program.valueCount; }
    return c;
  }

  function defaultRuleExpr(modes = DEFAULT_MODES, sources = DEFAULT_SOURCES) {
    let human = false, skimmer = false, all = false;
    for (const s of sources) {
      const c = upper(trimSpace(s));
      if (c === "") continue;
      if (c === "ALL") all = true; else if (c === "HUMAN") human = true; else if (c === "SKIMMER") skimmer = true;
      else throw new Error(`unsupported default source ${goQuote(c)}`);
    }
    const clause = all || human === skimmer ? " AND SOURCE=HUMAN,SKIMMER" : human ? " AND SOURCE=HUMAN" : " AND SOURCE=SKIMMER";
    const selected = [];
    for (const m of modes) { let c = canonicalModeForFilter(m); if (c === "ALL") c = '"ALL"'; if (!selected.includes(c)) selected.push(c); }
    return "MODE=" + selected.join(",") + clause;
  }
  const defaultRuleSet = () => withRule(null, { slot: 0, action: "pass", expr: defaultRuleExpr() });

  // ---------------------------------------------------------------- CTY canonical labels (cty/dxcc.go)
  const CTY = new Map();
  function setCTYTable(rows) { CTY.clear(); for (const [label, adif] of rows) CTY.set(label, adif); }
  function ctyResolve(label) {
    const key = normalizeUpper(label);
    if (!CTY.has(key)) return { err: "unknown DXCC canonical prefix" };
    const adif = CTY.get(key);
    return adif === 0 ? { err: "conflicting DXCC canonical prefix" } : { value: adif };
  }

  // ---------------------------------------------------------------- NEARBY (filter.matchesNearbyCells)
  const BAND_TABLE = [["2200m", 135.7, 137.8], ["630m", 472, 479], ["160m", 1800, 2000], ["80m", 3500, 4000], ["60m", 5260, 5405],
    ["40m", 7000, 7300], ["30m", 10100, 10150], ["20m", 14000, 14350], ["17m", 18068, 18168], ["15m", 21000, 21450], ["12m", 24890, 24990],
    ["10m", 28000, 29700], ["6m", 50000, 54000], ["4m", 70000, 70650], ["2m", 144000, 148000], ["1.25m", 220000, 225000], ["70cm", 420000, 450000],
    ["33cm", 902000, 928000], ["23cm", 1240000, 1300000], ["13cm", 2300000, 2310000], ["13cm", 2390000, 2450000]];
  function freqToBand(f) { for (const [n, lo, hi] of BAND_TABLE) if (f >= lo && f <= hi) return n; return "???"; }
  // encode(grid, coarse) -> positive cell id or 0. fine/coarse are the user's cells (0 = unavailable).
  function nearbyCells(spot, fine, coarse, encode) {
    if (!spot) return false;
    let band = normalizeBand(spot.bandNorm || "");
    if (band === "") band = normalizeBand(spot.band || "");
    if (band === "" || band === "???") band = normalizeBand(freqToBand(spot.freq || 0));
    if (band === "" || band === "???") return false;
    const useCoarse = band === "160m" || band === "80m" || band === "60m";
    const user = useCoarse ? coarse : fine;
    if (!user) return false;
    return user === encode(trimSpace(spot.dxGrid || ""), useCoarse) || user === encode(trimSpace(spot.deGrid || ""), useCoarse);
  }

  // pathreliability.EncodeCell/EncodeCoarseCell. The table holds the server's ids for
  // every 4-character square; a 6-character locator uses its square (approximation).
  let CELLS = null;
  const setCellTable = t => { CELLS = t; };
  function validGrid(grid) {
    const g = normalizeUpper(grid || "");
    if (g.length < 4 || g.length === 5) return "";
    if (!/^[A-R]{2}[0-9]{2}/.test(g)) return "";
    if (g.length >= 6 && !/^[A-X]{2}$/.test(g.slice(4, 6))) return "";
    return g;
  }
  function encodeGrid(grid, coarse) {
    const g = validGrid(grid);
    if (!g || !CELLS) return 0;
    const idx = (((g.charCodeAt(0) - 65) * 18 + (g.charCodeAt(1) - 65)) * 10 + (g.charCodeAt(2) - 48)) * 10 + (g.charCodeAt(3) - 48);
    return CELLS[2 * idx + (coarse ? 1 : 0)];
  }

  // ---------------------------------------------------------------- native CC bank (filter/cc_*.go)
  const CC_BANDS = ["160", "80", "60", "40", "30", "20", "17", "15", "12", "10", "6", "4", "2", "1", "MW"];
  const CC_SLOTS = [["160-CW", "160", 1800, 1850], ["160-SSB", "160", 1850, 2000], ["80-CW", "80", 3500, 3580], ["80-RTTY", "80", 3580, 3700],
    ["80-SSB", "80", 3700, 4000], ["60-SSB", "60", 5260, 5405], ["40-CW", "40", 7000, 7040], ["40-RTTY", "40", 7040, 7100], ["40-SSB", "40", 7100, 7300],
    ["30-CW", "30", 10100, 10130], ["30-RTTY", "30", 10130, 10150], ["20-CW", "20", 14000, 14070], ["20-RTTY", "20", 14070, 14150], ["20-SSB", "20", 14150, 14350],
    ["17-CW", "17", 18068, 18100], ["17-RTTY", "17", 18100, 18110], ["17-SSB", "17", 18110, 18168], ["15-CW", "15", 21000, 21070], ["15-RTTY", "15", 21070, 21200],
    ["15-SSB", "15", 21200, 21450], ["12-CW", "12", 24890, 24920], ["12-RTTY", "12", 24920, 24930], ["12-SSB", "12", 24930, 24990],
    ["10-CW", "10", 28000, 28070], ["10-RTTY", "10", 28070, 28300], ["10-SSB", "10", 28300, 29700], ["6-CW", "6", 50000, 50080], ["6-SSB", "6", 50080, 50500],
    ["6-FM", "6", 50500, 54000], ["4-MTR", "4", 70000, 70650], ["2-CW", "2", 144000, 144100], ["2-SSB", "2", 144100, 144500], ["2-FM", "2", 144500, 148000],
    ["1-CW", "1", 220000, 221000], ["1-FM", "1", 221000, 222000], ["1-SSB", "1", 222000, 224000], ["MW-MW", "MW", 500000, 47000000]];
  const CC_GEO = ["DOC", "DOS", "DXCTY", "DXSTATE"];
  // Operator taxonomy cc_shortcut modes (data/config/spot_taxonomy.yaml).
  const CC_SHORTCUT_MODES = ["CW", "RTTY", "FT8", "FT4", "FT2", "PSK"];
  const ccSlotIndex = name => CC_SLOTS.findIndex(s => s[0] === name);
  function ccFrequencySlot(f) {
    if (!(f > 0) || !isFinite(f)) return -1;
    let index = -1;
    for (let i = 0; i < CC_SLOTS.length; i++) if (CC_SLOTS[i][2] <= f) index = i;
    if (index < 0 || f > CC_SLOTS[index][3]) return -1;
    return index;
  }
  // filter/cc_frequency_cells.go: catalog-derived exact points and open intervals.
  const CC_ENDPOINTS = (() => {
    const e = [];
    for (const sl of CC_SLOTS) e.push(sl[2], sl[3]);
    for (const [, lo, hi] of BAND_TABLE) e.push(lo, hi);
    return Array.from(new Set(e)).sort((a, b) => a - b);
  })();
  const CC_CELLS = (() => {
    const cells = [];
    CC_ENDPOINTS.forEach((x, i) => {
      if (i > 0) { const lo = CC_ENDPOINTS[i - 1]; cells.push({ id: `I:${lo}:${x}`, low: lo, high: x, point: false }); }
      cells.push({ id: `P:${x}`, low: x, high: x, point: true });
    });
    return cells;
  })();
  const cellProbe = c => (c.point ? c.low : c.low + (c.high - c.low) / 2);
  function ccFrequencyCellIndex(f) {
    if (!(f > 0) || !isFinite(f)) return -1;
    let i = 0;
    while (i < CC_ENDPOINTS.length && CC_ENDPOINTS[i] < f) i++;
    if (i < CC_ENDPOINTS.length && CC_ENDPOINTS[i] === f) return i * 2;
    if (i === 0 || i === CC_ENDPOINTS.length) return -1;
    return i * 2 - 1;
  }
  const bandContains = (name, f) => BAND_TABLE.some(([n, lo, hi]) => n === name && f >= lo && f <= hi);
  function ccNativeDecision(d, f) {
    const slot = ccFrequencySlot(f);
    if (slot >= 0 && CC_SLOTS[slot][0] in d.slots) return d.slots[CC_SLOTS[slot][0]];
    return d.baseAllow;
  }
  function clearSlotOverrides(d, slot) {
    for (const c of CC_CELLS) if (ccFrequencySlot(cellProbe(c)) === slot) delete d.overrides[c.id];
  }
  const sortStrings = a => Array.from(new Set(a)).sort();
  const sortInts = a => Array.from(new Set(a)).sort((x, y) => x - y);
  function ccClone(c) {
    const geo = {};
    for (const n of CC_GEO) geo[n] = { action: c.geo[n].action, values: c.geo[n].values.slice() };
    return { modes: c.modes.slice(), sources: c.sources.slice(), geo, dxbm: { active: c.dxbm.active, baseAllow: c.dxbm.baseAllow, slots: Object.assign({}, c.dxbm.slots), overrides: Object.assign({}, c.dxbm.overrides || {}) } };
  }
  function ccNormalizeDXBM(c) {
    if (!c.dxbm.active) { c.dxbm.baseAllow = true; c.dxbm.slots = {}; c.dxbm.overrides = {}; return c; }
    for (const k of Object.keys(c.dxbm.slots)) if (c.dxbm.slots[k] === c.dxbm.baseAllow) delete c.dxbm.slots[k];
    // A false override may equal BaseAllow yet veto an enabled native slot.
    for (const c2 of CC_CELLS) {
      if (!(c2.id in c.dxbm.overrides)) continue;
      if (c.dxbm.overrides[c2.id] === ccNativeDecision(c.dxbm, cellProbe(c2))) delete c.dxbm.overrides[c2.id];
    }
    return c;
  }
  function ccDefault(modes = DEFAULT_MODES, sources = DEFAULT_SOURCES) {
    const geo = {};
    for (const n of CC_GEO) geo[n] = { action: "off", values: [] };
    return { modes: sortStrings(modes.map(canonicalModeForFilter)), sources: sortStrings(sources.map(normalizeUpper)), geo, dxbm: { active: false, baseAllow: true, slots: {}, overrides: {} } };
  }
  function ccWithMode(c, mode, enabled) {
    if (trimSpace(mode) === "") throw new Error("native CC mode must not be empty");
    const m = canonicalModeForFilter(mode);
    if (!isSupportedMode(m)) throw new Error(`unsupported native CC mode ${goQuote(mode)}`);
    const n = ccClone(c);
    n.modes = enabled ? sortStrings(n.modes.concat(m)) : n.modes.filter(x => x !== m);
    return n;
  }
  function ccWithSource(c, source, enabled) {
    const s = normalizeUpper(source);
    if (!SOURCES.includes(s)) throw new Error(`unsupported native CC source ${goQuote(source)}`);
    const n = ccClone(c);
    n.sources = enabled ? sortStrings(n.sources.concat(s)) : n.sources.filter(x => x !== s);
    return n;
  }
  // values: ADIF integers (DOC/DXCTY) or state codes (DOS/DXSTATE); action off|pass|reject.
  function ccWithGeography(c, category, action, values) {
    const n = ccClone(c);
    const country = category === "DOC" || category === "DXCTY";
    n.geo[category] = { action, values: action === "off" ? [] : country ? sortInts(values) : sortStrings(values.map(upper)) };
    return n;
  }
  function ccWithDXBM(c, action, selectors) {
    const n = ccClone(c);
    if (action === "off") {
      if (selectors.length) throw new Error("native CC DXBM OFF takes no selectors");
      n.dxbm = { active: false, baseAllow: true, slots: {}, overrides: {} };
      return n;
    }
    if ((action !== "pass" && action !== "reject") || !selectors.length) throw new Error("native CC DXBM requires pass/reject and at least one selector");
    if (!n.dxbm.active) { n.dxbm.active = true; n.dxbm.baseAllow = action === "reject"; }
    for (const raw of selectors) {
      if (raw.length > 16) throw new Error("unsupported native CC DXBM selector");
      const name = upper(raw), enabled = action === "pass";
      const members = metaBandMembers(name);
      if (members) {
        for (const c of CC_CELLS) if (members.some(m => bandContains(m, cellProbe(c)))) n.dxbm.overrides[c.id] = enabled;
        continue;
      }
      const index = ccSlotIndex(name);
      if (index >= 0) { clearSlotOverrides(n.dxbm, index); n.dxbm.slots[name] = enabled; continue; }
      if (!CC_BANDS.includes(name)) throw new Error(`unsupported native CC DXBM selector ${goQuote(raw)}`);
      CC_SLOTS.forEach((s, i) => { if (s[1] === name) { clearSlotOverrides(n.dxbm, i); n.dxbm.slots[s[0]] = enabled; } });
    }
    return ccNormalizeDXBM(n);
  }
  // PreparedCCFilter.Matches with a deciding reason for the simulator.
  function ccDecide(c, spot) {
    if (!spot) return { pass: false, by: "no spot" };
    if (spot.human ? !c.sources.includes("HUMAN") : !c.sources.includes("SKIMMER")) return { pass: false, by: "CC SOURCE" };
    const v = newView(spot, null);
    if (!c.modes.includes(textValue(v, "MODE"))) return { pass: false, by: "CC MODE" };
    if (c.dxbm.active) {
      const slot = ccFrequencySlot(spot.freq);
      let allowed = c.dxbm.baseAllow;
      if (slot >= 0) { const name = CC_SLOTS[slot][0]; allowed = name in c.dxbm.slots ? c.dxbm.slots[name] : c.dxbm.baseAllow; }
      const ov = c.dxbm.overrides || {};
      if (Object.keys(ov).length) {
        const cell = ccFrequencyCellIndex(spot.freq);
        if (cell >= 0) { const cc = CC_CELLS[cell]; allowed = cc.id in ov ? ov[cc.id] : ccNativeDecision(c.dxbm, cellProbe(cc)); }
      }
      if (!allowed) return { pass: false, by: "CC DXBM" };
    }
    for (const name of CC_GEO) {
      const g = c.geo[name];
      if (g.action === "off") continue;
      let matched = false;
      if (name === "DOC" || name === "DXCTY") {
        const r = intValue(v, name === "DOC" ? "DEDXCC" : "DXDXCC");
        if (r.known) matched = g.values.includes(r.n);
      } else {
        const st = textValue(v, name === "DOS" ? "DESTATE" : "DXSTATE");
        if (st !== "") matched = g.values.includes(st);
      }
      if ((g.action === "pass") !== matched) return { pass: false, by: `CC ${name}` };
    }
    return { pass: true, by: "CC selections" };
  }
  const ccMatches = (c, spot) => ccDecide(c, spot).pass;

  return {
    BANDS, FILTER_MODES, EVENTS, SOURCES, CONTINENTS, CONF_SYMBOLS, PATH_CLASSES, STATES, FIELDS, LIM, GEO,
    parseExpr, canonical, compileRule, withRule, withoutRule, matches, decideRules, mayPassOnBand, needsPath, needsNearby, readback, counts,
    defaultRuleExpr, defaultRuleSet, normalizeBand, upper, goQuote, isState, goAtoi, canonicalModeForFilter, isSupportedMode,
    setCTYTable, ctyResolve, freqToBand, nearbyCells, setCellTable, validGrid, encodeGrid,
    CC_BANDS, CC_SLOTS, CC_GEO, CC_SHORTCUT_MODES, ccDefault, ccWithMode, ccWithSource, ccWithGeography, ccWithDXBM, ccDecide, ccMatches, ccFrequencySlot, CC_CELLS, META_BANDS, metaBandMembers,
  };
})();

if (typeof module !== "undefined") module.exports = { GC };

const CTY_CANONICAL = [["1A",246,"Sov Mil Order of Malta"],["1S",247,"Spratly Islands"],["3A",260,"Monaco"],["3B6",4,"Agalega & St. Brandon"],["3B8",165,"Mauritius"],["3B9",207,"Rodriguez Island"],["3C",49,"Equatorial Guinea"],["3C0",195,"Annobon Island"],["3D2",176,"Fiji"],["3D2/C",489,"Conway Reef"],["3D2/R",460,"Rotuma Island"],["3DA",468,"Kingdom of Eswatini"],["3V",474,"Tunisia"],["3W",293,"Vietnam"],["3X",107,"Guinea"],["3Y/B",24,"Bouvet"],["3Y/P",199,"Peter 1 Island"],["4J",18,"Azerbaijan"],["4L",75,"Georgia"],["4O",514,"Montenegro"],["4S",315,"Sri Lanka"],["4U1I",117,"ITU HQ"],["4U1U",289,"United Nations HQ"],["4W",511,"Timor - Leste"],["4X",336,"Israel"],["5A",436,"Libya"],["5B",215,"Cyprus"],["5H",470,"Tanzania"],["5N",450,"Nigeria"],["5R",438,"Madagascar"],["5T",444,"Mauritania"],["5U",187,"Niger"],["5V",483,"Togo"],["5W",190,"Samoa"],["5X",286,"Uganda"],["5Z",430,"Kenya"],["6W",456,"Senegal"],["6Y",82,"Jamaica"],["7O",492,"Yemen"],["7P",432,"Lesotho"],["7Q",440,"Malawi"],["7X",400,"Algeria"],["8P",62,"Barbados"],["8Q",159,"Maldives"],["8R",129,"Guyana"],["9A",497,"Croatia"],["9G",424,"Ghana"],["9H",257,"Malta"],["9J",482,"Zambia"],["9K",348,"Kuwait"],["9L",458,"Sierra Leone"],["9M2",299,"West Malaysia"],["9M6",46,"East Malaysia"],["9N",369,"Nepal"],["9Q",414,"Dem. Rep. of the Congo"],["9U",404,"Burundi"],["9V",381,"Singapore"],["9X",454,"Rwanda"],["9Y",90,"Trinidad & Tobago"],["A2",402,"Botswana"],["A3",160,"Tonga"],["A4",370,"Oman"],["A5",306,"Bhutan"],["A6",391,"United Arab Emirates"],["A7",376,"Qatar"],["A9",304,"Bahrain"],["AP",372,"Pakistan"],["BS7",506,"Scarborough Reef"],["BV",386,"Taiwan"],["BV9P",505,"Pratas Island"],["BY",318,"China"],["C2",157,"Nauru"],["C3",203,"Andorra"],["C5",422,"The Gambia"],["C6",60,"Bahamas"],["C9",181,"Mozambique"],["CE",112,"Chile"],["CE0X",217,"San Felix & San Ambrosio"],["CE0Y",47,"Easter Island"],["CE0Z",125,"Juan Fernandez Islands"],["CE9",13,"Antarctica"],["CM",70,"Cuba"],["CN",446,"Morocco"],["CP",104,"Bolivia"],["CT",272,"Portugal"],["CT3",256,"Madeira Islands"],["CU",149,"Azores"],["CX",144,"Uruguay"],["CY0",211,"Sable Island"],["CY9",252,"St. Paul Island"],["D2",401,"Angola"],["D4",409,"Cabo Verde"],["D6",411,"Comoros"],["DL",230,"Fed. Rep. of Germany"],["DU",375,"Philippines"],["E3",51,"Eritrea"],["E4",510,"Palestine"],["E5/N",191,"North Cook Islands"],["E5/S",234,"South Cook Islands"],["E6",188,"Niue"],["E7",501,"Bosnia-Herzegovina"],["EA",281,"Spain"],["EA6",21,"Balearic Islands"],["EA8",29,"Canary Islands"],["EA9",32,"Ceuta & Melilla"],["EI",245,"Ireland"],["EK",14,"Armenia"],["EL",434,"Liberia"],["EP",330,"Iran"],["ER",179,"Moldova"],["ES",52,"Estonia"],["ET",53,"Ethiopia"],["EU",27,"Belarus"],["EX",135,"Kyrgyzstan"],["EY",262,"Tajikistan"],["EZ",280,"Turkmenistan"],["F",227,"France"],["FG",79,"Guadeloupe"],["FH",169,"Mayotte"],["FJ",516,"St. Barthelemy"],["FK",162,"New Caledonia"],["FK/C",512,"Chesterfield Islands"],["FM",84,"Martinique"],["FO",175,"French Polynesia"],["FO/A",508,"Austral Islands"],["FO/C",36,"Clipperton Island"],["FO/M",509,"Marquesas Islands"],["FP",277,"St. Pierre & Miquelon"],["FR",453,"Reunion Island"],["FS",213,"St. Martin"],["FT/G",99,"Glorioso Islands"],["FT/J",124,"Juan de Nova, Europa"],["FT/T",276,"Tromelin Island"],["FT/W",41,"Crozet Island"],["FT/X",131,"Kerguelen Islands"],["FT/Z",10,"Amsterdam & St. Paul Is."],["FW",298,"Wallis & Futuna Islands"],["FY",63,"French Guiana"],["G",223,"England"],["GD",114,"Isle of Man"],["GI",265,"Northern Ireland"],["GJ",122,"Jersey"],["GM",279,"Scotland"],["GM/S",279,"Shetland Islands"],["GU",106,"Guernsey"],["GW",294,"Wales"],["H4",185,"Solomon Islands"],["H40",507,"Temotu Province"],["HA",239,"Hungary"],["HB",287,"Switzerland"],["HB0",251,"Liechtenstein"],["HC",120,"Ecuador"],["HC8",71,"Galapagos Islands"],["HH",78,"Haiti"],["HI",72,"Dominican Republic"],["HK",116,"Colombia"],["HK0/A",216,"San Andres & Providencia"],["HK0/M",161,"Malpelo Island"],["HL",137,"Republic of Korea"],["HP",88,"Panama"],["HR",80,"Honduras"],["HS",387,"Thailand"],["HV",295,"Vatican City"],["HZ",378,"Saudi Arabia"],["I",248,"Italy"],["IG9",248,"African Italy"],["IS",225,"Sardinia"],["IT9",248,"Sicily"],["J2",382,"Djibouti"],["J3",77,"Grenada"],["J5",109,"Guinea-Bissau"],["J6",97,"St. Lucia"],["J7",95,"Dominica"],["J8",98,"St. Vincent"],["JA",339,"Japan"],["JD/M",177,"Minami Torishima"],["JD/O",192,"Ogasawara"],["JT",363,"Mongolia"],["JW",259,"Svalbard"],["JW/B",259,"Bear Island"],["JX",118,"Jan Mayen"],["JY",342,"Jordan"],["K",291,"United States"],["KG4",105,"Guantanamo Bay"],["KH0",166,"Mariana Islands"],["KH1",20,"Baker & Howland Islands"],["KH2",103,"Guam"],["KH3",123,"Johnston Island"],["KH4",174,"Midway Island"],["KH5",197,"Palmyra & Jarvis Islands"],["KH6",110,"Hawaii"],["KH7K",138,"Kure Island"],["KH8",9,"American Samoa"],["KH8/S",515,"Swains Island"],["KH9",297,"Wake Island"],["KL",6,"Alaska"],["KP1",182,"Navassa Island"],["KP2",285,"US Virgin Islands"],["KP4",202,"Puerto Rico"],["KP5",43,"Desecheo Island"],["LA",266,"Norway"],["LU",100,"Argentina"],["LX",254,"Luxembourg"],["LY",146,"Lithuania"],["LZ",212,"Bulgaria"],["OA",136,"Peru"],["OD",354,"Lebanon"],["OE",206,"Austria"],["OH",224,"Finland"],["OH0",5,"Aland Islands"],["OJ0",167,"Market Reef"],["OK",503,"Czech Republic"],["OM",504,"Slovak Republic"],["ON",209,"Belgium"],["OX",237,"Greenland"],["OY",222,"Faroe Islands"],["OZ",221,"Denmark"],["P2",163,"Papua New Guinea"],["P4",91,"Aruba"],["P5",344,"DPR of Korea"],["PA",263,"Netherlands"],["PJ2",517,"Curacao"],["PJ4",520,"Bonaire"],["PJ5",519,"Saba & St. Eustatius"],["PJ7",518,"Sint Maarten"],["PY",108,"Brazil"],["PY0F",56,"Fernando de Noronha"],["PY0S",253,"St. Peter & St. Paul"],["PY0T",273,"Trindade & Martim Vaz"],["PZ",140,"Suriname"],["R1FJ",61,"Franz Josef Land"],["S0",302,"Western Sahara"],["S2",305,"Bangladesh"],["S5",499,"Slovenia"],["S7",379,"Seychelles"],["S9",219,"Sao Tome & Principe"],["SM",284,"Sweden"],["SP",269,"Poland"],["ST",466,"Sudan"],["SU",478,"Egypt"],["SV",236,"Greece"],["SV/A",180,"Mount Athos"],["SV5",45,"Dodecanese"],["SV9",40,"Crete"],["T2",282,"Tuvalu"],["T30",301,"Western Kiribati"],["T31",31,"Central Kiribati"],["T32",48,"Eastern Kiribati"],["T33",490,"Banaba Island"],["T5",232,"Somalia"],["T7",278,"San Marino"],["T8",22,"Palau"],["TA",390,"Asiatic Turkey"],["TA1",390,"European Turkey"],["TF",242,"Iceland"],["TG",76,"Guatemala"],["TI",308,"Costa Rica"],["TI9",37,"Cocos Island"],["TJ",406,"Cameroon"],["TK",214,"Corsica"],["TL",408,"Central African Republic"],["TN",412,"Republic of the Congo"],["TR",420,"Gabon"],["TT",410,"Chad"],["TU",428,"Cote d'Ivoire"],["TY",416,"Benin"],["TZ",442,"Mali"],["UA",54,"European Russia"],["UA2",126,"Kaliningrad"],["UA9",15,"Asiatic Russia"],["UK",292,"Uzbekistan"],["UN",130,"Kazakhstan"],["UR",288,"Ukraine"],["V2",94,"Antigua & Barbuda"],["V3",66,"Belize"],["V4",249,"St. Kitts & Nevis"],["V5",464,"Namibia"],["V6",173,"Micronesia"],["V7",168,"Marshall Islands"],["V8",345,"Brunei Darussalam"],["VE",1,"Canada"],["VK",150,"Australia"],["VK0H",111,"Heard Island"],["VK0M",153,"Macquarie Island"],["VK9C",38,"Cocos (Keeling) Islands"],["VK9L",147,"Lord Howe Island"],["VK9M",171,"Mellish Reef"],["VK9N",189,"Norfolk Island"],["VK9W",303,"Willis Island"],["VK9X",35,"Christmas Island"],["VP2E",12,"Anguilla"],["VP2M",96,"Montserrat"],["VP2V",65,"British Virgin Islands"],["VP5",89,"Turks & Caicos Islands"],["VP6",172,"Pitcairn Island"],["VP6/D",513,"Ducie Island"],["VP8",141,"Falkland Islands"],["VP8/G",235,"South Georgia Island"],["VP8/H",241,"South Shetland Islands"],["VP8/O",238,"South Orkney Islands"],["VP8/S",240,"South Sandwich Islands"],["VP9",64,"Bermuda"],["VQ9",33,"Chagos Islands"],["VR",321,"Hong Kong"],["VU",324,"India"],["VU4",11,"Andaman & Nicobar Is."],["VU7",142,"Lakshadweep Islands"],["XE",50,"Mexico"],["XF4",204,"Revillagigedo"],["XT",480,"Burkina Faso"],["XU",312,"Cambodia"],["XW",143,"Laos"],["XX9",152,"Macao"],["XZ",309,"Myanmar"],["YA",3,"Afghanistan"],["YB",327,"Indonesia"],["YI",333,"Iraq"],["YJ",158,"Vanuatu"],["YK",384,"Syria"],["YL",145,"Latvia"],["YN",86,"Nicaragua"],["YO",275,"Romania"],["YS",74,"El Salvador"],["YU",296,"Serbia"],["YV",148,"Venezuela"],["YV0",17,"Aves Island"],["Z2",452,"Zimbabwe"],["Z3",502,"North Macedonia"],["Z6",522,"Republic of Kosovo"],["Z8",521,"Republic of South Sudan"],["ZA",7,"Albania"],["ZB",233,"Gibraltar"],["ZC4",283,"UK Base Areas on Cyprus"],["ZD7",250,"St. Helena"],["ZD8",205,"Ascension Island"],["ZD9",274,"Tristan da Cunha & Gough Islands"],["ZF",69,"Cayman Islands"],["ZK3",270,"Tokelau Islands"],["ZL",170,"New Zealand"],["ZL7",34,"Chatham Islands"],["ZL8",133,"Kermadec Islands"],["ZL9",16,"N.Z. Subantarctic Is."],["ZP",132,"Paraguay"],["ZS",462,"South Africa"],["ZS8",201,"Pr. Edward & Marion Is."]];

// GoCluster UI — domain reference data.
// Vocabularies mirror GoCluster main: spot/bands.go, data/config/spot_taxonomy.yaml,
// filter/filter.go, data/config/path_reliability.yaml (noise_offsets) and
// data/config/dedupe.yaml (secondary_*_window_seconds). The entity table is a
// small sample: the console uses it only for the rule editor's entity picker and
// to guess the login's own continent for the in-browser rule preview. Live spots
// carry their own country, continent and zones from the node.
"use strict";

const DATA = (() => {
  const CONTINENTS = [
    ["NA", "North America"], ["SA", "South America"], ["EU", "Europe"],
    ["AF", "Africa"], ["AS", "Asia"], ["OC", "Oceania"], ["AN", "Antarctica"],
  ];
  const CONT_NAME = Object.fromEntries(CONTINENTS);

  const BAND_PLAN = {
    "160m": [1800, 2000], "80m": [3500, 4000], "60m": [5330, 5405], "40m": [7000, 7300], "30m": [10100, 10150],
    "20m": [14000, 14350], "17m": [18068, 18168], "15m": [21000, 21450], "12m": [24890, 24990], "10m": [28000, 29700],
    "6m": [50000, 54000], "2m": [144000, 148000],
  };
  // Display order for the band strip and openings matrix.
  const MAIN_BANDS = ["160m", "80m", "60m", "40m", "30m", "20m", "17m", "15m", "12m", "10m", "6m", "2m"];

  // Rough IARU-style segments used for band map shading (display only).
  const SEGMENTS = {
    "160m": [[1800, 1840, "cw"], [1840, 1843, "data"], [1843, 2000, "phone"]],
    "80m": [[3500, 3570, "cw"], [3570, 3600, "data"], [3600, 4000, "phone"]],
    "60m": [[5330, 5405, "phone"]],
    "40m": [[7000, 7040, "cw"], [7040, 7080, "data"], [7080, 7300, "phone"]],
    "30m": [[10100, 10130, "cw"], [10130, 10150, "data"]],
    "20m": [[14000, 14070, "cw"], [14070, 14100, "data"], [14100, 14350, "phone"]],
    "17m": [[18068, 18095, "cw"], [18095, 18110, "data"], [18110, 18168, "phone"]],
    "15m": [[21000, 21070, "cw"], [21070, 21150, "data"], [21150, 21450, "phone"]],
    "12m": [[24890, 24915, "cw"], [24915, 24930, "data"], [24930, 24990, "phone"]],
    "10m": [[28000, 28070, "cw"], [28070, 28300, "data"], [28300, 29700, "phone"]],
    "6m": [[50000, 50100, "cw"], [50100, 50500, "phone"], [50500, 54000, "data"]],
    "2m": [[144000, 144150, "cw"], [144150, 144400, "phone"], [144400, 148000, "data"]],
  };

  const META_BANDS = [
    ["HF", "All HF", ["160m", "80m", "60m", "40m", "30m", "20m", "17m", "15m", "12m", "10m"]],
    ["HFCONTEST", "Contest bands", ["160m", "80m", "40m", "20m", "15m", "10m"]],
    ["WARC", "WARC", ["30m", "17m", "12m"]],
    ["VHF", "VHF", ["6m", "4m", "2m", "1.25m"]],
    ["UHF", "UHF", ["70cm", "33cm", "23cm", "13cm"]],
  ];

  const MODES = ["CW", "RTTY", "FT8", "FT4", "FT2", "USB", "LSB", "PSK", "JS8", "MSK144", "SSTV", "UNKNOWN"];
  const MODE_FAMILY = { CW: "cw", RTTY: "rtty", FT8: "ft", FT4: "ft", FT2: "ft", JS8: "ft", PSK: "psk", MSK144: "psk", USB: "ssb", LSB: "ssb", SSTV: "ssb", UNKNOWN: "unk" };

  const CONF = {
    V: ["Verified", "Strongly corroborated by many reports."],
    P: ["Probable", "Corroborated, below the strongest level."],
    S: ["Single", "One current report with on-band support for the call."],
    "?": ["Unconfirmed", "Little supporting evidence yet."],
    C: ["Corrected", "The call was corrected and the new call passed validation."],
    B: ["Busted?", "A suggested correction failed validation; the original call was kept."],
  };
  const PATH = {
    HIGH: [">", "Favorable", 4], MEDIUM: ["=", "Workable", 3], LOW: ["<", "Weak", 2],
    UNLIKELY: ["-", "Poor", 1], CLOSED: ["#", "Closed", 0], INSUFFICIENT: [" ", "Not rated", -1],
  };

  // path_reliability.yaml noise_offsets (dB subtracted from DX->user evidence).
  const NOISE = [
    ["QUIET", "Quiet", 0, "Remote site, very low noise floor"],
    ["RURAL", "Rural", 4, "Countryside, few neighbors"],
    ["SUBURBAN", "Suburban", 12, "Typical residential lot"],
    ["URBAN", "Urban", 17, "City, many nearby devices"],
    ["INDUSTRIAL", "Industrial", 20, "Heavy local interference"],
  ];
  // dedupe.yaml shipped windows.
  const DEDUPE = [
    ["FAST", "Fast", 179, "Most repeats. Groups reports by spotter country and 2-letter grid field."],
    ["MED", "Medium", 359, "Fewer repeats. Same grouping, longer window."],
    ["SLOW", "Slow", 479, "Fewest repeats. Groups spotters by CQ zone, so one region counts once."],
  ];
  const DIAG = [
    ["OFF", "Off", "Normal comments"], ["SOURCE", "Source", "Which feed reported the spot"],
    ["DEDUPE", "Dedupe", "Repeat-suppression keys"], ["CONF", "Confidence", "Calculated call confidence"],
    ["PATH", "Path", "Path evidence behind the glyph"], ["MODE", "Mode", "How the mode was determined"],
  ];
  const SWITCHES = [
    ["beacon", "BEACON", "Beacon spots", "NCDXF and other beacons. Still pass through your rules."],
    ["self", "SELF", "Spots of my call", "Let spots of your own call bypass your rules."],
    ["announce", "ANNOUNCE", "Announcements", "Messages sent to all users. Not spots."],
    ["wwv", "WWV", "WWV bulletins", "Solar flux and geomagnetic index reports."],
    ["wcy", "WCY", "WCY bulletins", "DK0WCY space weather reports."],
    ["toxic", "TOXIC", "Flagged comments", "Human spots whose comment the node flagged as abusive."],
  ];
  const SWITCH_DEFAULTS = { beacon: true, self: true, announce: true, wwv: true, wcy: true, toxic: true };

  // Shipped runtime.yaml filter.default_modes / default_sources -> new-user PASS rule 0.
  const NEW_USER_RULE = "MODE=CW,LSB,USB,UNKNOWN,RTTY,MSK144,PSK,SSTV AND SOURCE=HUMAN,SKIMMER";

  // ---------------------------------------------------------------- simulation sample entities
  // [label, adif, name, cont, prefixes, [[grid4, cqZone, state]...], weight]
  const ENTITIES = [
    ["K", 291, "United States", "NA", ["K", "W", "N", "AA", "KD", "WB"], [["FN31", 5, "CT"], ["FN42", 5, "MA"], ["EM73", 5, "GA"], ["DM79", 4, "CO"], ["CN87", 3, "WA"], ["EN52", 4, "IL"], ["EM12", 4, "TX"], ["FM18", 5, "VA"], ["FN20", 5, "PA"], ["CM87", 3, "CA"], ["EL96", 5, "FL"], ["EN80", 4, "OH"]], 16],
    ["VE", 1, "Canada", "NA", ["VE", "VA", "VY"], [["FN25", 5, "ON"], ["FN35", 2, "QC"], ["DO21", 4, "AB"], ["CN89", 3, "BC"], ["FN74", 5, "NS"]], 4],
    ["XE", 50, "Mexico", "NA", ["XE", "XE"], [["EK09", 6, ""], ["DL44", 6, ""]], 1.4],
    ["KP4", 202, "Puerto Rico", "NA", ["KP4"], [["FK68", 8, ""]], 0.8],
    ["KH6", 110, "Hawaii", "OC", ["KH6"], [["BL11", 31, ""]], 0.7],
    ["KL", 6, "Alaska", "NA", ["KL7"], [["BP51", 1, ""]], 0.5],
    ["PY", 108, "Brazil", "SA", ["PY", "PU", "PP"], [["GG66", 11, ""], ["GG87", 11, ""], ["GH64", 11, ""]], 3],
    ["LU", 100, "Argentina", "SA", ["LU", "LW", "LT"], [["GF05", 13, ""], ["FF96", 13, ""]], 2],
    ["CE", 112, "Chile", "SA", ["CE", "CA"], [["FF46", 12, ""]], 0.9],
    ["HK", 116, "Colombia", "SA", ["HK", "HJ"], [["FJ24", 9, ""]], 0.7],
    ["OA", 136, "Peru", "SA", ["OA"], [["FH17", 10, ""]], 0.5],
    ["G", 223, "England", "EU", ["G", "M", "2E"], [["IO91", 14, ""], ["IO83", 14, ""], ["JO01", 14, ""]], 5],
    ["DL", 230, "Fed. Rep. of Germany", "EU", ["DL", "DK", "DJ", "DF", "DO"], [["JO31", 14, ""], ["JN58", 14, ""], ["JO62", 14, ""], ["JN49", 14, ""]], 8],
    ["F", 227, "France", "EU", ["F", "F", "F"], [["JN18", 14, ""], ["IN97", 14, ""], ["JN03", 14, ""]], 3.5],
    ["I", 248, "Italy", "EU", ["I", "IK", "IZ"], [["JN45", 15, ""], ["JN61", 15, ""], ["JM78", 15, ""]], 3.5],
    ["EA", 281, "Spain", "EU", ["EA", "EB", "EC"], [["IN80", 14, ""], ["JN11", 14, ""], ["IM87", 14, ""]], 3],
    ["ON", 209, "Belgium", "EU", ["ON", "OO"], [["JO20", 14, ""]], 1.5],
    ["PA", 263, "Netherlands", "EU", ["PA", "PD", "PE"], [["JO22", 14, ""], ["JO32", 14, ""]], 2],
    ["OH", 224, "Finland", "EU", ["OH", "OG"], [["KP20", 15, ""], ["KP11", 15, ""]], 1.5],
    ["SM", 284, "Sweden", "EU", ["SM", "SA"], [["JO89", 14, ""], ["JP80", 14, ""]], 1.8],
    ["LA", 266, "Norway", "EU", ["LA", "LB"], [["JO59", 14, ""]], 1],
    ["OK", 503, "Czech Republic", "EU", ["OK", "OL"], [["JO70", 15, ""], ["JN89", 15, ""]], 2],
    ["SP", 269, "Poland", "EU", ["SP", "SQ", "SO"], [["KO02", 15, ""], ["JO92", 15, ""]], 2.5],
    ["HA", 239, "Hungary", "EU", ["HA", "HG"], [["JN97", 15, ""]], 1.2],
    ["S5", 499, "Slovenia", "EU", ["S5"], [["JN76", 15, ""]], 0.8],
    ["9A", 497, "Croatia", "EU", ["9A"], [["JN75", 15, ""], ["JN95", 15, ""]], 1],
    ["UA", 54, "European Russia", "EU", ["UA", "RA", "RW", "RZ"], [["KO85", 16, ""], ["LO07", 16, ""], ["KO59", 16, ""]], 4],
    ["UR", 288, "Ukraine", "EU", ["UR", "UT", "UX"], [["KO50", 16, ""], ["KN98", 16, ""]], 2],
    ["YO", 275, "Romania", "EU", ["YO"], [["KN34", 20, ""]], 1],
    ["LZ", 212, "Bulgaria", "EU", ["LZ"], [["KN22", 20, ""]], 0.9],
    ["SV", 236, "Greece", "EU", ["SV", "SW"], [["KM18", 20, ""]], 0.9],
    ["CT", 272, "Portugal", "EU", ["CT", "CS"], [["IM58", 14, ""]], 1],
    ["EI", 245, "Ireland", "EU", ["EI"], [["IO63", 14, ""]], 0.8],
    ["GM", 279, "Scotland", "EU", ["GM", "MM"], [["IO85", 14, ""]], 0.8],
    ["OE", 206, "Austria", "EU", ["OE"], [["JN78", 15, ""]], 0.9],
    ["HB", 287, "Switzerland", "EU", ["HB9"], [["JN47", 14, ""]], 0.9],
    ["JA", 339, "Japan", "AS", ["JA", "JH", "JR", "JE"], [["PM95", 25, ""], ["PM74", 25, ""], ["QN02", 25, ""]], 5],
    ["BY", 318, "China", "AS", ["BY", "BG", "BH"], [["OM89", 24, ""], ["PM01", 24, ""]], 1.5],
    ["HL", 137, "Republic of Korea", "AS", ["HL", "DS"], [["PM37", 25, ""]], 1],
    ["VU", 324, "India", "AS", ["VU"], [["MK82", 22, ""]], 0.7],
    ["UA9", 15, "Asiatic Russia", "AS", ["UA9", "RA9", "R0"], [["MO06", 17, ""], ["NO14", 18, ""]], 1.5],
    ["4X", 336, "Israel", "AS", ["4X", "4Z"], [["KM72", 20, ""]], 0.7],
    ["A6", 391, "United Arab Emirates", "AS", ["A6"], [["LL75", 21, ""]], 0.4],
    ["HS", 387, "Thailand", "AS", ["HS", "E2"], [["OK03", 26, ""]], 0.5],
    ["YB", 327, "Indonesia", "OC", ["YB", "YC", "YD"], [["OI33", 28, ""]], 1],
    ["DU", 375, "Philippines", "OC", ["DU", "DW"], [["PK04", 27, ""]], 0.6],
    ["VK", 150, "Australia", "OC", ["VK"], [["QF56", 30, ""], ["QF22", 30, ""], ["OF78", 29, ""]], 2.4],
    ["ZL", 170, "New Zealand", "OC", ["ZL"], [["RF70", 32, ""], ["RE66", 32, ""]], 1],
    ["FO", 175, "French Polynesia", "OC", ["FO5", "FO8"], [["BH52", 32, ""]], 0.25],
    ["3D2", 176, "Fiji", "OC", ["3D2"], [["RH91", 32, ""]], 0.2],
    ["ZS", 462, "South Africa", "AF", ["ZS", "ZR"], [["KG33", 38, ""], ["JF96", 38, ""]], 1.4],
    ["5Z", 430, "Kenya", "AF", ["5Z4"], [["KI88", 37, ""]], 0.3],
    ["CN", 446, "Morocco", "AF", ["CN8"], [["IM63", 33, ""]], 0.6],
    ["EA8", 29, "Canary Islands", "AF", ["EA8", "EC8"], [["IL18", 33, ""]], 0.9],
    ["D4", 409, "Cabo Verde", "AF", ["D44"], [["HK76", 35, ""]], 0.3],
    ["9J", 482, "Zambia", "AF", ["9J2"], [["KH54", 36, ""]], 0.2],
    ["VP8", 141, "Falkland Islands", "SA", ["VP8"], [["FD86", 13, ""]], 0.15],
    ["ZD7", 250, "St. Helena", "AF", ["ZD7"], [["IH74", 36, ""]], 0.12],
    ["T32", 48, "Eastern Kiribati", "OC", ["T32"], [["AJ11", 31, ""]], 0.1],
    ["CE9", 13, "Antarctica", "AN", ["KC4", "DP0"], [["FC57", 39, ""]], 0.08],
  ];

  // NCDXF/IARU beacons (public beacon network).
  const BEACONS = [
    ["4U1UN", 14100, "FN30", 289], ["VE8AT", 14100, "EQ79", 1], ["W6WX", 14100, "CM97", 291], ["KH6RS", 14100, "BL10", 110],
    ["ZL6B", 14100, "RE78", 170], ["VK6RBP", 14100, "OF87", 150], ["JA2IGY", 14100, "PM84", 339], ["4S7B", 14100, "MJ96", 315],
    ["ZS6DN", 14100, "KG44", 462], ["5Z4B", 14100, "KI88", 430], ["4X6TU", 14100, "KM72", 336], ["OH2B", 14100, "KP20", 224],
    ["CS3B", 14100, "IM12", 256], ["LU4AA", 14100, "GF05", 100], ["OA4B", 14100, "FH17", 136], ["YV5B", 14100, "FJ69", 148],
  ];

  const HUMAN_COMMENTS = ["TNX QSO", "UP 2", "UP 5-10", "QSX 14.205", "loud in FN", "5NN", "CQ DX", "nice signal", "QRZ", "booming in",
    "TU", "split", "pse QSL LoTW", "weak but readable", "QRP 5W", "big pileup", "listening up", "CQ NA", "new one", "599"];
  const ANNOUNCEMENTS = [
    "Contest this weekend. Please keep the spots coming.",
    "Node maintenance window Tuesday 0400Z, about 10 minutes.",
    "Good opening to JA on 15m right now.",
    "New CTY file loaded on this node.",
  ];

  return {
    CONTINENTS, CONT_NAME, BAND_PLAN, MAIN_BANDS, SEGMENTS, META_BANDS, MODES, MODE_FAMILY, CONF, PATH,
    NOISE, DEDUPE, DIAG, SWITCHES, SWITCH_DEFAULTS, NEW_USER_RULE, ENTITIES, BEACONS, HUMAN_COMMENTS, ANNOUNCEMENTS,
  };
})();

// GoCluster UI — filter rule model.
//
// The server owns numbered GO rules: { slot 0..9, action pass|reject, expr }.
// This module converts between that canonical expression and a visual "card"
// model the UI can edit without the user writing Boolean syntax:
//
//   card  = { slot, action, items[] }            items are ANDed together
//   item  = cond  { type:"cond", field, op, values[] }   values are ORed
//         | pred  { type:"pred", negate }                 NEARBY / NOT NEARBY
//         | any   { type:"any", items:[cond|pred] }       one ORed group
//
// Anything the card model cannot express (NOT over a group, nested groups,
// NOT over a comparison) stays an "expert" card holding the raw expression.
// Validation always runs the real grammar through the ported engine (GC),
// so the UI never accepts an expression the server would refuse.
"use strict";

const RULES = (() => {
  const G = GC;
  const LIM = G.LIM;

  // ---------------------------------------------------------------- field catalog
  const F = {
    BAND: { label: "Band", group: "What", kind: "enum", noun: "band" },
    MODE: { label: "Mode", group: "What", kind: "enum", noun: "mode" },
    FREQ: { label: "Frequency", group: "What", kind: "freq", noun: "kHz", ordered: true },
    EVENT: { label: "Activity", group: "What", kind: "enum", noun: "activity" },
    COMMENT: { label: "Comment text", group: "What", kind: "text", noun: "text" },
    DXCALL: { label: "DX callsign", group: "Who", kind: "call", noun: "call" },
    DECALL: { label: "Spotter callsign", group: "Who", kind: "call", noun: "call" },
    SOURCE: { label: "Reported by", group: "Who", kind: "enum", noun: "source" },
    DXCONT: { label: "DX continent", group: "DX location", kind: "enum", noun: "continent" },
    DXDXCC: { label: "DX country", group: "DX location", kind: "entity", noun: "country" },
    DXZONE: { label: "DX CQ zone", group: "DX location", kind: "zone", noun: "zone", ordered: true },
    DXSTATE: { label: "DX state / province", group: "DX location", kind: "state", noun: "state" },
    DXGRID2: { label: "DX grid field", group: "DX location", kind: "grid2", noun: "field" },
    DECONT: { label: "Spotter continent", group: "Spotter location", kind: "enum", noun: "continent" },
    DEDXCC: { label: "Spotter country", group: "Spotter location", kind: "entity", noun: "country" },
    DEZONE: { label: "Spotter CQ zone", group: "Spotter location", kind: "zone", noun: "zone", ordered: true },
    DESTATE: { label: "Spotter state / province", group: "Spotter location", kind: "state", noun: "state" },
    DEGRID2: { label: "Spotter grid field", group: "Spotter location", kind: "grid2", noun: "field" },
    NEARBY: { label: "Near me", group: "Spotter location", kind: "pred", noun: "" },
    CONF: { label: "Call confidence", group: "Quality", kind: "enum", noun: "level" },
    PATH: { label: "Path to me", group: "Quality", kind: "enum", noun: "class" },
    SNR: { label: "Signal report (dB)", group: "Quality", kind: "number", noun: "dB", ordered: true },
  };
  const GROUPS = ["What", "Who", "DX location", "Spotter location", "Quality"];

  const ENUM = {
    BAND: () => G.BANDS.map(b => [b.toUpperCase(), b]),
    MODE: () => DATA.MODES.map(m => [m, m === "UNKNOWN" ? "No mode" : m]),
    SOURCE: () => [["HUMAN", "People"], ["SKIMMER", "Skimmers"]],
    EVENT: () => [["POTA", "POTA"], ["SOTA", "SOTA"], ["IOTA", "IOTA"], ["WWFF", "WWFF"], ["LLOTA", "LLOTA"], ["NONE", "No activity tag"]],
    DXCONT: () => [...DATA.CONTINENTS.map(([c, n]) => [c, n]), ["MYCONT", "My continent"]],
    DECONT: () => [...DATA.CONTINENTS.map(([c, n]) => [c, n]), ["MYCONT", "My continent"]],
    CONF: () => ["V", "P", "S", "?", "C", "B"].map(c => [c, `${c} · ${DATA.CONF[c][0]}`]),
    PATH: () => Object.entries(DATA.PATH).map(([k, v]) => [k, v[1]]),
  };

  const OPS = {
    "=": { short: "is", long: "is any of" },
    "<>": { short: "is not", long: "is none of" },
    "<": { short: "below", long: "is below" },
    "<=": { short: "at most", long: "is at most" },
    ">": { short: "above", long: "is above" },
    ">=": { short: "at least", long: "is at least" },
  };
  const opsFor = field => (F[field] && F[field].ordered ? ["=", "<>", "<", "<=", ">", ">="] : ["=", "<>"]);

  // ---------------------------------------------------------------- CTY lookup
  const CTY_ROWS = typeof CTY_CANONICAL !== "undefined" ? CTY_CANONICAL : [];
  const ADIF_NAME = new Map();
  const ADIF_LABEL = new Map();
  for (const [label, adif, name] of CTY_ROWS) {
    if (!ADIF_NAME.has(adif)) { ADIF_NAME.set(adif, name); ADIF_LABEL.set(adif, label); }
  }
  G.setCTYTable(CTY_ROWS.map(r => [r[0], r[1]]));
  const resolve = G.ctyResolve;
  function searchEntities(q, limit = 8) {
    const u = q.trim().toUpperCase();
    if (!u) return [];
    const out = [];
    for (const [label, adif, name] of CTY_ROWS) {
      const hitLabel = label === u ? 3 : label.startsWith(u) ? 2 : 0;
      const hitName = name.toUpperCase().includes(u) ? 1 : 0;
      if (hitLabel || hitName) out.push({ label, adif, name, score: hitLabel * 2 + hitName });
    }
    out.sort((a, b) => b.score - a.score || a.label.length - b.label.length);
    const seen = new Set();
    return out.filter(e => (seen.has(e.adif) ? false : seen.add(e.adif))).slice(0, limit);
  }

  // ---------------------------------------------------------------- value display
  function valueLabel(field, v) {
    if (v === "ALL") return "Any value";
    if (field === "DXDXCC" || field === "DEDXCC") {
      if (v === "MYDXCC") return "My country";
      const n = Number(v);
      return ADIF_NAME.get(n) || `ADIF ${v}`;
    }
    if (field === "BAND") return v.toLowerCase();
    const en = ENUM[field];
    if (en) { const hit = en().find(([k]) => k === v); if (hit) return hit[1]; }
    if (field === "FREQ") return v.includes("-") ? v.replace("-", "–") + " kHz" : v + " kHz";
    if (field === "SNR") return v + " dB";
    if (field === "DXZONE" || field === "DEZONE") return "zone " + v;
    return v;
  }
  const metaFor = values => {
    const set = new Set(values.map(v => v.toLowerCase()));
    for (const [name, label, members] of DATA.META_BANDS) {
      if (members.length === set.size && members.every(m => set.has(m))) return name;
    }
    return null;
  };

  // ---------------------------------------------------------------- expression <-> card
  const bare = s => s !== "" && /^[A-Za-z0-9_\-./*?#@]+$/.test(s);
  function quoteValue(field, v) {
    if (field === "COMMENT") return '"' + v.replace(/"/g, '""') + '"';
    return bare(v) ? v : '"' + v.replace(/"/g, '""') + '"';
  }
  function condExpr(c) {
    if (c.type === "pred") return c.negate ? "NOT NEARBY" : "NEARBY";
    return c.field + c.op + c.values.map(v => quoteValue(c.field, v)).join(",");
  }
  function itemExpr(it) {
    if (it.type === "any") {
      const parts = it.items.map(condExpr);
      return parts.length > 1 ? "(" + parts.join(" OR ") + ")" : parts[0] || "";
    }
    return condExpr(it);
  }
  function cardExpr(card) {
    if (card.expert) return card.expr.trim();
    return card.items.map(itemExpr).filter(Boolean).join(" AND ");
  }

  function atomToCond(n) {
    if (n.kind === 4) return { type: "pred", negate: false };
    const a = n.atom;
    return { type: "cond", field: a.field, op: a.op, values: a.values.map(v => v.text) };
  }
  // Parse canonical expression into card items, or null when not representable.
  function exprToItems(expr) {
    let ex;
    try { ex = G.parseExpr(expr); } catch (e) { return null; }
    const N = ex.nodes;
    const flat = (i, kind, out) => { if (N[i].kind === kind) { flat(N[i].left, kind, out); flat(N[i].right, kind, out); } else out.push(i); return out; };
    const leaf = i => {
      const n = N[i];
      if (n.kind === 0 || n.kind === 4) return atomToCond(n);
      if (n.kind === 1 && N[n.left].kind === 4) return { type: "pred", negate: true };
      return null;
    };
    const items = [];
    for (const i of flat(ex.root, 2, [])) {
      const n = N[i];
      if (n.kind === 3) {
        const subs = flat(i, 3, []).map(leaf);
        if (subs.some(s => !s)) return null;
        items.push({ type: "any", items: subs });
      } else {
        const l = leaf(i);
        if (!l) return null;
        items.push(l);
      }
    }
    return items;
  }

  let uid = 0;
  const newId = () => "c" + (++uid).toString(36) + Date.now().toString(36).slice(-3);
  function cardFromRule(rule) {
    const items = exprToItems(rule.expr);
    const card = { id: newId(), slot: rule.slot, action: rule.action, expr: rule.expr, expert: !items, items: items || [] };
    // Keep the node's spelling when the visual editor formats an untouched
    // expression differently. A later whole-bank PATCH must preserve that rule.
    card.originalExpr = rule.expr;
    card.originalEditorExpr = cardExpr(card);
    return card;
  }
  const rulesFromCards = cards => cards.map(c => ({ slot: c.slot, action: c.action, expr: cardExpr(c) }));

  // ---------------------------------------------------------------- validation
  // Returns { ok, canonical, error, col } for one card.
  function checkCard(card) {
    const expr = cardExpr(card);
    if (!expr) return { ok: false, error: card.action === "pass" ? "Add at least one condition." : "Add at least one condition. An empty Hide rule is not allowed." };
    for (const it of card.expert ? [] : card.items) {
      const conds = it.type === "any" ? it.items : [it];
      for (const c of conds) if (c.type === "cond" && c.values.length === 0) return { ok: false, error: `Pick at least one ${F[c.field].noun} for “${F[c.field].label}”.` };
      if (it.type === "any" && it.items.length === 0) return { ok: false, error: "An “any of” group needs at least one condition." };
    }
    try {
      const prog = G.compileRule(expr, resolve);
      return { ok: true, canonical: prog.canonical, prog };
    } catch (e) {
      return { ok: false, error: friendlyError(e.message), col: e.col };
    }
  }
  function friendlyError(msg) {
    return msg.replace(/^col \d+: /, "").replace(/^rule slot \d+: /, "")
      .replace("expected field or Boolean term", "Expected a field such as BAND or MODE")
      .replace("expected comparison operator", "Expected = or <> after the field name");
  }
  // Validates the whole set against combined limits. Returns { set, error }.
  function buildSet(cards) {
    let set = null;
    try {
      for (const c of cards) {
        const r = checkCard(c);
        if (!r.ok) return { set: null, error: `Rule ${c.slot}: ${r.error}` };
        set = G.withRule(set, { slot: c.slot, action: c.action, expr: r.canonical }, resolve);
      }
      return { set, error: null };
    } catch (e) { return { set: null, error: friendlyError(e.message) }; }
  }

  // Teaching lints for the most common Boolean misunderstandings.
  function lintCard(card, ctx) {
    const out = [];
    if (card.expert) return out;
    const conds = [];
    for (const it of card.items) {
      if (it.type === "cond") conds.push(it);
      if (it.type === "pred" && !ctx.grid) out.push({ level: "warn", text: "“Near me” needs your grid. Set it in Settings, or this condition is never true." });
    }
    const byField = {};
    for (const c of conds) (byField[c.field] = byField[c.field] || []).push(c);
    for (const [f, list] of Object.entries(byField)) {
      const eqs = list.filter(c => c.op === "=");
      if (eqs.length > 1 && !F[f].ordered) {
        out.push({ level: "error", fix: { kind: "merge", field: f }, text: `A spot has only one ${F[f].noun}, so two “${F[f].label} is …” conditions can never both match. Merge them into one condition to mean “either”.` });
      }
    }
    for (const c of conds) {
      if (c.field === "PATH" && !ctx.grid) out.push({ level: "warn", text: "Path classes need your grid. Without it every spot is “Not rated”." });
      if (c.op === "<>" && ["DXSTATE", "DESTATE", "DXZONE", "DEZONE", "DXDXCC", "DEDXCC", "DXCONT", "DECONT", "CONF"].includes(c.field)) {
        out.push({ level: "info", text: `“${F[c.field].label} is not …” is false when the ${F[c.field].noun} is unknown. Those spots won't match.` });
      }
      if (c.field === "SNR" && card.action === "reject" && !conds.some(x => x.field === "SOURCE")) {
        out.push({ level: "info", text: "This also hides human spots that carry a signal report. Add “Reported by is Skimmers” to limit it to automated reports." });
      }
    }
    if (card.action === "reject" && card.items.length === 0) out.push({ level: "error", text: "An empty Hide rule would hide every spot." });
    return out;
  }
  function lintSet(cards) {
    const out = [];
    const pass = cards.filter(c => c.action === "pass");
    if (cards.length && !pass.length) out.push({ level: "info", text: "No Show rules: every spot appears unless a Hide rule matches it." });
    if (!cards.length) out.push({ level: "info", text: "No rules: you receive every spot the node delivers." });
    return out;
  }

  function mergeField(card, field) {
    const keep = [];
    let merged = null;
    for (const it of card.items) {
      if (it.type === "cond" && it.field === field && it.op === "=") {
        if (!merged) { merged = { ...it, values: [...it.values] }; keep.push(merged); }
        else for (const v of it.values) if (!merged.values.includes(v)) merged.values.push(v);
      } else keep.push(it);
    }
    return { ...card, items: keep };
  }

  // ---------------------------------------------------------------- plain English
  function listWords(arr, joiner = "or") {
    if (arr.length <= 1) return arr.join("");
    if (arr.length === 2) return arr.join(` ${joiner} `);
    return arr.slice(0, -1).join(", ") + ` ${joiner} ` + arr[arr.length - 1];
  }
  const META_PHRASE = { HF: "all HF bands", HFCONTEST: "contest bands", WARC: "WARC bands", VHF: "VHF bands", UHF: "UHF bands" };
  function condWords(c) {
    if (c.type === "pred") return c.negate ? "not near me" : "near me";
    const vals = c.values;
    const short = (arr, n = 4) => (arr.length > n ? arr.slice(0, n).concat([`${arr.length - n} more`]) : arr);
    const neg = c.op === "<>";
    const words = short(vals.map(v => valueLabel(c.field, v).replace(/^My /, "my ")));
    switch (c.field) {
      case "BAND": { const m = metaFor(vals); const w = m ? META_PHRASE[m] : listWords(words); return (neg ? "not on " : "on ") + w; }
      case "MODE": return (neg ? "not " : "") + listWords(words);
      case "SOURCE": return (neg ? "not " : "") + "from " + listWords(words).toLowerCase();
      case "EVENT": return (neg ? "not " : "") + listWords(words) + (vals.includes("NONE") ? "" : " activations");
      case "DXCONT": return "DX " + (neg ? "outside " : "in ") + listWords(words);
      case "DECONT": return "heard " + (neg ? "outside " : "in ") + listWords(words);
      case "DXDXCC": return "DX " + (neg ? "not in " : "in ") + listWords(words);
      case "DEDXCC": return "heard " + (neg ? "outside " : "in ") + listWords(words);
      case "COMMENT": return "comment " + (neg ? "without " : "with ") + listWords(words.map(w => `“${w}”`));
      case "DXCALL": return (neg ? "DX not " : "DX ") + listWords(words);
      case "DECALL": return (neg ? "not spotted by " : "spotted by ") + listWords(words);
      case "CONF": return "confidence " + (neg ? "not " : "") + listWords(vals);
      case "PATH": return "path " + (neg ? "not " : "") + listWords(words).toLowerCase();
      case "SNR": return c.op === "=" ? `report ${listWords(vals)} dB` : c.op === "<>" ? `report not ${listWords(vals)} dB` : `report ${OPS[c.op].short} ${vals[0]} dB`;
      case "FREQ": return c.op === "=" || c.op === "<>" ? (neg ? "outside " : "within ") + listWords(words) : `${OPS[c.op].short} ${vals[0]} kHz`;
      case "DXZONE": case "DEZONE": return `${c.field === "DXZONE" ? "DX" : "spotter"} in CQ zone ${c.op === "=" ? "" : OPS[c.op].short + " "}${listWords(vals)}`.replace("zone is not", "zone not");
      default: return `${F[c.field].label.toLowerCase()} ${OPS[c.op].short} ${listWords(words)}`;
    }
  }
  function cardSentence(card) {
    if (card.expert) return card.action === "pass" ? "Custom expression" : "Custom expression";
    if (!card.items.length) return "No conditions yet";
    const parts = card.items.map(it => it.type === "any" ? "(" + listWords(it.items.map(condWords)) + ")" : condWords(it));
    const s = parts.join(", ");
    return s.charAt(0).toUpperCase() + s.slice(1);
  }
  // ---------------------------------------------------------------- slots
  function nextSlot(cards, action) {
    const used = new Set(cards.map(c => c.slot));
    if (action === "reject") { for (let s = 9; s >= 0; s--) if (!used.has(s)) return s; }
    else { for (let s = 0; s <= 9; s++) if (!used.has(s)) return s; }
    return -1;
  }
  const sortCards = cards => cards.slice().sort((a, b) => a.slot - b.slot);

  // ---------------------------------------------------------------- recipes
  const RECIPES = [
    { id: "contest-cw", title: "CW on the contest bands", desc: "160 through 10m, no WARC.", action: "pass", expr: "BAND=HFCONTEST AND MODE=CW" },
    { id: "digi", title: "FT8 and FT4 with a good path", desc: "Only stations the path model rates workable or better.", action: "pass", expr: "MODE=FT8,FT4 AND PATH=HIGH,MEDIUM" },
    { id: "activators", title: "Parks and summits", desc: "POTA and SOTA activators, posted by people.", action: "pass", expr: "EVENT=POTA,SOTA AND SOURCE=HUMAN" },
    { id: "dx-only", title: "DX outside my continent", desc: "Skip stations on your own continent.", action: "pass", expr: "DXCONT<>MYCONT" },
    { id: "local-skimmers", title: "Heard near me", desc: "Skimmers in your continent hearing CW DX.", action: "pass", expr: "MODE=CW AND SOURCE=SKIMMER AND DECONT=MYCONT AND DXCONT<>MYCONT" },
    { id: "eu-or-near", title: "Europe, or anything near me", desc: "CW from Europe, or with an endpoint in your area.", action: "pass", expr: "MODE=CW AND (DXCONT=EU OR NEARBY)" },
    { id: "warc", title: "WARC bands", desc: "30, 17 and 12 meters, any mode.", action: "pass", expr: "BAND=WARC" },
    { id: "weak-skimmer", title: "Hide weak skimmer CW and RTTY", desc: "Automated reports below 10 dB.", action: "reject", expr: "SOURCE=SKIMMER AND MODE=CW,RTTY AND SNR<10" },
    { id: "hide-ft", title: "Hide FT8 and FT4", desc: "Keeps everything else your Show rules allow.", action: "reject", expr: "MODE=FT8,FT4" },
    { id: "doubtful", title: "Hide doubtful calls", desc: "Unconfirmed or busted calls from skimmers.", action: "reject", expr: "SOURCE=SKIMMER AND CONF=?,B" },
    { id: "closed", title: "Hide closed paths", desc: "Paths rated poor or closed from your grid.", action: "reject", expr: "PATH=UNLIKELY,CLOSED" },
  ];

  function blankCond(field) {
    if (field === "NEARBY") return { type: "pred", negate: false };
    return { type: "cond", field, op: "=", values: [] };
  }

  return {
    F, GROUPS, ENUM, OPS, opsFor, valueLabel, metaFor, searchEntities, ADIF_NAME, ADIF_LABEL, resolve,
    cardExpr, condExpr, exprToItems, cardFromRule, rulesFromCards, checkCard, buildSet, lintCard, lintSet, mergeField,
    condWords, cardSentence, listWords, nextSlot, sortCards, RECIPES, blankCond, newId, quoteValue, LIM,
  };
})();

// GoCluster UI — JSON v1 protocol session.
//
// Pure protocol logic with no DOM or socket: the transport feeds it each
// inbound WebSocket text message and gives it a send() function. It runs the
// text login, negotiates SET JSON ON VERSION 1, decodes records through the
// schema names the server sends, streams history pages and matches schema 7
// YAML replies to their requests. Node tests load this file directly.
"use strict";

const GCP = (() => {
  const ACK = "JSON ON 1";
  const NEGOTIATE = "SET JSON ON VERSION 1";
  const COMMIT_ENDS = new Set(["exhausted", "count", "budget"]);
  const MAX_PAGE_ROWS = 250;
  const MAX_TEXT_LINE = 4096;
  let keySeq = 0; // row keys stay unique across sessions and reconnects

  // Text-mode patterns. The prompt and greeting come from the node's runtime.yaml,
  // so they are deliberately loose; the login call must appear in the success line.
  const PATTERNS = {
    prompt: /(login|call(sign)?)\s*:\s*$/i,
    refused: /(not accepted|enter your callsign|invalid call|login refused|too many|not allowed to log|blocked|try again later)/i,
    evicted: /another login with your call/i,
    jsonRefused: /^JSON REFUSED\b/i,
  };

  // ---------------------------------------------------------------- small helpers
  function splitLines(text) {
    const out = String(text).split("\n");
    for (let i = 0; i < out.length; i++) if (out[i].endsWith("\r")) out[i] = out[i].slice(0, -1);
    if (out.length && out[out.length - 1] === "") out.pop(); // the message ended with \n
    return out;
  }
  // Remote text is shown as plain text only. Drop control characters other than tab
  // and anything a telnet bridge might leave behind (IAC decoded as U+FFFD).
  function cleanText(s) {
    s = String(s == null ? "" : s);
    if (s.length > MAX_TEXT_LINE) s = s.slice(0, MAX_TEXT_LINE) + "…";
    return s.replace(/[\u0000-\u0008\u000b-\u001f\u007f�]/g, "");
  }
  // Unix nanoseconds arrive as decimal strings (D7). Keep the string; convert to
  // milliseconds only for display, without losing precision through a float.
  function nsToMs(ns) {
    if (typeof ns !== "string" || !/^-?\d{1,25}$/.test(ns)) return null;
    if (typeof BigInt === "function") return Number(BigInt(ns) / 1000000n);
    return Math.floor(Number(ns.slice(0, -6) || "0"));
  }
  const str = v => (typeof v === "string" ? v : null);
  const num = v => (typeof v === "number" && isFinite(v) ? v : null);
  const bool = v => v === true;
  const strArr = v => (Array.isArray(v) ? v.filter(x => typeof x === "string") : []);

  // ---------------------------------------------------------------- schema-indexed decoding
  function indexOf(fields) {
    const idx = Object.create(null);
    if (Array.isArray(fields)) fields.forEach((name, i) => { if (typeof name === "string" && !(name in idx)) idx[name] = i; });
    return idx;
  }
  function field(rec, idx, name) { const i = idx[name]; return i == null ? undefined : rec[i]; }

  function decodeStation(arr, idx) {
    if (!Array.isArray(arr)) return null;
    const call = str(field(arr, idx, "call"));
    if (!call) return null;
    return {
      call,
      adif: num(field(arr, idx, "adif")),
      country: str(field(arr, idx, "country")),
      cont: str(field(arr, idx, "cont")),
      cq: num(field(arr, idx, "cq")),
      itu: num(field(arr, idx, "itu")),
      state: str(field(arr, idx, "state")),
      grid: str(field(arr, idx, "grid")),
      gridDerived: bool(field(arr, idx, "gridDerived")),
      grid2: str(field(arr, idx, "grid2")),
      iaru: num(field(arr, idx, "iaru")),
    };
  }

  const SOURCE_LABEL = { MANUAL: "Local", RBN: "RBN", FT8: "RBN FT8", FT4: "RBN FT4", PSKREPORTER: "PSKReporter", UPSTREAM: "Upstream", PEER: "Peer" };

  // One s or h record -> the console's spot shape. Missing values stay null.
  // `key` is a client-side row key: live ids are per node process and history ids
  // are null, so neither is used as a key.
  function decodeSpot(rec, sIdx, stIdx, key) {
    const type = rec[0];
    const freq = num(field(rec, sIdx, "freq"));
    const dx = decodeStation(field(rec, sIdx, "dx"), stIdx);
    const de = decodeStation(field(rec, sIdx, "de"), stIdx);
    if (freq == null || freq <= 0 || !dx || !de) return null;
    const timeNs = str(field(rec, sIdx, "time"));
    const source = str(field(rec, sIdx, "source"));
    const human = field(rec, sIdx, "human");
    const cwSpeeds = strArr(field(rec, sIdx, "cwSpeeds"));
    const wpm = cwSpeeds.length ? parseInt(cwSpeeds[0], 10) || 0 : 0;
    const t = nsToMs(timeNs);
    return {
      id: key, kind: type === "h" ? "history" : "live", wireId: num(field(rec, sIdx, "id")),
      timeNs, t: t == null ? null : t,
      freq, obsFreq: num(field(rec, sIdx, "obsFreq")),
      band: str(field(rec, sIdx, "band")) || "", mode: str(field(rec, sIdx, "mode")) || "UNKNOWN",
      modeProv: str(field(rec, sIdx, "modeProv")), snr: num(field(rec, sIdx, "report")),
      comment: str(field(rec, sIdx, "comment")) || "", cwSpeeds, wpm,
      events: strArr(field(rec, sIdx, "events")), source, feed: SOURCE_LABEL[source] || source || "",
      node: str(field(rec, sIdx, "node")), human: human === true, automated: human === false, beacon: bool(field(rec, sIdx, "beacon")),
      conf: str(field(rec, sIdx, "conf")), confPct: num(field(rec, sIdx, "confPct")),
      toxic: bool(field(rec, sIdx, "toxic")), path: str(field(rec, sIdx, "path")), glyph: str(field(rec, sIdx, "glyph")),
      dx, de,
      // flattened names the existing views and rule preview already use
      dxCall: dx.call, dxAdif: dx.adif, dxEntity: dx.country, dxCont: dx.cont, dxZone: dx.cq, dxState: dx.state, dxGrid: dx.grid,
      deCall: de.call, deCallStripped: de.call.replace(/-#$/, ""), deAdif: de.adif, deEntity: de.country, deCont: de.cont, deZone: de.cq, deState: de.state, deGrid: de.grid,
    };
  }

  // ---------------------------------------------------------------- YAML request text
  let reqSeq = 0;
  function newRequestId(prefix = "ui") {
    reqSeq = (reqSeq + 1) % 1e6;
    const id = `${prefix}-${Date.now().toString(36)}-${reqSeq.toString(36)}`;
    return id.slice(0, 32);
  }
  // Deterministic block YAML. Every string is double-quoted (a JSON string is a valid
  // YAML double-quoted scalar), so DEFAULT, revisions and expressions stay strings.
  function yamlScalar(v) {
    if (typeof v === "string") return JSON.stringify(v);
    if (typeof v === "boolean") return v ? "true" : "false";
    if (typeof v === "number" && Number.isInteger(v)) return String(v);
    throw new Error("Unsupported YAML value: " + JSON.stringify(v));
  }
  function yamlBlock(obj, ind = 0) {
    const pad = " ".repeat(ind);
    const out = [];
    for (const [k, v] of Object.entries(obj)) {
      if (v === undefined) continue;
      if (v === null) throw new Error(`YAML field ${k} cannot be null`);
      if (Array.isArray(v)) {
        if (!v.length) { out.push(`${pad}${k}: []`); continue; }
        out.push(`${pad}${k}:`);
        for (const it of v) {
          if (it && typeof it === "object") {
            const lines = yamlBlock(it, ind + 4);
            out.push(`${pad}  - ${lines[0].trimStart()}`, ...lines.slice(1));
          } else out.push(`${pad}  - ${yamlScalar(it)}`);
        }
      } else if (typeof v === "object") {
        out.push(`${pad}${k}:`, ...yamlBlock(v, ind + 2));
      } else out.push(`${pad}${k}: ${yamlScalar(v)}`);
    }
    return out;
  }
  const SWITCH_FIELD = { beacon: "include_beacons", wwv: "allow_wwv", wcy: "allow_wcy", announce: "allow_announce", self: "allow_self", toxic: "allow_toxic" };
  const SETTING_FIELD = { dialect: "dialect", grid: "grid", noise: "noise_class", dedupe: "dedupe_policy", pathSamples: "path_min_observation_count", solar: "solar_summary_minutes", cwSpeed: "cw_speed" };

  // One upload = one message: header, ---, body, ... and nothing after the terminator.
  function uploadText(header, body) {
    const lines = yamlBlock(body);
    return [header, "---", ...lines, "..."].join("\n");
  }
  function getText(resource, id) { return `GET YAML ${resource} SCHEMA 7 ID ${id}`; }
  // rules: [{slot, action, expr}] replaces every slot; omit to leave rules alone.
  // switches: {beacon: true|false|"DEFAULT", ...}; only the fields given are sent.
  function patchFilterText({ requestId, revision, rules, switches }) {
    const filters = {};
    if (rules) filters.rules = rules.map(r => ({ slot: r.slot, action: r.action, expr: r.expr }));
    for (const [k, v] of Object.entries(switches || {})) {
      if (!SWITCH_FIELD[k]) throw new Error("Unknown switch " + k);
      filters[SWITCH_FIELD[k]] = v === "DEFAULT" ? "DEFAULT" : !!v;
    }
    return uploadText("PATCH YAML FILTER SCHEMA 7", { schema_version: 7, request_id: requestId, if_revision: String(revision), configuration: { dialect: "go", filters } });
  }
  function patchSettingsText({ requestId, revision, settings }) {
    const conf = {};
    for (const [k, v] of Object.entries(settings)) {
      if (!SETTING_FIELD[k]) throw new Error("Unknown setting " + k);
      conf[SETTING_FIELD[k]] = v;
    }
    return uploadText("PATCH YAML SETTINGS SCHEMA 7", { schema_version: 7, request_id: requestId, if_revision: String(revision), configuration: conf });
  }

  // GET YAML CONFIG SCHEMA 7 document -> the console's configuration view.
  // The raw document is kept as well; nothing here is written back.
  function configFromDoc(doc) {
    if (!doc || doc.schema_version !== 7 || !doc.configuration) throw new Error("Not a schema 7 configuration document");
    const c = doc.configuration, go = c.filters?.go, st = c.settings || {}, status = doc.status || {};
    // Never substitute an empty bank or inherited switch for missing readback
    // evidence. The same profile is used to confirm saves and recover drafts.
    if (!doc.revision || !Array.isArray(go?.rules) || go.rules.length > GC.LIM.Slots) throw new Error("The node returned an incomplete filter profile.");
    const slots = new Set();
    const rules = go.rules.map(r => {
      const action = String(r?.action).toLowerCase();
      if (!Number.isInteger(r?.slot) || r.slot < 0 || r.slot >= GC.LIM.Slots || slots.has(r.slot) ||
          (action !== "pass" && action !== "reject") || typeof r.expr !== "string" || !r.expr.trim()) throw new Error("The node returned an invalid filter rule.");
      slots.add(r.slot);
      return { slot: r.slot, action, expr: r.expr };
    });
    const switches = {};
    for (const [k, f] of Object.entries(SWITCH_FIELD)) {
      const value = go[f];
      if (value !== true && value !== false && value !== "DEFAULT") throw new Error("The node did not return a valid value for " + f + ".");
      switches[k] = value;
    }
    const active = status.active_dialect || (status.effective && status.effective.dialect) || st.dialect || "";
    const presets = status.presets || {};
    const goRef = presets.go || {};
    return {
      revision: String(doc.revision == null ? "" : doc.revision),
      rules: rules.sort((a, b) => a.slot - b.slot), switches,
      settings: {
        dialect: st.dialect || "", grid: st.grid || "", noise: st.noise_class || "", dedupe: st.dedupe_policy || "",
        pathSamples: Number(st.path_min_observation_count) || 0, solar: Number(st.solar_summary_minutes) || 0, cwSpeed: st.cw_speed === true,
      },
      activeDialect: String(active).toLowerCase(),
      effective: status.effective || {}, session: status.session || {}, server: status.server || {},
      preset: { name: goRef.associated ? goRef.name || "" : "", modified: !!goRef.modified },
      ccPreserved: !!(c.filters && c.filters.cc),
    };
  }

  // ---------------------------------------------------------------- the session
  // opts: send(text), emit(event, data), parseYaml(text), setTimer(fn, ms), clearTimer(id), now()
  class Session {
    constructor(opts) {
      this.o = Object.assign({ setTimer: (f, ms) => setTimeout(f, ms), clearTimer: id => clearTimeout(id), now: () => Date.now(), timeouts: {} }, opts);
      this.t = Object.assign({ prompt: 15000, login: 20000, welcomeGrace: 2500, negotiate: 15000, hello: 15000, yaml: 30000, busyRetry: [1000, 2000, 4000] }, this.o.timeouts);
      this.mode = "text";
      this.state = "idle";
      this.call = "";
      this.hello = null;
      this.buildRequested = false;
      this.schemas = {};
      this.sIdx = null; this.stIdx = null;
      this.timers = {};
      this.pendingYaml = new Map();
      this.hist = { open: null, pending: null, token: null, lastCompleted: null };
      this.presetCollect = null;
      this.counters = { malformed: 0, unknownType: 0, droppedRows: 0, keepalive: 0, lastRecordAt: 0 };
      this.closed = false;
    }
    emit(ev, data) { if (!this.closed) this.o.emit(ev, data); }
    send(text) { if (this.closed) throw new Error("Not connected"); this.o.send(text); this.emit("sent", text); }
    timer(name, ms, fn) { this.clear(name); this.timers[name] = this.o.setTimer(() => { delete this.timers[name]; fn(); }, ms); }
    clear(name) { if (this.timers[name] != null) { this.o.clearTimer(this.timers[name]); delete this.timers[name]; } }
    setState(s, detail) { this.state = s; this.emit("state", { state: s, detail }); }
    fail(code, message) { for (const k of Object.keys(this.timers)) this.clear(k); this.setState("failed", { code, message }); }

    // Live: wait for the prompt, then log in.
    start(call) {
      this.call = String(call).toUpperCase().trim();
      this.setState("await-prompt");
      this.timer("prompt", this.t.prompt, () => this.fail("prompt-timeout", "The cluster did not send a login prompt."));
    }
    // Replay captures start after the text acknowledgement.
    startInJson() { this.mode = "json"; this.setState("await-hello"); }

    feed(message) {
      if (this.closed) return;
      const lines = splitLines(message);
      for (const line of lines) {
        if (this.mode === "json") this.onJsonLine(line);
        else this.onTextLine(line);
      }
    }

    // ------------------------------------------------------------ text mode
    onTextLine(raw) {
      const line = cleanText(raw);
      const trimmed = line.trim();
      if (this.state === "negotiating" && trimmed === ACK) {
        this.clear("negotiate");
        this.mode = "json";
        this.emit("negotiated", trimmed);
        this.setState("await-hello");
        this.timer("hello", this.t.hello, () => this.fail("no-hello", "JSON was acknowledged but no hello record arrived."));
        return;
      }
      this.emit("text", line);
      if (PATTERNS.evicted.test(line)) { this.fail("evicted", line.trim()); return; }
      const upper = line.toUpperCase();
      switch (this.state) {
        case "await-prompt":
          if (PATTERNS.prompt.test(line)) {
            this.clear("prompt");
            this.setState("logging-in");
            this.send(this.call);
            this.timer("login", this.t.login, () => this.fail("login-timeout", "The cluster did not confirm the login."));
          }
          break;
        case "logging-in":
          if (PATTERNS.refused.test(line)) { this.fail("login-refused", trimmed); return; }
          if (PATTERNS.prompt.test(line) && !upper.includes(this.call)) { this.fail("login-refused", "The cluster asked for the callsign again."); return; }
          if (upper.includes(this.call) && / DE \S+.*>\s*$/.test(upper)) { this.loggedIn(); return; }
          if (upper.includes(this.call) && /WELCOME/.test(upper)) this.timer("welcome", this.t.welcomeGrace, () => this.loggedIn());
          break;
        case "negotiating":
          if (PATTERNS.jsonRefused.test(trimmed)) this.fail("json-refused", trimmed);
          break;
        default:
          this.observeText(line);
      }
    }
    loggedIn() {
      if (this.state !== "logging-in") return;
      this.clear("login"); this.clear("welcome");
      this.setState("negotiating");
      this.send(NEGOTIATE);
      this.timer("negotiate", this.t.negotiate, () => this.fail("negotiate-timeout", "The cluster did not acknowledge SET JSON ON VERSION 1."));
    }

    // ------------------------------------------------------------ JSON mode
    onJsonLine(line) {
      if (!line) return;
      let rec;
      try { rec = JSON.parse(line); } catch (e) { this.counters.malformed++; this.emit("protocol-error", { reason: "not JSON", line: line.slice(0, 200) }); return; }
      if (!Array.isArray(rec) || typeof rec[0] !== "string") { this.counters.malformed++; this.emit("protocol-error", { reason: "not a typed array", line: line.slice(0, 200) }); return; }
      this.counters.lastRecordAt = this.o.now();
      switch (rec[0]) {
        case "hello": return this.onHello(rec);
        case "schema": return this.onSchema(rec);
        case "s": return this.onSpot(rec);
        case "hb": return this.onHistoryBegin(rec);
        case "h": return this.onHistoryRow(rec);
        case "he": return this.onHistoryEnd(rec);
        case "text": { const l = cleanText(rec[1]); this.emit("text", l); this.observeText(l); return; }
        case "yaml": return this.onYaml(rec[1]);
        case "bulletin": return this.emit("bulletin", { kind: str(rec[1]) || "MESSAGE", t: nsToMs(rec[2]), timeNs: str(rec[2]), from: str(rec[3]), text: cleanText(rec[4]) });
        case "notice": return this.onNotice(rec);
        case "keepalive": this.counters.keepalive++; return;
        default: this.counters.unknownType++; // clients ignore unknown types
      }
    }
    onHello(rec) {
      const version = rec[1], info = rec[2] && typeof rec[2] === "object" ? rec[2] : {};
      if (version !== 1) { this.fail("version", `Unsupported JSON version ${version}.`); return; }
      this.hello = { node: str(info.node), login: str(info.login), server: str(info.server), timeNs: str(info.time), limits: info.limits || {} };
      this.emit("hello", this.hello);
      this.checkReady();
    }
    onSchema(rec) {
      const name = rec[1], version = rec[2], fields = rec[3];
      if (typeof name !== "string" || !Array.isArray(fields)) { this.counters.malformed++; return; }
      this.schemas[name] = { version, fields: fields.slice() };
      if (name === "s") this.sIdx = indexOf(fields);
      if (name === "station") this.stIdx = indexOf(fields);
      this.checkReady();
    }
    checkReady() {
      if (this.state === "await-hello" && this.hello && this.sIdx && this.stIdx) {
        this.clear("hello");
        this.setState("live", { hello: this.hello, schemas: this.schemas });
      }
    }
    decode(rec) {
      if (!this.sIdx || !this.stIdx) { this.counters.malformed++; return null; }
      const s = decodeSpot(rec, this.sIdx, this.stIdx, ++keySeq);
      if (!s) { this.counters.malformed++; this.emit("protocol-error", { reason: "spot record missing frequency or calls" }); }
      return s;
    }
    onSpot(rec) { const s = this.decode(rec); if (s) this.emit("spot", s); }
    onNotice(rec) {
      const code = str(rec[1]) || "", text = cleanText(rec[2]);
      if (code === "json-off") {
        this.mode = "text";
        this.discardOpenPage("json-off");
        this.emit("json-off", text);
        return;
      }
      this.emit("notice", { code, text });
    }

    // ------------------------------------------------------------ history pages
    // command: the full SH DX text; kind "fresh" | "next"; fresh: the command that
    // starts this search again after a restart.
    requestHistory(command, kind = "fresh", fresh = command) {
      if (this.state !== "live" || this.mode !== "json") throw new Error("Not live");
      if (this.hist.pending || this.hist.open) throw new Error("A history page is still loading.");
      this.hist.pending = { command, kind, fresh, attempt: 0, restarts: 0 };
      this.send(command);
      this.emit("history-request", { ...this.hist.pending });
    }
    requestOlder() {
      if (!this.hist.token) throw new Error("No older history to load.");
      const fresh = this.hist.lastCompleted ? this.hist.lastCompleted.fresh : null;
      this.requestHistory(`SH DX NEXT ${this.hist.token}`, "next", fresh);
    }
    get historyBusy() { return !!(this.hist.pending || this.hist.open); }
    onHistoryBegin(rec) {
      const info = rec[1] && typeof rec[1] === "object" ? rec[1] : {};
      if (this.hist.open) this.discardOpenPage("second hb before he");
      const request = this.hist.pending || { command: null, kind: "unrequested", fresh: null, attempt: 0, restarts: 0 };
      this.hist.open = { count: num(info.count), older: info.older === true, rows: [], request, overflow: 0 };
      this.emit("history-begin", { count: this.hist.open.count, older: this.hist.open.older, request });
    }
    onHistoryRow(rec) {
      const page = this.hist.open;
      if (!page) { this.counters.droppedRows++; return; }
      if (page.rows.length >= MAX_PAGE_ROWS) { page.overflow++; return; }
      const s = this.decode(rec);
      if (s) page.rows.push(s);
    }
    onHistoryEnd(rec) {
      const page = this.hist.open;
      const end = str(rec[1]) || "failed", token = str(rec[2]), meta = rec[3] && typeof rec[3] === "object" ? rec[3] : {};
      this.hist.open = null;
      const request = page ? page.request : (this.hist.pending || { command: null, kind: "unrequested" });
      if (COMMIT_ENDS.has(end) && page) {
        this.hist.pending = null;
        this.hist.token = token;
        this.hist.lastCompleted = { fresh: request.kind === "next" ? request.fresh : request.command || request.fresh, end, token };
        this.emit("history-page", {
          rows: page.rows, end, token, older: page.older, count: page.count, request,
          unreadable: meta.unreadable === true, hadMatches: meta.hadMatches === true,
          countMismatch: page.count != null && page.count !== page.rows.length, overflow: page.overflow,
        });
        return;
      }
      if (end === "busy" && request.command) {
        const delays = this.t.busyRetry;
        if (request.attempt < delays.length) {
          const next = { ...request, attempt: request.attempt + 1 };
          this.hist.pending = next;
          this.emit("history-busy", { attempt: next.attempt, of: delays.length, request: next });
          this.timer("history-retry", delays[request.attempt], () => { if (this.hist.pending === next && !this.closed) this.send(next.command); });
          return;
        }
        this.hist.pending = null;
        this.emit("history-failed", { end, request, reason: "The node stayed busy. Try again in a moment." });
        return;
      }
      if (end === "restart" && request.fresh && request.restarts < 1) {
        // The search was invalidated (for example by a settings change): start it again.
        this.hist.token = null;
        const next = { command: request.fresh, kind: "fresh", fresh: request.fresh, attempt: 0, restarts: request.restarts + 1 };
        this.hist.pending = next;
        this.emit("history-restart", { request: next });
        this.send(next.command);
        return;
      }
      // failed, an exhausted retry budget, or an end we do not know: discard the rows,
      // keep the previous continuation (D18).
      this.hist.pending = null;
      if (end === "restart") this.hist.token = null;
      this.emit("history-failed", { end, request, discarded: page ? page.rows.length : 0, reason: end === "failed" ? "The node could not finish this page." : `History ended with “${end}”.` });
    }
    discardOpenPage(reason) {
      const had = this.hist.open || this.hist.pending;
      this.clear("history-retry");
      const rows = this.hist.open ? this.hist.open.rows.length : 0;
      this.hist.open = null; this.hist.pending = null;
      if (had) this.emit("history-interrupted", { reason, discarded: rows });
    }

    // ------------------------------------------------------------ schema 7 YAML
    requestYaml(text, { requestId, verb, resource }) {
      if (this.state !== "live" || this.mode !== "json") return Promise.reject(Object.assign(new Error("Not connected"), { code: "offline" }));
      return new Promise((resolve, reject) => {
        const entry = { requestId, verb, resource, resolve, reject, sentAt: this.o.now() };
        this.pendingYaml.set(requestId, entry);
        this.timer("yaml:" + requestId, this.t.yaml, () => {
          if (!this.pendingYaml.delete(requestId)) return;
          reject(Object.assign(new Error(`No reply to ${verb} ${resource} within ${Math.round(this.t.yaml / 1000)} s.`), { code: "timeout", requestId, verb }));
        });
        try { this.send(text); } catch (e) { this.pendingYaml.delete(requestId); this.clear("yaml:" + requestId); reject(e); }
      });
    }
    getYaml(resource) {
      const requestId = newRequestId("get");
      return this.requestYaml(getText(resource, requestId), { requestId, verb: "GET", resource });
    }
    patchFilter(args) {
      const requestId = newRequestId("flt");
      return this.requestYaml(patchFilterText({ ...args, requestId }), { requestId, verb: "PATCH", resource: "FILTER" });
    }
    patchSettings(args) {
      const requestId = newRequestId("set");
      return this.requestYaml(patchSettingsText({ ...args, requestId }), { requestId, verb: "PATCH", resource: "SETTINGS" });
    }
    requestBuildVersion() {
      if (this.buildRequested) return;
      this.buildRequested = true;
      this.timer("build", 8000, () => {});
      try { this.send("SHOW BUILD"); } catch { this.clear("build"); }
    }
    onYaml(text) {
      let doc;
      try { doc = this.o.parseYaml(String(text)); } catch (e) {
        this.emit("protocol-error", { reason: "YAML did not parse: " + e.message });
        return;
      }
      if (!doc || typeof doc !== "object") { this.emit("protocol-error", { reason: "YAML document is not a mapping" }); return; }
      const rid = doc.request_id == null ? "" : String(doc.request_id);
      let entry = rid ? this.pendingYaml.get(rid) : null;
      if (!entry && !rid && this.pendingYaml.size) entry = this.pendingYaml.values().next().value; // a schema 6 or framing error may not echo the id
      if (!entry) { this.emit("yaml-unmatched", { doc, text }); return; }
      this.pendingYaml.delete(entry.requestId);
      this.clear("yaml:" + entry.requestId);
      if (doc.error) {
        const e = typeof doc.error === "object" ? doc.error : { code: "error", message: String(doc.error) };
        entry.reject(Object.assign(new Error(e.message || e.code || "The node refused the request."), { code: e.code || "error", doc, text, requestId: entry.requestId, verb: entry.verb }));
        return;
      }
      if (doc.schema_version !== 7) {
        entry.reject(Object.assign(new Error(`Unexpected schema_version ${doc.schema_version}`), { code: "schema", doc, text }));
        return;
      }
      if (entry.verb !== "GET") {
        const r = doc.result || {};
        if (!(r.valid && r.applied)) {
          entry.reject(Object.assign(new Error("The node did not apply the change."), { code: "not-applied", doc, text, verb: entry.verb }));
          return;
        }
      }
      entry.resolve({ doc, text });
    }

    // ------------------------------------------------------------ text observations
    // Human command replies are plain text lines; these few are recognised so the
    // console can refresh its views. Nothing here is required for correctness.
    observeText(line) {
      const t = line.trim();
      let m;
      if (this.timers.build != null && (m = /^Build version: (\S.{0,127})$/.exec(t))) {
        this.clear("build");
        this.emit("build-version", m[1]);
      }
      if ((m = /^Saved presets for (\S+) \((\d+)\/(\d+)\):$/.exec(t))) {
        this.flushPresetCollect();
        this.presetCollect = { owner: m[1], limit: Number(m[3]), names: [] };
        this.timer("presets", 400, () => this.flushPresetCollect());
        return;
      }
      if (this.presetCollect) {
        const row = /^([A-Z0-9][A-Z0-9-]{0,31})(?: \((GO|CC)\))?$/.exec(t);
        if (row || t === "(none)") {
          if (row) this.presetCollect.names.push({ name: row[1], dialect: (row[2] || "GO").toLowerCase() });
          this.timer("presets", 400, () => this.flushPresetCollect());
          return;
        }
        this.flushPresetCollect();
      }
      if ((m = /^(Loaded|Saved|Deleted) preset (\S+?)[.( ,]/.exec(t))) this.emit("preset-reply", { ok: !/not found|could not/i.test(t), verb: m[1].toUpperCase(), name: m[2], line: t });
      else if (/^(SAVE|LOAD|DELETE|LIST) PRESET failed:|^Saved preset \S+ not found\./.test(t)) this.emit("preset-reply", { ok: false, line: t });
      else if (t === "Spot queued." || t === "Spot rejected.") this.emit("post-reply", { ok: t === "Spot queued.", line: t });
    }
    // A list is complete when a different line arrives or 400 ms pass without a row.
    flushPresetCollect() {
      if (!this.presetCollect) return;
      this.clear("presets");
      const p = this.presetCollect; this.presetCollect = null;
      this.emit("presets", p);
    }

    // ------------------------------------------------------------ teardown
    close(reason) {
      if (this.closed) return;
      this.discardOpenPage(reason || "disconnected");
      for (const [id, e] of this.pendingYaml) {
        this.clear("yaml:" + id);
        e.reject(Object.assign(new Error(`Connection lost before the node answered ${e.verb} ${e.resource}.`), { code: "disconnected", verb: e.verb, requestId: id }));
      }
      this.pendingYaml.clear();
      for (const k of Object.keys(this.timers)) this.clear(k);
      this.hist.token = null; // continuations belong to the connection
      this.closed = true;
    }
  }

  return { Session, splitLines, cleanText, nsToMs, decodeSpot, decodeStation, indexOf, configFromDoc, patchFilterText, patchSettingsText, getText, uploadText, newRequestId, PATTERNS, ACK, NEGOTIATE, SWITCH_FIELD, SETTING_FIELD };
})();

if (typeof module !== "undefined" && module.exports) module.exports = GCP;

// GoCluster UI — connection adapter: Live (WebSocket) and Replay (captured JSONL).
//
// The console's views talk to one object, `cluster`, with the surface the
// preview simulator used to provide. Live opens a secure WebSocket on this
// page's own host at the deployment's socket path, logs in, negotiates JSON v1
// and loads the profile with GET YAML CONFIG SCHEMA 7. Replay feeds a capture
// through the same decoder and views and never sends anything.
//
// Deployment settings are <meta> tags in index.html, so the page needs no
// inline script: gocluster-socket-path (default /ws), gocluster-replay-url,
// gocluster-startup-history and gocluster-access-check.
"use strict";

const CONN = (() => {
  const meta = n => { const el = document.querySelector(`meta[name="${n}"]`); return el ? el.getAttribute("content") : null; };
  const DEPLOY = {
    socketPath: meta("gocluster-socket-path") || "/ws",
    replayUrl: meta("gocluster-replay-url") || "replay/session.jsonl",
    startupHistory: Math.max(0, Math.min(250, Number(meta("gocluster-startup-history") || 5) || 0)),
    accessCheck: (meta("gocluster-access-check") || "on") !== "off", // Cloudflare Access in front of the page
  };
  function socketUrl() {
    const p = DEPLOY.socketPath.startsWith("/") ? DEPLOY.socketPath : "/" + DEPLOY.socketPath;
    return (location.protocol === "https:" ? "wss://" : "ws://") + location.host + p;
  }
  const RETRY_DELAYS = [1, 2, 5, 10, 20, 30, 60, 60]; // seconds, then stop and offer Retry
  const MAX_SAMPLE = 4000;     // received spots kept for previews and the drawer
  const MAX_HISTORY_ROWS = 1000;
  const REPLAY_STEP_MS = 350;
  const FATAL = new Set(["evicted", "login-refused", "json-refused", "version"]);
  const READ_ONLY = "Replay is read-only. Connect Live to change filters, settings or presets.";

  // ---------------------------------------------------------------- helpers shared with the views
  const baseCallOf = c => String(c || "").toUpperCase().replace(/-\d{1,2}$/, "");
  function gridLatLon(g) {
    const s = (g || "").toUpperCase();
    if (!/^[A-R]{2}[0-9]{2}/.test(s)) return null;
    return { lat: (s.charCodeAt(1) - 65) * 10 - 90 + Number(s[3]) + 0.5, lon: (s.charCodeAt(0) - 65) * 20 - 180 + Number(s[2]) * 2 + 1 };
  }
  // Approximation used only for the in-browser rule preview; the node decides NEARBY.
  function nearbyFor(userGrid) {
    const u = gridLatLon(userGrid);
    return spot => {
      if (!u) return false;
      const lim = ["160m", "80m", "60m"].includes(spot.band) ? 6 : 2.2;
      for (const g of [spot.dxGrid, spot.deGrid]) {
        const p = gridLatLon(g);
        if (p && Math.abs(p.lat - u.lat) <= lim && Math.abs(p.lon - u.lon) <= lim * 2) return true;
      }
      return false;
    };
  }
  function entityFor(call) {
    const c = baseCallOf(call);
    let best = null;
    for (const e of DATA.ENTITIES) for (const p of e[4]) if (c.startsWith(p) && (!best || p.length > best[1].length)) best = [e, p];
    return best ? best[0] : null;
  }
  function solarFrom(text) {
    const g = re => { const m = re.exec(text); return m ? Number(m[1]) : null; };
    const v = { sfi: g(/\bSFI\s*=\s*(\d+)/i), a: g(/\bA\s*=\s*(\d+)/), k: g(/\bK\s*=\s*(\d+)/) };
    return v.sfi != null || v.k != null ? v : null;
  }
  const END_WORDS = { exhausted: "no more matching spots", count: "more may be available", budget: "the node stopped at its search limit; older spots may remain" };

  // ================================================================ Cluster
  class Cluster {
    constructor() {
      this.handlers = {};
      this.deploy = DEPLOY;
      this.mode = "off";          // off | live | replay
      this.phase = "idle";        // idle | connecting | login | negotiating | live | disconnected | failed | replaying | replay-done
      this.error = null;          // { code, message }
      this.loginCall = LS.get("gcui.login", "");
      this.nodeCall = ""; this.hello = null; this.buildVersion = "";
      this.visitorIP = ""; this.visitorRequest = null;
      this.loggerConnected = false;
      this.cfg = null; this.cfgDoc = ""; this.cfgAt = 0; this.cfgError = "";
      this.presets = []; this.presetsLoaded = false;
      this.sample = [];
      this.session = null; this.ws = null;
      this.retry = { attempt: 0, timer: null, at: 0 };
      this.manual = false; this.fatal = false; this.stale = false; this.accessExpired = false;
      this.pausedUntil = 0;
      this.writing = ""; this.needsReview = false;
      this.history = { status: "idle", note: "", command: "", pages: [], token: null, source: "" };
      this.replay = null;
      this.solar = { sfi: null, a: null, k: null };
      this.spotBuf = []; this.spotTimer = null;
      this.waiters = [];
      this.refreshTimer = null; this.refreshPending = false; this.refreshRetries = 0; this.profileRead = null;
      this.nearby = () => false; this.operating = { cont: "", dxcc: 0 };
      this.errorLines = 0;
    }
    on(ev, fn) { (this.handlers[ev] = this.handlers[ev] || []).push(fn); return () => { this.handlers[ev] = this.handlers[ev].filter(f => f !== fn); }; }
    emit(ev, data) { for (const fn of this.handlers[ev] || []) fn(data); }
    line(dir, text) { this.emit("line", { dir, text, t: Date.now() }); }
    changed() { this.emit("state", this.snapshot()); }
    setPhase(p) { this.phase = p; this.changed(); }

    get baseCall() { return baseCallOf(this.loginCall); }
    get live() { return this.mode === "live" && this.phase === "live" && this.session && !this.session.closed; }
    get canWrite() { return this.live && !!this.cfg && !this.writing && !this.profileRead; }

    snapshot() {
      const c = this.cfg;
      return {
        revision: c ? c.revision : "", loginCall: this.loginCall || "", baseCall: this.baseCall, nodeCall: this.nodeCall || "GoCluster",
        rules: c ? c.rules.map(r => ({ ...r })) : [],
        switches: c ? { ...c.switches } : { beacon: "DEFAULT", self: "DEFAULT", announce: "DEFAULT", wwv: "DEFAULT", wcy: "DEFAULT", toxic: "DEFAULT" },
        settings: c ? { ...c.settings } : { dialect: "", grid: "", noise: "", dedupe: "", pathSamples: 0, solar: 0, cwSpeed: false },
        operatingGrid: c?.settings.grid || c?.effective.grid || "",
        operatingGridDerived: !c?.settings.grid && c?.effective.grid_derived === true,
        preset: c ? { ...c.preset } : { name: "", modified: false },
        presets: this.presets.slice(),
        operating: this.operating,
        session: { connected: this.phase === "live" || this.phase === "replaying", diag: "OFF", pausedUntil: this.pausedUntil, suppressed: 0 },
        server: {
          dedupe: { FAST: true, MED: true, SLOW: true },
          pathMin: { stations: c && c.server.path_min_observation_count != null ? c.server.path_min_observation_count : null, beacons: null },
          switchDefaults: DATA.SWITCH_DEFAULTS, presetLimit: 20,
        },
        conn: {
          mode: this.mode, phase: this.phase, error: this.error, stale: this.stale, accessExpired: this.accessExpired,
          retryAt: this.retry.timer ? this.retry.at : 0, attempt: this.retry.attempt, maxAttempts: RETRY_DELAYS.length,
          configLoaded: !!c, cfgError: this.cfgError, activeDialect: c ? c.activeDialect : "", writing: this.writing, needsReview: this.needsReview,
          replay: this.replay ? { name: this.replay.name, total: this.replay.total, done: this.replay.done } : null,
          socketUrl: socketUrl(), canWrite: this.canWrite, hello: this.hello, buildVersion: this.buildVersion, visitorIP: this.visitorIP,
          loggerConnected: this.loggerConnected,
        },
      };
    }

    // ------------------------------------------------------------ Live
    connectLive(call) {
      call = String(call || "").toUpperCase().trim();
      if (!call) throw new Error("Enter your callsign.");
      this.stopReplay();
      this.cancelRetry();
      if (this.ws) this.closeSocket();
      if (this.mode !== "live" || call !== this.loginCall) this.resetData();
      this.loginCall = call; LS.set("gcui.login", call);
      this.mode = "live"; this.manual = false; this.fatal = false; this.accessExpired = false; this.error = null;
      this.retry.attempt = 0;
      this.open();
    }
    open() {
      this.setPhase("connecting");
      let ws;
      try { ws = new WebSocket(socketUrl()); }
      catch (e) { this.error = { code: "socket", message: e.message }; this.onSocketGone(false, 0, ""); return; }
      this.ws = ws;
      let opened = false;
      const session = this.session = new GCP.Session({
        send: t => { if (ws.readyState !== 1) throw new Error("Not connected"); ws.send(t); },
        emit: (ev, d) => this.onSession(session, ev, d),
        parseYaml: t => jsyaml.load(t, { schema: jsyaml.CORE_SCHEMA }),
      });
      ws.onopen = () => {
        if (this.ws !== ws) return;
        opened = true;
        this.line("sys", `Connected to ${location.host}. Logging in as ${this.loginCall}.`);
        this.setPhase("login");
        session.start(this.loginCall);
      };
      ws.onmessage = ev => {
        if (this.ws !== ws) return;
        if (typeof ev.data === "string") session.feed(ev.data);
        else this.line("sys", "Ignored a binary message from the server.");
      };
      ws.onerror = () => { /* the close event follows */ };
      ws.onclose = ev => { if (this.ws !== ws) return; this.ws = null; this.onSocketGone(opened, ev.code, ev.reason); };
    }
    closeSocket() {
      const ws = this.ws; this.ws = null;
      if (this.session) { this.session.close("closed by the console"); this.session = null; }
      this.clearConnectionViews();
      if (this.writing) { this.needsReview = true; this.writing = ""; }
      if (ws) try { ws.close(1000, "bye"); } catch (e) { /* already closing */ }
    }
    onSocketGone(opened, code, reason) {
      const s = this.session; this.session = null;
      if (s) s.close("connection lost");
      this.clearConnectionViews();
      this.flushSpots();
      this.failWaiters("The connection closed before the node replied.");
      if (this.mode !== "live") return;
      this.stale = this.sample.length > 0 || !!this.cfg;
      if (this.history.status === "loading" || this.history.status === "retrying") this.historyState("interrupted", "The connection dropped before the page finished. Its rows were discarded.");
      if (this.writing) { this.needsReview = true; this.writing = ""; }
      const why = code && code !== 1000 && code !== 1005 ? ` (code ${code}${reason ? ", " + reason : ""})` : "";
      if (this.manual) { this.line("sys", "Disconnected."); this.setPhase("disconnected"); return; }
      if (this.fatal) { this.setPhase("failed"); return; }
      this.line("sys", `Connection closed${why}.`);
      if (!opened) {
        this.checkAccess().then(expired => {
          if (this.mode !== "live" || this.manual || this.ws) return;
          if (expired) {
            this.accessExpired = true;
            this.error = { code: "access", message: "Your Cloudflare sign-in has expired." };
            this.setPhase("disconnected");
          } else this.scheduleRetry();
        });
      } else this.scheduleRetry();
    }
    // A WebSocket to an Access-protected host fails without a status when the
    // Access session has expired. A same-origin request shows the redirect.
    async checkAccess() {
      if (!this.deploy.accessCheck || !/^https?:$/.test(location.protocol)) return false;
      try {
        const r = await fetch(location.origin + "/", { method: "GET", cache: "no-store", redirect: "manual", credentials: "same-origin" });
        return r.type === "opaqueredirect" || r.status === 401 || r.status === 403;
      } catch (e) { return false; }
    }
    scheduleRetry() {
      const n = this.retry.attempt;
      if (n >= RETRY_DELAYS.length) {
        this.error = this.error || { code: "gave-up", message: "Could not reach the cluster." };
        this.setPhase("disconnected");
        return;
      }
      const ms = RETRY_DELAYS[n] * 1000 * (0.85 + Math.random() * 0.3);
      this.retry.attempt = n + 1;
      this.retry.at = Date.now() + ms;
      this.retry.timer = setTimeout(() => { this.retry.timer = null; if (this.mode === "live" && !this.manual) this.open(); }, ms);
      this.setPhase("disconnected");
    }
    cancelRetry() { if (this.retry.timer) clearTimeout(this.retry.timer); this.retry.timer = null; this.retry.at = 0; }
    retryNow() {
      if (!this.loginCall) return;
      this.cancelRetry();
      this.manual = false; this.fatal = false; this.accessExpired = false; this.error = null; this.retry.attempt = 0;
      this.mode = "live";
      if (this.ws) this.closeSocket();
      this.open();
    }
    disconnect() {
      this.cancelRetry();
      if (this.mode === "replay") { this.stopReplay(); this.mode = "off"; this.setPhase("idle"); return; }
      this.manual = true;
      const ws = this.ws;
      if (this.session && this.live) try { this.session.send("BYE"); } catch (e) { /* closing anyway */ }
      this.closeSocket();
      if (ws) this.onSocketGone(true, 1000, "");
      else this.setPhase(this.phase === "idle" ? "idle" : "disconnected");
    }
    resetData() {
      this.clearConnectionViews();
      this.cfg = null; this.cfgDoc = ""; this.cfgError = ""; this.presets = []; this.presetsLoaded = false;
      this.sample = []; this.stale = false; this.needsReview = false; this.pausedUntil = 0;
      this.history = { status: "idle", note: "", command: "", pages: [], token: null, source: "" };
      this.solar = { sfi: null, a: null, k: null }; this.hello = null; this.nodeCall = "";
      this.emit("reset");
    }
    clearConnectionViews() {
      clearTimeout(this.refreshTimer);
      this.refreshTimer = null; this.refreshPending = false; this.refreshRetries = 0; this.profileRead = null;
      this.buildVersion = "";
      this.visitorRequest?.abort(); this.visitorRequest = null; this.visitorIP = "";
      this.loggerConnected = false;
    }

    // ------------------------------------------------------------ session events
    onSession(session, ev, d) {
      if (session !== this.session) return;
      switch (ev) {
        case "state": return this.onSessionState(d);
        case "sent": return this.line("out", d);
        case "text":
          // Login can deliver the first presence notice before the JSON switch.
          if (session.mode === "text" && (session.state === "logging-in" || session.state === "negotiating")) this.observeLoggerPresence(d);
          return this.line("in", d);
        case "negotiated": return this.line("sys", `${d}: JSON v1 active.`);
        case "hello": this.hello = d; this.nodeCall = d.node || this.nodeCall; return;
        case "build-version": this.buildVersion = d; this.changed(); return;
        case "spot": return this.onSpot(d);
        case "bulletin": {
          const sol = /^(WWV|WCY|SOLAR)$/.test(d.kind) ? solarFrom(d.text) : null;
          if (sol) this.solar = { ...this.solar, ...Object.fromEntries(Object.entries(sol).filter(([, v]) => v != null)), at: d.t || Date.now() };
          return this.emit("message", { kind: d.kind, t: d.t || Date.now(), from: d.from || "", text: d.text, solar: sol });
        }
        case "notice":
          if (d.code === "presence-changed") this.observeLoggerPresence(d.text);
          if (d.code === "config-changed" || d.code === "save-status-changed") this.refreshSoon();
          if (d.code === "refused") { this.emit("notice", { text: d.text || "The node refused that request.", kind: "error" }); this.line("sys", "Refused: " + d.text); }
          else if (d.code === "omitted") this.line("sys", "Node notice (omitted): " + d.text);
          else this.line("sys", `Node notice (${d.code}): ${d.text}`);
          return;
        case "json-off":
          this.line("sys", "The node left JSON mode.");
          if (this.mode === "live" && this.live) { this.line("sys", "Switching back to JSON."); this.renegotiate(); }
          return;
        case "presets":
          this.presets = d.names.map(p => ({ name: p.name, dialect: p.dialect }));
          this.presetsLoaded = true; this.changed();
          return;
        case "preset-reply": case "post-reply": return this.resolveWaiter(ev, d);
        case "history-request": return this.historyState("loading", "", d);
        case "history-begin": return;
        case "history-page": return this.onHistoryPage(d);
        case "history-busy": return this.historyState("retrying", `The node is busy. Retrying (${d.attempt} of ${d.of})…`);
        case "history-restart": return this.historyState("loading", "Your settings changed, so the search started again.");
        case "history-failed": return this.historyState("failed", d.reason + (d.discarded ? ` ${d.discarded} partial row${d.discarded === 1 ? "" : "s"} discarded.` : ""), d.request);
        case "history-interrupted": if (this.history.status === "loading" || this.history.status === "retrying") this.historyState("interrupted", "The page was interrupted and its rows were discarded."); return;
        case "protocol-error": if (this.errorLines++ < 20) this.line("sys", "Ignored a malformed record: " + d.reason); return;
        case "yaml-unmatched": this.line("sys", "Received a YAML reply that matched no request."); return;
        default:
      }
    }
    observeLoggerPresence(text) {
      if (this.mode !== "live") return;
      const connected = text === "Logger connected.";
      if (!connected && text !== "Logger disconnected.") return;
      if (this.loggerConnected === connected) return;
      this.loggerConnected = connected;
      this.changed();
    }
    renegotiate() {
      const s = this.session;
      s.hello = null; s.sIdx = null; s.stIdx = null;
      s.setState("negotiating");
      try { s.send(GCP.NEGOTIATE); } catch (e) { /* socket closing */ }
      s.timer("negotiate", s.t.negotiate, () => s.fail("negotiate-timeout", "The cluster did not acknowledge SET JSON ON VERSION 1."));
    }
    onSessionState({ state, detail }) {
      if (this.mode === "replay") {
        if (state === "live") { this.nodeCall = (this.hello && this.hello.node) || "capture"; this.changed(); }
        if (state === "failed") { this.error = detail; this.line("sys", detail.message); this.changed(); }
        return;
      }
      if (state === "await-prompt" || state === "logging-in") return this.setPhase("login");
      if (state === "negotiating" || state === "await-hello") return this.setPhase("negotiating");
      if (state === "live") return this.onLive();
      if (state === "failed") {
        this.error = detail;
        this.line("sys", detail.message);
        if (FATAL.has(detail.code)) { this.fatal = true; this.cancelRetry(); }
        const ws = this.ws;
        if (ws) { try { ws.close(1000, "login failed"); } catch (e) { /* closing */ } }
        if (this.fatal) this.setPhase("failed");
      }
    }
    onLive() {
      this.retry.attempt = 0; this.stale = false; this.error = null; this.accessExpired = false;
      this.line("sys", `Live on ${this.nodeCall || "the node"} as ${(this.hello && this.hello.login) || this.loginCall}.`);
      this.setPhase("live");
      // A new session always reloads the profile from the node.
      this.refreshConfig().catch(() => { /* reported in cfgError */ });
      this.session.requestBuildVersion();
      this.loadVisitorIP();
      this.send("LIST PRESET");
      if (this.deploy.startupHistory > 0 && !this.session.historyBusy) this.historySearch({ count: this.deploy.startupHistory }, true);
    }
    send(text) { if (!this.session || this.session.closed) throw new Error("Not connected."); this.session.send(text); }
    async loadVisitorIP() {
      if (this.visitorRequest || !/^https?:$/.test(location.protocol)) return;
      const request = this.visitorRequest = new AbortController();
      try {
        const response = await fetch("/visitor-ip", { cache: "no-store", redirect: "error", signal: request.signal });
        if (!response.ok) return;
        const { ipv4 } = await response.json();
        if (request !== this.visitorRequest || !this.live || typeof ipv4 !== "string") return;
        this.visitorIP = ipv4;
        this.changed();
      } catch { /* The address is optional; the build version can stand alone. */ }
    }

    // ------------------------------------------------------------ spots
    onSpot(s) {
      if (s.t == null) s.t = Date.now();
      this.spotBuf.push(s);
      if (!this.spotTimer) this.spotTimer = setTimeout(() => this.flushSpots(), 150);
    }
    flushSpots() {
      if (this.spotTimer) clearTimeout(this.spotTimer);
      this.spotTimer = null;
      if (!this.spotBuf.length) return;
      const batch = this.spotBuf; this.spotBuf = [];
      this.sample.push(...batch);
      if (this.sample.length > MAX_SAMPLE) this.sample.splice(0, this.sample.length - MAX_SAMPLE);
      this.emit("sample", batch);
      this.emit("spots", batch);
    }
    sampleFor(call) { return this.sample.filter(s => s.dxCall === call).slice(-30); }
    prop() { return null; }        // SHOW PROP has no structured form in JSON v1
    whoSpotsMe() { return null; }  // WHOSPOTSME replies are text; run it in the console

    // ------------------------------------------------------------ configuration
    async refreshConfig(session = this.session) {
      if (!this.live || session !== this.session) throw Object.assign(new Error("Not connected."), { code: "disconnected" });
      if (this.profileRead) return this.profileRead;
      const read = this.readProfile(session);
      this.profileRead = read;
      this.changed();
      try { return await read; }
      finally {
        if (this.profileRead === read) {
          this.profileRead = null;
          this.changed();
          this.scheduleProfileRefresh();
        }
      }
    }
    async readProfile(session) {
      try {
        const { doc, text } = await session.getYaml("CONFIG");
        if (session !== this.session || session.closed) throw Object.assign(new Error("The connection closed before the profile arrived."), { code: "disconnected" });
        const cfg = GCP.configFromDoc(doc);
        this.cfg = cfg; this.cfgDoc = text; this.cfgAt = Date.now(); this.cfgError = "";
        this.nearby = nearbyFor(cfg.settings.grid || (cfg.effective && cfg.effective.grid) || "");
        const ent = entityFor(this.loginCall);
        this.operating = { cont: ent ? ent[3] : "", dxcc: ent ? ent[1] : 0 };
        return cfg;
      } catch (e) {
        if (session === this.session && !session.closed) this.cfgError = e.message;
        throw e;
      }
    }
    refreshSoon() {
      if (!this.live) return;
      this.refreshPending = true;
      this.refreshRetries = 0;
      this.scheduleProfileRefresh();
    }
    scheduleProfileRefresh() {
      if (!this.refreshPending || !this.live || this.refreshTimer !== null || this.writing || this.profileRead) return;
      // Retain one invalidation through a read or a PATCH confirmation. The
      // node holds its response reservation until the writer flushes it.
      const session = this.session;
      this.refreshTimer = setTimeout(async () => {
        if (session !== this.session) return;
        this.refreshTimer = null;
        if (!this.live || this.writing || this.profileRead) return;
        this.refreshPending = false;
        try { await this.refreshConfig(session); }
        catch (error) {
          if (session !== this.session || error.code !== "configuration_busy" || this.refreshRetries >= 2) return;
          this.refreshRetries++;
          this.refreshPending = true;
          this.scheduleProfileRefresh();
        }
      }, 700);
    }
    configYaml() { return this.cfgDoc || "Not loaded yet. Connect Live to read GET YAML CONFIG SCHEMA 7."; }
    guardWrite(baseRev) {
      if (this.mode === "replay") throw new Error(READ_ONLY);
      if (!this.live) throw new Error("Not connected. Reconnect, then try again.");
      if (!this.cfg) throw new Error("Your profile has not loaded yet.");
      if (this.writing) throw new Error("Another change is still being saved.");
      if (this.profileRead) throw new Error("The profile is refreshing. Try again when it finishes.");
      if (baseRev && baseRev !== this.cfg.revision) throw Object.assign(new Error("The node's configuration changed after you started editing. Review the latest version first."), { code: "revision_conflict" });
    }
    async write(kind, fn) {
      const session = this.session;
      this.writing = kind; this.changed();
      try {
        const r = await fn(session);
        if (session === this.session) this.needsReview = false;
        return r;
      }
      catch (e) {
        if (session !== this.session) throw e;
        if (e.code === "revision_conflict") {
          try { await this.refreshConfig(session); }
          catch (reload) { throw new Error("Your profile changed. Reload the profile before retrying. " + reload.message); }
          throw Object.assign(new Error("Your profile changed on the node (another login or the logger). The latest version is now shown. Review your edits and apply again."), { code: e.code });
        }
        if (e.code === "disconnected" || e.code === "timeout") {
          this.needsReview = true;
          if (this.live) await this.refreshConfig(session).catch(() => {});
          throw Object.assign(new Error("The node did not confirm the change. Nothing was resent. Review the latest profile before trying again."), { code: e.code });
        }
        throw e;
      } finally {
        if (session === this.session) {
          this.writing = "";
          this.changed();
          this.scheduleProfileRefresh();
        }
      }
    }
    // The review and save use the same patch. A supplied rule list replaces the GO bank.
    async patchFilter({ rules, switches }, baseRev) {
      this.guardWrite(baseRev);
      if (!baseRev || this.cfgError) throw new Error("Reload the profile before applying filter changes.");
      if (this.cfg.activeDialect && this.cfg.activeDialect !== "go") throw new Error("This login uses the CC dialect. Switch it to GO in Settings before editing rules here.");
      const cur = this.cfg;
      return this.write("filters", async session => {
        await session.patchFilter({ revision: cur.revision, rules, switches });
        const cfg = await this.refreshConfig(session); // confirm by readback
        const okRules = !rules || (rules.length === cfg.rules.length &&
          rules.every(r => sameFilterRule(r, cfg.rules.find(other => other.slot === r.slot))));
        const okSw = Object.entries(switches).every(([k, v]) => cfg.switches[k] === v);
        return { confirmed: okRules && okSw, cfg };
      });
    }
    async patchSettings(patch, baseRev) {
      this.guardWrite(baseRev);
      const cur = this.cfg;
      const send = {};
      for (const [k, v] of Object.entries(patch)) if (v !== cur.settings[k]) send[k] = v;
      if (!Object.keys(send).length) return this.snapshot();
      return this.write("settings", async session => {
        await session.patchSettings({ revision: cur.revision, settings: send });
        const cfg = await this.refreshConfig(session);
        if (send.grid != null || send.dedupe) this.sample = this.sample.slice();
        const confirmed = Object.entries(send).every(([k, v]) => cfg.settings[k] === v);
        return { ...this.snapshot(), confirmed };
      });
    }
    switchToGo() { return this.patchSettings({ dialect: "go" }, this.cfg && this.cfg.revision); }

    // ------------------------------------------------------------ presets (human commands)
    waitFor(kind, ms = 8000) {
      return new Promise(resolve => {
        const w = { kind, resolve, timer: setTimeout(() => { this.waiters = this.waiters.filter(x => x !== w); resolve({ ok: null, line: "" }); }, ms) };
        this.waiters.push(w);
      });
    }
    resolveWaiter(kind, d) {
      const w = this.waiters.find(x => x.kind === kind);
      if (!w) return;
      clearTimeout(w.timer); this.waiters = this.waiters.filter(x => x !== w); w.resolve(d);
    }
    failWaiters(message) { for (const w of this.waiters) { clearTimeout(w.timer); w.resolve({ ok: false, line: message }); } this.waiters = []; }
    async presetCommand(verb, name) {
      if (this.mode === "replay") throw new Error(READ_ONLY);
      if (!this.live) throw new Error("Not connected.");
      const wait = this.waitFor("preset-reply");
      this.send(`${verb} PRESET ${name}`);
      const r = await wait;
      if (r.ok === false) throw new Error(r.line || `${verb} PRESET ${name} failed.`);
      await this.refreshConfig().catch(() => {});
      try { this.send("LIST PRESET"); } catch (e) { /* offline */ }
      if (r.ok === null) throw new Error(`No reply to ${verb} PRESET ${name} yet. The list has been reloaded; check it before trying again.`);
      return r;
    }
    savePreset(n) { return this.presetCommand("SAVE", n); }
    loadPreset(n) { return this.presetCommand("LOAD", n); }
    deletePreset(n) { return this.presetCommand("DELETE", n); }

    // ------------------------------------------------------------ history
    historyState(status, note, request) {
      this.history = { ...this.history, status, note: note || "" };
      if (request && request.kind !== "next" && request.command) this.history.command = request.fresh || request.command;
      this.emit("history", this.history);
      this.changed();
    }
    historySearch({ selector = "", count = 20 } = {}, quiet) {
      const n = Math.max(1, Math.min(250, Number(count) || 20));
      const sel = String(selector || "").trim().toUpperCase();
      if (sel && !/^[A-Z0-9/]{1,20}$/.test(sel)) throw new Error("Use a callsign, prefix or country label without spaces.");
      return this.historyCommand(`SH DX ${sel ? sel + " " : ""}${n}`, quiet);
    }
    historyCommand(command, quiet) {
      if (this.mode === "replay") throw new Error(READ_ONLY);
      if (!this.live) throw new Error("Not connected.");
      if (this.session.historyBusy) throw new Error("A history page is still loading.");
      this.history.source = quiet ? "startup" : "user";
      this.session.requestHistory(command, "fresh", command);
    }
    historyOlder() {
      if (!this.live) throw new Error("Not connected.");
      this.session.requestOlder();
    }
    onHistoryPage(p) {
      const page = { rows: p.rows, end: p.end, token: p.token, older: p.older, count: p.count, unreadable: p.unreadable, countMismatch: p.countMismatch, overflow: p.overflow, at: Date.now(), replay: this.mode === "replay" };
      let pages = p.request.kind === "next" ? [...this.history.pages, page] : [page];
      let total = pages.reduce((a, x) => a + x.rows.length, 0);
      while (pages.length > 1 && total > MAX_HISTORY_ROWS) { total -= pages[0].rows.length; pages = pages.slice(1); }
      const cmd = p.request.kind === "next" ? this.history.command : (p.request.command || "");
      this.history = { status: "done", note: "", command: cmd, pages, token: p.token, source: this.history.source };
      const n = p.rows.length;
      this.line("sys", `History${cmd ? " (" + cmd + ")" : ""}: ${n} spot${n === 1 ? "" : "s"}${p.older ? ", older page" : ""}; ${END_WORDS[p.end] || p.end}.${p.unreadable ? " Some archived records could not be read." : ""}${p.countMismatch ? ` The node announced ${p.count}.` : ""}`);
      this.emit("history", this.history);
      this.changed();
    }

    // ------------------------------------------------------------ posting, pause, console
    async postSpot(freq, call, comment) {
      if (this.mode === "replay") throw new Error(READ_ONLY);
      const f = Number(freq);
      if (!(f >= 135.7 && f <= 2450000)) throw new Error("Frequency must be in kHz, for example 14025.00.");
      const C = String(call).toUpperCase().trim();
      if (!/^[A-Z0-9/]{3,}$/.test(C)) throw new Error("That does not look like a callsign.");
      if (comment && !/^[A-Za-z0-9 \/#@\-.,*?_+]*$/.test(comment)) throw new Error("Comments accept letters, digits, spaces and / # @ - . , * ? _ +");
      const cmd = `DX ${f.toFixed(2)} ${C}${comment ? " " + comment : ""}`;
      if (cmd.length > 128) throw new Error("The whole command must fit in 128 characters.");
      if (!this.live) throw new Error("Not connected.");
      const wait = this.waitFor("post-reply");
      this.send(cmd);
      const r = await wait;
      if (r.ok === false) throw new Error(r.line === "Spot rejected." ? "The node rejected the spot." : r.line || "The spot was not accepted.");
      return { pass: true, queued: r.ok === true };
    }
    pause(sec) { this.command(`PAUSE ${sec}`); }
    resume() { this.command("RESUME"); }
    setDiag() { /* diagnostics are not part of JSON v1 (D9) */ }
    command(raw) {
      const line = String(raw).trim();
      if (!line) return;
      const U = line.toUpperCase().replace(/\s+/g, " ");
      if (this.mode === "replay") { this.line("out", line); this.line("sys", READ_ONLY); return; }
      if (!this.live) { this.line("out", line); this.line("sys", "Not connected."); return; }
      if (/^SET JSON\b|^(SET|UNSET)\/VE7CC\b/.test(U)) { this.line("out", line); this.line("sys", "The console manages JSON mode itself. That command is not sent."); return; }
      if (/^(SH|SHOW)[ /](DX|MYDX)\b/.test(U)) {
        try {
          if (/^(SH|SHOW)[ /](DX|MYDX) NEXT\b/.test(U)) this.historyOlder();
          else this.historyCommand(line);
          this.emit("show-history");
        } catch (e) { this.line("out", line); this.line("sys", e.message); }
        return;
      }
      if (U === "BYE" || U === "QUIT") { this.disconnect(); return; }
      let m;
      if ((m = /^PAUSE(?: (\d+))?$/.exec(U))) this.pausedUntil = Date.now() + Math.max(1, Math.min(300, Number(m[1] || 30))) * 1000;
      if (U === "RESUME") this.pausedUntil = 0;
      try { this.send(line); } catch (e) { this.line("sys", e.message); return; }
      if (/^(PASS|REJECT|RESET|SET|UNSET|LOAD|SAVE|DELETE|DIALECT)\b/.test(U)) this.refreshSoon();
      if (/^(SAVE|LOAD|DELETE) PRESET\b/.test(U)) setTimeout(() => { try { this.send("LIST PRESET"); } catch (e) { /* offline */ } }, 800);
      this.changed();
    }

    // ------------------------------------------------------------ Replay
    async startReplayFromUrl() {
      let text;
      try {
        const r = await fetch(this.deploy.replayUrl, { cache: "no-store" });
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        text = await r.text();
      } catch (e) { throw new Error(`Could not load the bundled capture (${e.message}). Choose the .jsonl file instead.`); }
      this.startReplay(text, this.deploy.replayUrl.split("/").pop());
    }
    startReplay(text, name) {
      this.cancelRetry();
      if (this.ws) { this.manual = true; this.closeSocket(); }
      this.stopReplay();
      this.resetData();
      const records = String(text).split("\n").map(l => l.replace(/\r$/, "")).filter(l => l.trim());
      if (!records.length) throw new Error("That file has no records.");
      this.mode = "replay"; this.error = null; this.manual = false;
      const session = this.session = new GCP.Session({
        send: () => { throw new Error(READ_ONLY); },
        emit: (ev, d) => this.onSession(session, ev, d),
        parseYaml: t => jsyaml.load(t, { schema: jsyaml.CORE_SCHEMA }),
      });
      session.startInJson(); // captures start after the JSON ON 1 acknowledgement
      this.replay = { name: name || "capture", total: records.length, done: 0, timer: null };
      this.line("sys", `Replaying ${this.replay.name}: ${records.length} records. Nothing is sent to a cluster.`);
      this.setPhase("replaying");
      const step = () => {
        if (!this.replay || this.session !== session) return;
        session.feed(records[this.replay.done] + "\n");
        this.replay.done++;
        if (this.replay.done >= records.length) {
          this.flushSpots();
          this.replay.timer = null;
          this.line("sys", "Replay finished.");
          this.setPhase("replay-done");
          return;
        }
        this.changed();
        this.replay.timer = setTimeout(step, REPLAY_STEP_MS);
      };
      this.replay.timer = setTimeout(step, REPLAY_STEP_MS);
    }
    stopReplay() {
      if (!this.replay) return;
      clearTimeout(this.replay.timer);
      if (this.session && this.mode === "replay") { this.session.close("replay stopped"); this.session = null; }
      this.replay = null;
    }
  }

  // 76-column ordinary-text spot line, kept for copy actions.
  function spotLine(s) {
    const de = ("DX de " + s.deCall + ":").padEnd(16);
    const f = s.freq.toFixed(2).padStart(9);
    const mid = `${s.mode}${s.snr != null ? " " + s.snr + " dB" : ""} ${s.comment}`.slice(0, 24).padEnd(24);
    const tm = new Date(s.t);
    return `${de}${f}  ${s.dxCall.padEnd(12)} ${mid} ${s.glyph || " "} ${(s.dxGrid || "").slice(0, 4).padEnd(4)} ${s.conf || " "} ${String(tm.getUTCHours()).padStart(2, "0")}${String(tm.getUTCMinutes()).padStart(2, "0")}Z`;
  }

  return { Cluster, DEPLOY, socketUrl, baseCallOf, gridLatLon, spotLine, END_WORDS };
})();

// Helpers the views used to take from the simulator.
const SIM = { baseCallOf: CONN.baseCallOf, gridLatLon: CONN.gridLatLon, spotLine: CONN.spotLine };

// GoCluster UI — store, formatting helpers and icons.
"use strict";

const { html, render, useState, useEffect, useLayoutEffect, useRef, useMemo, useCallback } = htmPreact;

// ---------------------------------------------------------------- local persistence (one browser only)
const LS = {
  get(k, d) { try { const v = localStorage.getItem(k); return v ? JSON.parse(v) : d; } catch (e) { return d; } },
  set(k, v) { try { localStorage.setItem(k, JSON.stringify(v)); } catch (e) { /* storage unavailable */ } },
};
const DEFAULT_PREFS = {
  theme: "system", contrast: "system", textSize: "normal", density: "comfortable", timeMode: "utc", rowCap: 250, watch: ["VP8", "ZD7", "T32"],
  groupRepeats: true,
  distanceUnit: "km", colOrder: [], colShow: {}, colWidths: {},
};

// ---------------------------------------------------------------- the store
const S = {
  snap: null, spots: [], messages: [], lines: [],
  view: "stream", focusBand: "ALL", focusCont: "", search: "",
  drawer: null, consoleOpen: false, selSpot: null, postOpen: false,
  draft: null, filterTab: "rules", editing: null, showHidden: false, connectOpen: false,
  freezeId: 0, prefs: { ...DEFAULT_PREFS, ...LS.get("gcui.prefs", {}) },
  toasts: [], rate: [], now: Date.now(),
};
if (S.prefs.cols) { // v1 visibility keys
  const m = { comment: "cmt", spotter: "de", where: "where", report: "rep" };
  S.prefs.colShow = { ...(S.prefs.colShow || {}) };
  for (const [o, n] of Object.entries(m)) if (S.prefs.cols[o] === false) S.prefs.colShow[n] = false;
  delete S.prefs.cols;
}
S.prefs.colShow = S.prefs.colShow || {};
const subs = new Set();
let renderQueued = false, lastRender = 0;
function notify(immediate) {
  if (renderQueued) return;
  renderQueued = true;
  const wait = immediate ? 0 : Math.max(0, 350 - (performance.now() - lastRender));
  setTimeout(() => requestAnimationFrame(() => { renderQueued = false; lastRender = performance.now(); S.now = Date.now(); for (const f of subs) f(); }), wait);
}
function set(patch) { Object.assign(S, typeof patch === "function" ? patch(S) : patch); notify(true); }
function useStore() {
  const [, force] = useState(0);
  useEffect(() => { const f = () => force(n => n + 1); subs.add(f); return () => subs.delete(f); }, []);
  return S;
}
function setPrefs(p) { S.prefs = { ...S.prefs, ...p }; LS.set("gcui.prefs", S.prefs); applyTheme(); notify(true); }

let toastSeq = 0;
function toast(text, kind = "ok", action) {
  const id = ++toastSeq;
  S.toasts = [...S.toasts, { id, text, kind, action }].slice(-4);
  notify(true);
  setTimeout(() => { S.toasts = S.toasts.filter(t => t.id !== id); notify(true); }, kind === "error" ? 7000 : 4200);
}

// ---------------------------------------------------------------- theme
function effectiveTheme() {
  const p = S.prefs.theme;
  if (p === "light" || p === "dark") return p;
  const host = document.documentElement.getAttribute("data-theme");
  if (host === "light" || host === "dark") return host;
  return window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
}
function effectiveContrast() {
  const p = S.prefs.contrast;
  if (p === "high" || p === "standard") return p;
  return window.matchMedia && window.matchMedia("(prefers-contrast: more)").matches ? "high" : "standard";
}
const TEXT_SCALE = { normal: 1, large: 1.15, xl: 1.3 };
function applyTheme() {
  const el = document.documentElement;
  el.setAttribute("data-ui", effectiveTheme());
  el.setAttribute("data-contrast", effectiveContrast());
  el.setAttribute("data-text", S.prefs.textSize || "normal");
  const root = document.getElementById("root");
  if (root) root.style.zoom = String(TEXT_SCALE[S.prefs.textSize] || 1);
}

// ---------------------------------------------------------------- formatting
const pad2 = n => String(n).padStart(2, "0");
function fmtTime(t, mode = S.prefs.timeMode) {
  if (t == null) return "—";
  const d = new Date(t);
  return mode === "local" ? `${pad2(d.getHours())}:${pad2(d.getMinutes())}` : `${pad2(d.getUTCHours())}${pad2(d.getUTCMinutes())}Z`;
}
function fmtAge(t) {
  if (t == null) return "";
  const s = Math.max(0, Math.round((S.now - t) / 1000));
  if (s < 60) return `${s}s`;
  if (s < 3600) return `${Math.floor(s / 60)}m`;
  return `${Math.floor(s / 3600)}h`;
}
const fmtFreq = f => f.toFixed(2);
const fmtReport = s => (s.snr == null ? "" : (s.snr > 0 && ["FT8", "FT4", "FT2", "JS8"].includes(s.mode) ? "+" : "") + s.snr);
const plural = (n, w, p) => `${n.toLocaleString()} ${n === 1 ? w : (p || w + "s")}`;
const cls = (...a) => a.filter(Boolean).join(" ");
async function copyText(text, what = "Copied") {
  try { await navigator.clipboard.writeText(text); toast(`${what} to clipboard`); }
  catch (e) { toast("Your browser blocked the clipboard. Select the text and copy it manually.", "error"); }
}
const BAND_COLOR = {
  "160m": "#8b5cf6", "80m": "#6366f1", "60m": "#4f7df0", "40m": "#3b82f6", "30m": "#0ea5c6", "20m": "#10b981",
  "17m": "#65b32e", "15m": "#d4a80b", "12m": "#f59e0b", "10m": "#f97316", "6m": "#ef4444", "2m": "#ec4899",
};

function watchHit(call) {
  const w = S.prefs.watch || [];
  return w.some(p => p && (call === p || (call.startsWith(p) && p.length >= 2)));
}

// "PASS 0" / "REJECT 9" / "SELF" / "no PASS matched" -> words
function reasonWords(by, cards) {
  if (!by) return "";
  let m;
  if ((m = /^PASS (\d)$/.exec(by))) {
    const c = cards && cards.find(x => x.slot === Number(m[1]));
    return `Show rule ${m[1]}${c ? ": " + RULES.cardSentence(c) : ""}`;
  }
  if ((m = /^REJECT (\d)$/.exec(by))) {
    const c = cards && cards.find(x => x.slot === Number(m[1]));
    return `Hide rule ${m[1]}${c ? ": " + RULES.cardSentence(c) : ""}`;
  }
  return {
    "SELF": "Spot of your own call (own-call switch is on)",
    "no PASS matched": "No Show rule matched",
    "no PASS rules": "No Show rules, and no Hide rule matched",
    "no rules": "You have no rules, so every spot passes",
    "BEACON switch": "Beacon spots are switched off",
    "TOXIC switch": "Flagged comments are switched off",
  }[by] || by;
}

// ---------------------------------------------------------------- icons
const ICONS = {
  filter: "M3 5h18l-7 8v5l-4 2v-7L3 5Z",
  sliders: "M4 7h10M18 7h2M4 17h2M10 17h10M16 5v4M8 15v4",
  gear: "M12 15.5a3.5 3.5 0 1 0 0-7 3.5 3.5 0 0 0 0 7Z M19.4 15a1.7 1.7 0 0 0 .3 1.8l.1.1a2 2 0 1 1-2.8 2.8l-.1-.1a1.7 1.7 0 0 0-1.8-.3 1.7 1.7 0 0 0-1 1.5V21a2 2 0 1 1-4 0v-.1a1.7 1.7 0 0 0-1.1-1.5 1.7 1.7 0 0 0-1.8.3l-.1.1a2 2 0 1 1-2.8-2.8l.1-.1a1.7 1.7 0 0 0 .3-1.8 1.7 1.7 0 0 0-1.5-1H3a2 2 0 1 1 0-4h.1a1.7 1.7 0 0 0 1.5-1.1 1.7 1.7 0 0 0-.3-1.8l-.1-.1a2 2 0 1 1 2.8-2.8l.1.1a1.7 1.7 0 0 0 1.8.3H9a1.7 1.7 0 0 0 1-1.5V3a2 2 0 1 1 4 0v.1a1.7 1.7 0 0 0 1 1.5 1.7 1.7 0 0 0 1.8-.3l.1-.1a2 2 0 1 1 2.8 2.8l-.1.1a1.7 1.7 0 0 0-.3 1.8V9a1.7 1.7 0 0 0 1.5 1H21a2 2 0 1 1 0 4h-.1a1.7 1.7 0 0 0-1.5 1Z",
  pulse: "M3 12h4l3-8 4 16 3-8h4",
  terminal: "M4 6l6 6-6 6M12 19h8",
  globe: "M12 22a10 10 0 1 0 0-20 10 10 0 0 0 0 20ZM2 12h20M12 2c5 5.5 5 14.5 0 20-5-5.5-5-14.5 0-20Z",
  sync: "M3 7h18l-4-4M21 7l-4 4M21 17H3l4-4M3 17l4 4",
  plus: "M12 5v14M5 12h14",
  close: "M6 6l12 12M18 6L6 18",
  search: "M11 18a7 7 0 1 0 0-14 7 7 0 0 0 0 14ZM20 20l-4-4",
  sun: "M12 16a4 4 0 1 0 0-8 4 4 0 0 0 0 8ZM12 2v2M12 20v2M4.9 4.9l1.4 1.4M17.7 17.7l1.4 1.4M2 12h2M20 12h2M4.9 19.1l1.4-1.4M17.7 6.3l1.4-1.4",
  moon: "M20 14.5A8 8 0 0 1 9.5 4a8 8 0 1 0 10.5 10.5Z",
  monitor: "M3 5h18v11H3zM8 20h8M12 16v4",
  chevron: "M9 6l6 6-6 6",
  chevronDown: "M6 9l6 6 6-6",
  trash: "M4 7h16M10 11v6M14 11v6M6 7l1 13h10l1-13M9 7V4h6v3",
  copy: "M9 9h11v11H9zM5 15H4V4h11v1",
  check: "M5 12l5 5L20 7",
  pause: "M8 5v14M16 5v14",
  play: "M7 5l12 7-12 7z",
  eye: "M2 12s3.6-7 10-7 10 7 10 7-3.6 7-10 7S2 12 2 12Z M12 15a3 3 0 1 0 0-6 3 3 0 0 0 0 6Z",
  eyeOff: "M3 3l18 18M10.6 6.1A10 10 0 0 1 12 6c6.4 0 10 6 10 6a17 17 0 0 1-3.2 3.9M6.6 6.6C3.8 8.4 2 12 2 12s3.6 7 10 7a9.6 9.6 0 0 0 4.4-1.1M9.9 10a3 3 0 0 0 4.1 4.1",
  info: "M12 22a10 10 0 1 0 0-20 10 10 0 0 0 0 20ZM12 16v-5M12 8h.01",
  warn: "M12 3l10 18H2L12 3ZM12 10v5M12 18h.01",
  error: "M12 22a10 10 0 1 0 0-20 10 10 0 0 0 0 20ZM15 9l-6 6M9 9l6 6",
  spark: "M12 3l1.9 5.6L19.5 10l-5.6 1.9L12 17.5l-1.9-5.6L4.5 10l5.6-1.4z",
  history: "M3 12a9 9 0 1 0 3-6.7L3 8M3 3v5h5M12 7v5l3 2",
  bookmark: "M6 3h12v18l-6-4-6 4z",
  radio: "M4.9 19.1a10 10 0 0 1 0-14.2M19.1 4.9a10 10 0 0 1 0 14.2M7.8 16.2a6 6 0 0 1 0-8.4M16.2 7.8a6 6 0 0 1 0 8.4M12 13a1 1 0 1 0 0-2 1 1 0 0 0 0 2Z",
  map: "M3 6l6-3 6 3 6-3v15l-6 3-6-3-6 3z M9 3v15M15 6v15",
  grid: "M3 3h7v7H3zM14 3h7v7h-7zM14 14h7v7h-7zM3 14h7v7H3z",
  list: "M8 6h13M8 12h13M8 18h13M3 6h.01M3 12h.01M3 18h.01",
  code: "M8 8l-5 4 5 4M16 8l5 4-5 4",
  dots: "M5 12h.01M12 12h.01M19 12h.01",
  undo: "M9 14L4 9l5-5M4 9h10a6 6 0 0 1 0 12h-3",
  user: "M12 12a4 4 0 1 0 0-8 4 4 0 0 0 0 8ZM4 21a8 8 0 0 1 16 0",
  send: "M22 2L11 13M22 2l-7 20-4-9-9-4z",
  save: "M5 3h11l5 5v13H5zM8 3v5h7M8 14h8v7H8z",
  download: "M12 3v12M7 10l5 5 5-5M5 21h14",
  layers: "M12 3l9 5-9 5-9-5 9-5ZM3 13l9 5 9-5",
  wand: "M15 4V2M15 10V8M11 6h2M17 6h2M3 21l12-12",
};
function Icon({ name, size = 18, stroke = 1.8, title }) {
  return html`<svg class="ic" width=${size} height=${size} viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width=${stroke} stroke-linecap="round" stroke-linejoin="round" aria-hidden=${title ? undefined : "true"} role=${title ? "img" : undefined}>${title ? html`<title>${title}</title>` : null}<path d=${ICONS[name] || ""} /></svg>`;
}
function BrandMark({ size = 28 }) {
  return html`<svg width=${size} height=${size} viewBox="0 0 64 64" aria-hidden="true"><defs><linearGradient id="gcg" x1="0" y1="0" x2="1" y2="1"><stop offset="0" stop-color="#5fd3ff"/><stop offset=".55" stop-color="#0a84d6"/><stop offset="1" stop-color="#7c6cf2"/></linearGradient></defs><rect width="64" height="64" rx="16" fill="url(#gcg)"/><g fill="none" stroke="#fff" stroke-width="4.2" stroke-linecap="round"><path d="M20 44a17 17 0 0 1 0-24" opacity=".55"/><path d="M44 20a17 17 0 0 1 0 24" opacity=".55"/><path d="M26 38a8.5 8.5 0 0 1 0-12"/><path d="M38 26a8.5 8.5 0 0 1 0 12"/></g><circle cx="32" cy="32" r="4.4" fill="#fff"/></svg>`;
}

// ---------------------------------------------------------------- small shared components
function PathGlyph({ cls: c, size = "md", reportedBy }) {
  if (!c) return html`<span class=${"pathg p-none " + size} title="No path rating" role="img" aria-label="No path rating"></span>`;
  const p = DATA.PATH[c] || DATA.PATH.INSUFFICIENT;
  const lvl = DATA.PATH[c] ? p[2] : 0;
  const label = `Path: ${DATA.PATH[c] ? p[1] : c} (${c})${reportedBy ? " · " + reportedBy : ""}`;
  return html`<span class=${"pathg p-" + (DATA.PATH[c] ? c : "INSUFFICIENT").toLowerCase() + " " + size} title=${label} role="img" aria-label=${label}>
    ${[1, 2, 3, 4].map(i => html`<i class=${i <= lvl ? "on" : ""} style=${`height:${3 + i * 2.6}px`}></i>`)}
  </span>`;
}
function ConfBadge({ conf, big, reportedBy }) {
  if (!conf) return html`<span class="conf conf-none" title="Confidence not rated for this mode">·</span>`;
  const d = DATA.CONF[conf];
  const label = d ? `${d[0]}: ${d[1]}` : conf;
  return html`<span class=${cls("conf", "conf-" + (conf === "?" ? "q" : conf.toLowerCase()), big && "big")} title=${reportedBy ? `${label} · ${reportedBy}` : label}>${conf}</span>`;
}
function ModeBadge({ mode }) {
  return html`<span class=${"mode m-" + (DATA.MODE_FAMILY[mode] || "unk")}>${mode === "UNKNOWN" ? "?" : mode}</span>`;
}
function Toggle({ on, onChange, label, id, disabled }) {
  return html`<button type="button" id=${id} class=${cls("toggle", on && "on")} role="switch" aria-checked=${on ? "true" : "false"} aria-label=${label} disabled=${disabled} onClick=${() => onChange(!on)}><i></i></button>`;
}
function Seg({ options, value, onChange, label, small }) {
  return html`<div class=${cls("seg", small && "small")} role="radiogroup" aria-label=${label}>
    ${options.map(([v, l, title]) => html`<button type="button" role="radio" aria-checked=${value === v ? "true" : "false"} class=${value === v ? "on" : ""} title=${title || ""} onClick=${() => onChange(v)}>${l}</button>`)}
  </div>`;
}

// GoCluster UI — filters: draft model, preview, rule cards, presets, switches.
"use strict";

// ---------------------------------------------------------------- draft state
let _cardsRev = "", _cards = [];
function currentCards() {
  if (!S.snap) return [];
  if (_cardsRev !== S.snap.revision) { _cardsRev = S.snap.revision; _cards = RULES.sortCards(S.snap.rules.map(RULES.cardFromRule)); }
  return _cards;
}
const cloneCards = cards => JSON.parse(JSON.stringify(cards));
function workingCards() { return S.draft ? S.draft.cards : currentCards(); }
function workingSwitches() { return S.draft ? S.draft.switches : (S.snap ? S.snap.switches : {}); }
function filterBase(profile) {
  return { revision: profile.revision, rules: profile.rules, switches: profile.switches };
}
function startDraft() {
  if (!S.draft) S.draft = { base: filterBase(S.snap), cards: cloneCards(currentCards()), switches: { ...S.snap.switches }, conflicts: new Map() };
  return S.draft;
}
function editDraft(fn) { const d = startDraft(); fn(d); S.draft = { ...d, cards: RULES.sortCards(d.cards) }; notify(true); }
function updateCard(id, fn) { editDraft(d => { d.cards = d.cards.map(c => (c.id === id ? fn(c) : c)); }); }
function cardRule(c) {
  const checked = RULES.checkCard(c);
  const text = RULES.cardExpr(c);
  let expr = checked.ok ? checked.canonical : text;
  if (checked.ok && text === c.originalEditorExpr) expr = c.originalExpr;
  return { slot: c.slot, action: c.action, expr, ok: checked.ok };
}
function sameFilterRule(a, b) {
  return a === b || !!(a && b && a.ok !== false && b.ok !== false && a.slot === b.slot && a.action === b.action && a.expr === b.expr);
}
function draftChanges() {
  if (!S.draft || !S.snap) return null;
  const before = new Map(S.draft.base.rules.map(r => [r.slot, r]));
  const after = new Map(S.draft.cards.map(c => [c.slot, cardRule(c)]));
  const changes = [];
  for (const [slot, r] of after) {
    const b = before.get(slot);
    if (!b) changes.push({ kind: "add", slot, r });
    else if (!sameFilterRule(b, r)) changes.push({ kind: "edit", slot, r, b });
  }
  for (const [slot, b] of before) if (!after.has(slot)) changes.push({ kind: "remove", slot, b });
  const sw = [];
  for (const [k] of DATA.SWITCHES) if (S.draft.switches[k] !== S.draft.base.switches[k]) sw.push(k);
  return { changes: changes.sort((a, b) => a.slot - b.slot), sw, count: changes.length + sw.length };
}
function isDirty() { const d = draftChanges(); return !!d && (d.count > 0 || S.draft.conflicts.size > 0); }
function draftErrors() { return S.draft ? S.draft.cards.map(c => ({ c, r: RULES.checkCard(c) })).filter(x => !x.r.ok) : []; }
function discardDraft() { S.draft = null; S.editing = null; notify(true); }

function filterPatch(draft, changes) {
  return {
    rules: changes.changes.length ? draft.cards.map(cardRule) : undefined,
    switches: Object.fromEntries(changes.sw.map(k => [k, draft.switches[k]])),
  };
}
// Sends PATCH YAML FILTER SCHEMA 7 with the latest GET revision, then confirms by
// reading the profile back. A refused or unconfirmed write keeps the draft.
async function applyDraft() {
  const draft = S.draft;
  if (!draft || draft.conflicts.size || draft.base.revision !== S.snap.revision || S.snap.conn.cfgError) return;
  const errs = draftErrors();
  if (errs.length) { toast(`Fix rule ${errs[0].c.slot} first: ${errs[0].r.error}`, "error"); return; }
  const built = RULES.buildSet(S.draft.cards);
  if (built.error) { toast(built.error, "error"); return; }
  const changes = draftChanges();
  if (!changes.count) return;
  const patch = filterPatch(draft, changes);
  const submitted = { rules: patch.rules || draft.base.rules, switches: { ...draft.switches } };
  try {
    const r = await cluster.patchFilter(patch, draft.base.revision);
    if (!r.confirmed) throw new Error("The node accepted the change, but the readback differs. Your draft is available for review.");
    if (S.draft === draft) { S.draft = null; S.editing = null; }
    else if (S.draft && S.draft.base === draft.base) {
      // While saving, the operator may even undo a submitted edit. Compare those
      // later edits with what was sent, rather than with the pre-save profile.
      mergeFilterDraft(S.draft, submitted, r.cfg);
    }
    toast(`Filters applied on the node and confirmed by readback. ${plural(changes.count, "change")} saved.`);
    notify(true);
  } catch (e) {
    if (S.draft && S.draft !== draft && S.draft.base === draft.base) {
      // A reversal made during an uncertain save is still an edit, even if
      // its value equals the original base. Preserve it through the next GET.
      S.draft.uncertainSubmission = submitted;
    }
    toast(e.message, "error");
  }
}
// Only this browser knows the original profile and the unsaved intent. Slots
// and switch keys are the node's stable identifiers across successive GETs.
function mergeFilterDraft(draft, base, latest, uncertainSubmission) {
  const conflicts = new Map(draft.conflicts);
  const cards = [];
  for (let slot = 0; slot < RULES.LIM.Slots; slot++) {
    const old = base.rules.find(r => r.slot === slot);
    const node = latest.rules.find(r => r.slot === slot);
    const card = draft.cards.find(c => c.slot === slot);
    const mine = card && cardRule(card);
    const edited = !sameFilterRule(mine, old) || (uncertainSubmission && !sameFilterRule(mine, uncertainSubmission.rules.find(r => r.slot === slot)));
    if (!conflicts.has(slot) && edited && sameFilterRule(node, old)) {
      if (card) cards.push(card);
      continue;
    }
    if (!conflicts.has(slot) && edited && !sameFilterRule(mine, node)) conflicts.set(slot, mine);
    if (node) cards.push(RULES.cardFromRule(node));
  }
  const switches = { ...latest.switches };
  for (const [key] of DATA.SWITCHES) {
    const old = base.switches[key], mine = draft.switches[key], node = switches[key];
    const edited = mine !== old || (uncertainSubmission && mine !== uncertainSubmission.switches[key]);
    if (conflicts.has(key) || !edited || mine === node) continue;
    if (node === old) switches[key] = mine;
    else conflicts.set(key, mine);
  }
  S.draft = { base: filterBase(latest), cards, switches, conflicts };
}
function rebaseDraft() {
  if (!S.draft || !S.snap.conn.configLoaded || S.snap.conn.cfgError || S.snap.conn.writing) return;
  mergeFilterDraft(S.draft, S.draft.base, S.snap, S.draft.uncertainSubmission);
  notify(true);
}
function nextDraftSlot(draft, action) {
  const reserved = Array.from(draft.conflicts.keys()).filter(key => typeof key === "number");
  return RULES.nextSlot(draft.cards.concat(reserved.map(slot => ({ slot }))), action);
}

// ---------------------------------------------------------------- evaluation (same order as the node)
function switchOn(sw, name) { const v = sw[name]; return v === "DEFAULT" ? DATA.SWITCH_DEFAULTS[name] : !!v; }
function decideWith(set, sw, s, allowSelf = true) {
  if (s.beacon && !switchOn(sw, "beacon")) return { pass: false, by: "BEACON switch" };
  if (s.toxic && !switchOn(sw, "toxic")) return { pass: false, by: "TOXIC switch" };
  if (allowSelf && SIM.baseCallOf(s.dxCall) === cluster.baseCall && switchOn(sw, "self")) return { pass: true, by: "SELF" };
  return GC.decideRules(set, s, { pathClass: s.path, nearby: cluster.nearby, operating: cluster.operating });
}
function recentSample(minutes = 15) {
  const arr = cluster.sample, cutoff = Date.now() - minutes * 60e3;
  let i = arr.length; while (i > 0 && arr[i - 1].t >= cutoff) i--;
  return arr.slice(i);
}
const _setMemo = { key: null, val: null };
const _appliedSetMemo = { key: null, val: null };
function preparedCards(cards, memo) {
  const key = JSON.stringify(RULES.rulesFromCards(cards));
  if (memo.key !== key) { memo.key = key; memo.val = RULES.buildSet(cards); }
  return memo.val;
}
function workingSet() {
  return preparedCards(workingCards(), _setMemo);
}
const _prevMemo = { key: "", val: null };
function previewStats() {
  const cards = workingCards(), sw = workingSwitches();
  const key = cards.map(c => c.slot + c.action + RULES.cardExpr(c)).join("|") + JSON.stringify(sw) + "|" + Math.floor(Date.now() / 5000) + "|" + (S.snap && S.snap.revision);
  if (_prevMemo.key === key) return _prevMemo.val;
  const sample = recentSample();
  const built = workingSet();
  const res = { total: sample.length, pass: 0, current: 0, per: {}, error: built.error };
  // current applied rules
  let curSet = null;
  try { for (const r of S.snap.rules) curSet = GC.withRule(curSet, r, RULES.resolve); } catch (e) { /* keep null */ }
  const progs = [];
  for (const c of cards) {
    const r = RULES.checkCard(c);
    if (!r.ok) continue;
    progs.push({ c, set: GC.withRule(null, { slot: c.slot, action: "pass", expr: r.canonical }, RULES.resolve) });
    res.per[c.id] = 0;
  }
  for (const s of sample) {
    if (decideWith(curSet, S.snap.switches, s).pass) res.current++;
    const ok = built.error ? false : decideWith(built.set, sw, s).pass;
    if (ok) res.pass++;
    for (const p of progs) {
      const hit = GC.decideRules(p.set, s, { pathClass: s.path, nearby: cluster.nearby, operating: cluster.operating }).pass;
      if (hit && (p.c.action === "reject" || ok)) res.per[p.c.id]++;
    }
  }
  _prevMemo.key = key; _prevMemo.val = res;
  return res;
}

// ---------------------------------------------------------------- card mutations
function addCard(action, expr) {
  editDraft(d => {
    const slot = nextDraftSlot(d, action);
    if (slot < 0) { toast("All 10 rule numbers are in use. Remove or combine a rule first.", "error"); return; }
    let card;
    if (expr) {
      const canonical = GC.compileRule(expr, RULES.resolve).canonical;
      card = RULES.cardFromRule({ slot, action, expr: canonical });
    } else card = { id: RULES.newId(), slot, action, expert: false, expr: "", items: [] };
    d.cards.push(card);
    S.editing = card.id;
    setTimeout(() => { const el = document.getElementById("card-" + card.id); if (el) el.scrollIntoView({ block: "nearest", behavior: "smooth" }); }, 60);
  });
}
function hideFromSpot(field, value) {
  editDraft(d => {
    const existing = d.cards.find(c => !d.conflicts.has(c.slot) && c.action === "reject" && !c.expert && c.items.length === 1 && c.items[0].type === "cond" && c.items[0].field === field && c.items[0].op === "=");
    if (existing) {
      const it = existing.items[0];
      if (!it.values.includes(value)) it.values.push(value);
      S.editing = existing.id;
    } else {
      const slot = nextDraftSlot(d, "reject");
      if (slot < 0) { toast("All 10 rule numbers are in use.", "error"); return; }
      const card = { id: RULES.newId(), slot, action: "reject", expert: false, expr: "", items: [{ type: "cond", field, op: "=", values: [value] }] };
      d.cards.push(card);
      S.editing = card.id;
    }
  });
  S.drawer = "filters"; S.filterTab = "rules"; notify(true);
}

// ---------------------------------------------------------------- value editors
function chipToggle(values, v) { return values.includes(v) ? values.filter(x => x !== v) : [...values, v]; }

function EnumEditor({ cond, onChange }) {
  const opts = RULES.ENUM[cond.field]();
  const vals = cond.values;
  const extra = vals.filter(v => !opts.some(([k]) => k === v));
  if (cond.field === "BAND") {
    const has = new Set(vals.map(v => v.toLowerCase()));
    const common = DATA.MAIN_BANDS.map(b => [b.toUpperCase(), b]);
    const rare = opts.filter(([k]) => !DATA.MAIN_BANDS.includes(k.toLowerCase()));
    return html`<div class="ved">
      <div class="meta-row">${DATA.META_BANDS.slice(0, 4).map(([name, label, members]) => {
        const all = members.every(m => has.has(m));
        return html`<button type="button" class=${cls("meta", all && "on")} title=${members.join(", ")} onClick=${() => {
          let next = vals.filter(v => !members.includes(v.toLowerCase()));
          if (!all) next = [...next, ...members.map(m => m.toUpperCase())];
          onChange({ ...cond, values: next });
        }}>${label}</button>`;
      })}</div>
      <div class="chips">${common.map(([k, l]) => html`<button type="button" class=${cls("chip", "band", vals.includes(k) && "on")} style=${`--bc:${BAND_COLOR[l] || "var(--accent)"}`} aria-pressed=${vals.includes(k) ? "true" : "false"} onClick=${() => onChange({ ...cond, values: chipToggle(vals, k) })}>${l}</button>`)}
      <details class="more"><summary>${rare.length} more</summary><div class="chips">${rare.map(([k, l]) => html`<button type="button" class=${cls("chip", vals.includes(k) && "on")} onClick=${() => onChange({ ...cond, values: chipToggle(vals, k) })}>${l}</button>`)}</div></details></div>
    </div>`;
  }
  return html`<div class="ved"><div class="chips">
    ${opts.map(([k, l]) => html`<button type="button" class=${cls("chip", vals.includes(k) && "on", k === "MYCONT" && "mine")} aria-pressed=${vals.includes(k) ? "true" : "false"} onClick=${() => onChange({ ...cond, values: chipToggle(vals, k) })}>
      ${cond.field === "PATH" ? html`<${PathGlyph} cls=${k} size="sm" />` : null}
      ${cond.field === "CONF" ? html`<${ConfBadge} conf=${k} />` : null}
      <span>${cond.field === "CONF" ? DATA.CONF[k][0] : l}</span></button>`)}
    ${extra.map(v => html`<button type="button" class="chip on" onClick=${() => onChange({ ...cond, values: vals.filter(x => x !== v) })}>${RULES.valueLabel(cond.field, v)} ×</button>`)}
  </div></div>`;
}

const TAG_RULES = {
  call: { ph: "K1ABC, W1*, *QRP", test: v => v === "*" || /^\*?[A-Z0-9/-]+\*?$/.test(v) && !(v.startsWith("*") && v.endsWith("*")), up: true, err: "Use letters, digits, / or -, with one optional * at the start or end." },
  state: { ph: "CT, MA, ON", test: v => GC.isState(v), up: true, err: "Use a US state or Canadian province code, such as CT or ON." },
  grid2: { ph: "FN, JO, PM", test: v => /^[A-R]{2}$/.test(v), up: true, err: "Use two letters from AA to RR." },
  text: { ph: "Type a phrase, press Enter", test: v => v.length >= 1 && v.length <= 64 && /^[\x20-\x7e]+$/.test(v) && v.trim() !== "", up: false, err: "1 to 64 printable ASCII characters." },
  number: { ph: "-10, 0, 15", test: v => /^-?\d+$/.test(v), up: false, err: "Whole numbers only." },
  freq: { ph: "14000-14070 or 7030", test: v => /^\d+(\.\d+)?(-\d+(\.\d+)?)?$/.test(v) && (!v.includes("-") || Number(v.split("-")[0]) <= Number(v.split("-")[1])), up: false, err: "A frequency in kHz or a range low-high, such as 14000-14070." },
  zone: { ph: "", test: v => /^\d+$/.test(v) && +v >= 1 && +v <= 40, up: false, err: "CQ zones run from 1 to 40." },
};
function TagEditor({ cond, onChange, kind }) {
  const rule = TAG_RULES[kind];
  const [text, setText] = useState("");
  const [err, setErr] = useState("");
  const add = raw => {
    const parts = kind === "text" ? [raw] : raw.split(/[,\s]+/);
    let next = cond.values.slice();
    for (let p of parts) {
      p = rule.up ? p.trim().toUpperCase() : (kind === "text" ? p : p.trim());
      if (!p) continue;
      if (!rule.test(p)) { setErr(`“${p}”: ${rule.err}`); return false; }
      if (!next.includes(p)) next.push(p);
    }
    setErr(""); setText(""); onChange({ ...cond, values: next });
    return true;
  };
  const segs = kind === "freq" ? Object.entries(DATA.SEGMENTS).flatMap(([b, ss]) => ss.filter(x => x[2] !== "phone" || b === "20m" || b === "40m").map(x => [b, x])) : [];
  return html`<div class="ved">
    <div class="tags">
      ${cond.values.map(v => html`<span class="tag">${kind === "text" ? `“${v}”` : RULES.valueLabel(cond.field, v)}<button type="button" aria-label=${"Remove " + v} onClick=${() => onChange({ ...cond, values: cond.values.filter(x => x !== v) })}><${Icon} name="close" size=${12} /></button></span>`)}
      <input class="tag-in" value=${text} placeholder=${cond.values.length ? "Add another" : rule.ph} aria-label=${"Add " + RULES.F[cond.field].noun}
        onInput=${e => { setText(e.target.value); if (err) setErr(""); }}
        onKeyDown=${e => {
          if (e.key === "Enter" || (e.key === "," && kind !== "text")) { e.preventDefault(); if (text.trim()) add(text); }
          else if (e.key === "Backspace" && !text && cond.values.length) onChange({ ...cond, values: cond.values.slice(0, -1) });
        }}
        onBlur=${() => { if (text.trim()) add(text); }} />
    </div>
    ${kind === "freq" ? html`<select class="mini-select" aria-label="Add a band segment" onChange=${e => { if (e.target.value) { add(e.target.value); e.target.value = ""; } }}>
      <option value="">Add a band segment…</option>
      ${segs.map(([b, x]) => html`<option value=${`${x[0]}-${x[1]}`}>${b} ${x[2].toUpperCase()} · ${x[0]}–${x[1]} kHz</option>`)}
    </select>` : null}
    ${err ? html`<div class="field-err" role="alert">${err}</div>` : null}
  </div>`;
}
function ZoneEditor({ cond, onChange }) {
  return html`<div class="ved"><div class="zones">${Array.from({ length: 40 }, (_, i) => String(i + 1)).map(z => html`<button type="button" class=${cls("zone", cond.values.includes(z) && "on")} aria-pressed=${cond.values.includes(z) ? "true" : "false"} onClick=${() => onChange({ ...cond, values: chipToggle(cond.values, z) })}>${z}</button>`)}</div></div>`;
}
function NumberEditor({ cond, onChange, unit }) {
  return html`<div class="ved num"><input type="number" class="num-in" value=${cond.values[0] || ""} aria-label=${RULES.F[cond.field].label}
    onInput=${e => onChange({ ...cond, values: e.target.value === "" ? [] : [String(Math.round(Number(e.target.value) * (cond.field === "FREQ" ? 10 : 1)) / (cond.field === "FREQ" ? 10 : 1))] })} /><span class="unit">${unit}</span></div>`;
}
function EntityEditor({ cond, onChange }) {
  const [q, setQ] = useState("");
  const [hi, setHi] = useState(0);
  const res = useMemo(() => RULES.searchEntities(q), [q]);
  const addAdif = a => { const v = String(a); if (!cond.values.includes(v)) onChange({ ...cond, values: [...cond.values, v] }); setQ(""); setHi(0); };
  const mine = cond.values.includes("MYDXCC");
  return html`<div class="ved">
    <div class="tags">
      ${cond.values.map(v => html`<span class=${cls("tag", v === "MYDXCC" && "mine")}>${v !== "MYDXCC" ? html`<b class="mono">${RULES.ADIF_LABEL.get(Number(v)) || v}</b>` : null} ${RULES.valueLabel(cond.field, v)}<button type="button" aria-label="Remove" onClick=${() => onChange({ ...cond, values: cond.values.filter(x => x !== v) })}><${Icon} name="close" size=${12} /></button></span>`)}
      <input class="tag-in" value=${q} placeholder="Search country or prefix (DL, Japan…)" aria-label="Search countries"
        onInput=${e => { setQ(e.target.value); setHi(0); }}
        onKeyDown=${e => {
          if (e.key === "ArrowDown") { e.preventDefault(); setHi(Math.min(res.length - 1, hi + 1)); }
          else if (e.key === "ArrowUp") { e.preventDefault(); setHi(Math.max(0, hi - 1)); }
          else if (e.key === "Enter" && res[hi]) { e.preventDefault(); addAdif(res[hi].adif); }
          else if (e.key === "Backspace" && !q && cond.values.length) onChange({ ...cond, values: cond.values.slice(0, -1) });
        }} />
    </div>
    ${q && res.length ? html`<ul class="suggest" role="listbox">${res.map((e, i) => html`<li role="option" aria-selected=${i === hi ? "true" : "false"} class=${i === hi ? "hi" : ""} onMouseDown=${ev => { ev.preventDefault(); addAdif(e.adif); }}><b class="mono">${e.label}</b> ${e.name}<span class="muted mono">ADIF ${e.adif}</span></li>`)}</ul>` : null}
    ${q && !res.length ? html`<div class="field-err">No CTY entity matches “${q}”.</div>` : null}
    <button type="button" class=${cls("chip", "mine", mine && "on")} onClick=${() => onChange({ ...cond, values: chipToggle(cond.values, "MYDXCC") })}>My country</button>
  </div>`;
}
function ValueEditor({ cond, onChange }) {
  const k = RULES.F[cond.field].kind;
  const ordered = !["=", "<>"].includes(cond.op);
  if (k === "enum") return html`<${EnumEditor} cond=${cond} onChange=${onChange} />`;
  if (k === "entity") return html`<${EntityEditor} cond=${cond} onChange=${onChange} />`;
  if (k === "zone") return ordered ? html`<${NumberEditor} cond=${cond} onChange=${onChange} unit="zone" />` : html`<${ZoneEditor} cond=${cond} onChange=${onChange} />`;
  if (k === "number") return ordered ? html`<${NumberEditor} cond=${cond} onChange=${onChange} unit="dB" />` : html`<${TagEditor} cond=${cond} onChange=${onChange} kind="number" />`;
  if (k === "freq") return ordered ? html`<${NumberEditor} cond=${cond} onChange=${onChange} unit="kHz" />` : html`<${TagEditor} cond=${cond} onChange=${onChange} kind="freq" />`;
  return html`<${TagEditor} cond=${cond} onChange=${onChange} kind=${k} />`;
}

// ---------------------------------------------------------------- condition rows
function CondRow({ cond, onChange, onRemove, inGroup }) {
  if (cond.type === "pred") {
    return html`<div class="cond pred">
      <div class="cond-head"><span class="fname"><${Icon} name="radio" size=${15} /> Near me</span>
        <${Seg} small label="Near me" value=${cond.negate ? "no" : "yes"} onChange=${v => onChange({ ...cond, negate: v === "no" })} options=${[["yes", "Is near me"], ["no", "Is not near me"]]} />
        <button type="button" class="icon-btn sm" aria-label="Remove condition" onClick=${onRemove}><${Icon} name="close" size=${14} /></button>
      </div>
      <p class="hint">True when the DX station or the spotter is in your area (wider on 160, 80 and 60m). Unknown locations count as not near.</p>
    </div>`;
  }
  const f = RULES.F[cond.field];
  const ops = RULES.opsFor(cond.field);
  return html`<div class=${cls("cond", inGroup && "in-group")}>
    <div class="cond-head">
      <span class="fname">${f.label}</span>
      <select class="op" aria-label=${f.label + " comparison"} value=${cond.op} onChange=${e => {
        const op = e.target.value;
        const wasOrd = !["=", "<>"].includes(cond.op), isOrd = !["=", "<>"].includes(op);
        onChange({ ...cond, op, values: wasOrd !== isOrd ? cond.values.slice(0, isOrd ? 1 : 32) : cond.values });
      }}>${ops.map(o => html`<option value=${o}>${RULES.OPS[o].short}</option>`)}</select>
      <span class="grow"></span>
      <button type="button" class="icon-btn sm" aria-label="Remove condition" onClick=${onRemove}><${Icon} name="close" size=${14} /></button>
    </div>
    <${ValueEditor} cond=${cond} onChange=${onChange} />
  </div>`;
}

function FieldPicker({ onPick, onClose, allowPred = true }) {
  return html`<div class="field-picker" role="menu">
    <div class="fp-head"><span>Choose what to check</span><button type="button" class="icon-btn sm" aria-label="Close" onClick=${onClose}><${Icon} name="close" size=${14} /></button></div>
    ${RULES.GROUPS.map(g => html`<div class="fp-group"><h5>${g}</h5><div class="fp-items">
      ${Object.entries(RULES.F).filter(([k, f]) => f.group === g && (allowPred || f.kind !== "pred")).map(([k, f]) => html`<button type="button" role="menuitem" onClick=${() => onPick(k)}>${f.label}</button>`)}
    </div></div>`)}
  </div>`;
}

// ---------------------------------------------------------------- rule card
function RuleCard({ card, stats, snapCards }) {
  const editing = S.editing === card.id;
  const check = RULES.checkCard(card);
  const lints = check.ok ? RULES.lintCard(card, { grid: S.snap.settings.grid }) : [];
  const [picker, setPicker] = useState(null); // null | "and" | groupIndex
  const [menu, setMenu] = useState(false);
  const orig = snapCards.find(c => c.slot === card.slot);
  const changed = !orig ? "new" : (orig.action !== card.action || RULES.cardExpr(orig) !== RULES.cardExpr(card)) ? "edited" : "";
  const upd = fn => updateCard(card.id, fn);
  const setItem = (i, v) => upd(c => ({ ...c, items: c.items.map((x, j) => (j === i ? v : x)) }));
  const delItem = i => upd(c => ({ ...c, items: c.items.filter((_, j) => j !== i) }));
  const pass = card.action === "pass";
  const count = stats && stats.per[card.id];
  const cmd = `${pass ? "PASS" : "REJECT"} RULE ${card.slot} ${check.ok ? check.canonical : RULES.cardExpr(card)}`;

  return html`<article id=${"card-" + card.id} class=${cls("rcard", pass ? "pass" : "reject", editing && "editing", !check.ok && "invalid")}>
    <header class="rc-head">
      <button type="button" class="act" title="Switch between Show and Hide" onClick=${() => upd(c => ({ ...c, action: pass ? "reject" : "pass" }))}>${pass ? "Show" : "Hide"}</button>
      <button type="button" class="rc-title" aria-expanded=${editing ? "true" : "false"} onClick=${() => set({ editing: editing ? null : card.id })}>
        <span class="sentence">${RULES.cardSentence(card)}</span>
        ${changed ? html`<span class=${"chg " + changed}>${changed === "new" ? "New" : "Edited"}</span>` : null}
      </button>
      ${count != null ? html`<span class="rc-count" title=${pass ? "Spots in the last 15 minutes this rule lets through" : "Spots in the last 15 minutes this rule hides"}>${pass ? "+" : "−"}${count.toLocaleString()}</span>` : null}
      <span class="slot" title="Rule number. Numbers identify rules; they do not set priority.">#${card.slot}</span>
      <div class="menu-wrap">
        <button type="button" class="icon-btn sm" aria-label="Rule actions" aria-expanded=${menu ? "true" : "false"} onClick=${() => setMenu(!menu)}><${Icon} name="dots" size=${16} stroke=${3} /></button>
        ${menu ? html`<div class="menu" role="menu" onMouseLeave=${() => setMenu(false)}>
          <button role="menuitem" type="button" onClick=${() => { setMenu(false); set({ editing: editing ? null : card.id }); }}><${Icon} name="sliders" size=${15} /> ${editing ? "Done editing" : "Edit"}</button>
          ${card.expert
            ? (RULES.exprToItems(card.expr) ? html`<button role="menuitem" type="button" onClick=${() => { setMenu(false); upd(c => ({ ...c, expert: false, items: RULES.exprToItems(c.expr) })); }}><${Icon} name="wand" size=${15} /> Edit visually</button>` : null)
            : html`<button role="menuitem" type="button" onClick=${() => { setMenu(false); upd(c => ({ ...c, expert: true, expr: RULES.cardExpr(c) })); set({ editing: card.id }); }}><${Icon} name="code" size=${15} /> Edit as expression</button>`}
          <button role="menuitem" type="button" onClick=${() => { setMenu(false); editDraft(d => { const slot = nextDraftSlot(d, card.action); if (slot < 0) return toast("All 10 rule numbers are in use.", "error"); const copy = { ...JSON.parse(JSON.stringify(card)), id: RULES.newId(), slot }; d.cards.push(copy); S.editing = copy.id; }); }}><${Icon} name="copy" size=${15} /> Duplicate</button>
          <button role="menuitem" type="button" class="danger" onClick=${() => { setMenu(false); editDraft(d => { d.cards = d.cards.filter(c => c.id !== card.id); }); }}><${Icon} name="trash" size=${15} /> Delete rule</button>
        </div>` : null}
      </div>
    </header>

    ${!editing && !card.expert ? html`<div class="rc-summary" onClick=${() => set({ editing: card.id })}>
      ${card.items.map((it, i) => html`${i > 0 ? html`<span class="joiner">and</span>` : null}${it.type === "any"
        ? html`<span class="sumgroup">${it.items.map((c, j) => html`${j > 0 ? html`<span class="joiner or">or</span>` : null}<${SumChip} c=${c} />`)}</span>`
        : html`<${SumChip} c=${it} />`}`)}
      ${!card.items.length ? html`<span class="muted">No conditions. Select to add one.</span>` : null}
    </div>` : null}

    ${editing && !card.expert ? html`<div class="rc-body">
      ${card.items.map((it, i) => html`
        ${i > 0 ? html`<div class="and-sep"><span>and</span></div>` : null}
        ${it.type === "any" ? html`<div class="anygroup">
          <div class="ag-head"><span>At least one of these</span><button type="button" class="link sm" onClick=${() => delItem(i)}>Remove group</button></div>
          ${it.items.map((c, j) => html`${j > 0 ? html`<div class="or-sep"><span>or</span></div>` : null}<${CondRow} inGroup cond=${c}
              onChange=${v => setItem(i, { ...it, items: it.items.map((x, k) => (k === j ? v : x)) })}
              onRemove=${() => setItem(i, { ...it, items: it.items.filter((_, k) => k !== j) })} />`)}
          ${picker === i ? html`<${FieldPicker} onClose=${() => setPicker(null)} onPick=${f => { setItem(i, { ...it, items: [...it.items, RULES.blankCond(f)] }); setPicker(null); }} />`
            : html`<button type="button" class="add-cond" onClick=${() => setPicker(i)}><${Icon} name="plus" size=${14} /> Add an alternative</button>`}
        </div>` : html`<${CondRow} cond=${it} onChange=${v => setItem(i, v)} onRemove=${() => delItem(i)} />`}
      `)}
      ${picker === "and" ? html`<${FieldPicker} onClose=${() => setPicker(null)} onPick=${f => { upd(c => ({ ...c, items: [...c.items, RULES.blankCond(f)] })); setPicker(null); }} />` : html`
        <div class="add-row">
          <button type="button" class="add-cond" onClick=${() => setPicker("and")}><${Icon} name="plus" size=${14} /> ${card.items.length ? "And also…" : "Add a condition"}</button>
          ${card.items.length ? html`<button type="button" class="add-cond ghost" title="A group passes when any one of its conditions matches" onClick=${() => upd(c => ({ ...c, items: [...c.items, { type: "any", items: [] }] }))}><${Icon} name="layers" size=${14} /> Either/or group</button>` : null}
        </div>`}
    </div>` : null}

    ${card.expert ? html`<${ExpertEditor} card=${card} editing=${editing} check=${check} />` : null}

    ${(editing || !check.ok || lints.some(l => l.level !== "info")) ? html`<div class="rc-foot">
      ${!check.ok ? html`<div class="lint error"><${Icon} name="error" size=${15} /><span>${check.error}</span></div>` : null}
      ${lints.filter(l => editing || l.level !== "info").map(l => html`<div class=${"lint " + l.level}><${Icon} name=${l.level === "error" ? "error" : l.level === "warn" ? "warn" : "info"} size=${15} /><span>${l.text}</span>
        ${l.fix ? html`<button type="button" class="link sm" onClick=${() => updateCard(card.id, c => RULES.mergeField(c, l.fix.field))}>Merge them</button>` : null}</div>`)}
      ${editing ? html`<div class="cmdline"><code title="The equivalent telnet command">${cmd}</code><button type="button" class="icon-btn sm" aria-label="Copy command" onClick=${() => copyText(cmd, "Command copied")}><${Icon} name="copy" size=${14} /></button></div>` : null}
    </div>` : null}
  </article>`;
}
function SumChip({ c }) {
  if (c.type === "pred") return html`<span class="sumchip pred">${c.negate ? "Not near me" : "Near me"}</span>`;
  const f = RULES.F[c.field];
  const meta = c.field === "BAND" ? RULES.metaFor(c.values) : null;
  const vals = meta ? [DATA.META_BANDS.find(m => m[0] === meta)[1]] : c.values.map(v => RULES.valueLabel(c.field, v));
  const shown = vals.length > 6 ? vals.slice(0, 6).concat([`+${vals.length - 6}`]) : vals;
  return html`<span class=${cls("sumchip", c.op === "<>" && "neg")}><em>${f.label}</em>${c.op === "=" ? "" : html`<i>${RULES.OPS[c.op].short}</i>`}<b>${shown.join(", ") || "—"}</b></span>`;
}
function ExpertEditor({ card, editing, check }) {
  const upd = expr => updateCard(card.id, c => ({ ...c, expr }));
  if (!editing) return html`<div class="rc-summary expert" onClick=${() => set({ editing: card.id })}><code>${card.expr}</code></div>`;
  return html`<div class="rc-body">
    <label class="lbl" for=${"ex-" + card.id}>Expression</label>
    <textarea id=${"ex-" + card.id} class="expr" spellcheck="false" rows="3" value=${card.expr} onInput=${e => upd(e.target.value)}></textarea>
    ${check.ok && check.canonical !== card.expr.trim() ? html`<div class="canon"><span>The node will store</span><code>${check.canonical}</code></div>` : null}
    <p class="hint">Fields: BAND MODE SOURCE EVENT DXCALL DECALL DXCONT DECONT DXZONE DEZONE DXDXCC DEDXCC DXSTATE DESTATE DXGRID2 DEGRID2 FREQ SNR CONF PATH COMMENT NEARBY. Combine with AND, OR, NOT and parentheses; NOT binds first, then AND, then OR.</p>
  </div>`;
}

// ---------------------------------------------------------------- preview meter
function PreviewMeter({ stats }) {
  if (!stats) return null;
  const pct = stats.total ? stats.pass / stats.total : 0;
  const cur = stats.total ? stats.current / stats.total : 0;
  const dirty = isDirty();
  const delta = stats.pass - stats.current;
  return html`<section class="meter" aria-label="Rule preview">
    <div class="meter-top">
      <div><div class="big">${stats.error ? "—" : stats.pass.toLocaleString()}<span> of ${stats.total.toLocaleString()}</span></div>
        <div class="muted small">recent spots would reach you (last 15 minutes)</div></div>
      ${dirty && !stats.error ? html`<div class=${cls("delta", delta > 0 ? "up" : delta < 0 ? "down" : "")}>${delta > 0 ? "+" : ""}${delta.toLocaleString()}<span>vs applied</span></div>` : null}
    </div>
    <div class="bar"><i class="cur" style=${`width:${(cur * 100).toFixed(1)}%`}></i><i class="new" style=${`width:${(pct * 100).toFixed(1)}%`}></i></div>
    ${stats.error ? html`<div class="lint error"><${Icon} name="error" size=${15} /><span>${stats.error}</span></div>` : null}
    <button type="button" class=${cls("link", "sm")} onClick=${() => set({ showHidden: !S.showHidden })}>${S.showHidden ? "Stop showing hidden spots in the list" : "Show hidden spots in the list, dimmed"}</button>
  </section>`;
}

function HowItWorks() {
  const [open, setOpen] = useState(LS.get("gcui.howto", true));
  const toggle = () => { LS.set("gcui.howto", !open); setOpen(!open); };
  return html`<section class=${cls("howto", open && "open")}>
    <button type="button" class="howto-head" aria-expanded=${open ? "true" : "false"} onClick=${toggle}><${Icon} name="info" size=${16} /> How Show and Hide rules combine <${Icon} name="chevronDown" size=${16} /></button>
    ${open ? html`<div class="howto-body">
      <div class="flow">
        <div class="fstep"><span class="dot reject"></span><div><b>Any Hide rule matches?</b><span>The spot is hidden. Hide always wins.</span></div></div>
        <div class="fstep"><span class="dot pass"></span><div><b>Otherwise, any Show rule matches?</b><span>The spot is shown. One match is enough.</span></div></div>
        <div class="fstep"><span class="dot none"></span><div><b>No Show rules at all?</b><span>Everything not hidden is shown.</span></div></div>
      </div>
      <p>Inside one rule every condition must be true (<b>and</b>). Several values in one condition mean <b>any of them</b>. To mean “20m or 40m”, put both bands in one condition, not two.</p>
    </div>` : null}
  </section>`;
}

function RulesTab() {
  const cards = workingCards();
  // Until the operator reviews a newer revision, the card labels and removal
  // notices must describe edits against the same base as the change count.
  const base = S.draft?.base;
  const snapCards = useMemo(() => base ? base.rules.map(RULES.cardFromRule) : currentCards(), [base, S.snap.revision]);
  const stats = previewStats();
  const editable = cards.filter(c => !S.draft?.conflicts.has(c.slot));
  const pass = editable.filter(c => c.action === "pass"), rej = editable.filter(c => c.action === "reject");
  const setLints = RULES.lintSet(cards);
  const [recipes, setRecipes] = useState(false);
  const removed = S.draft ? snapCards.filter(c => !cards.some(d => d.slot === c.slot)) : [];
  return html`<div class="tab-body">
    <${PreviewMeter} stats=${stats} />
    <${HowItWorks} />
    ${setLints.map(l => html`<div class=${"lint " + l.level}><${Icon} name="info" size=${15} /><span>${l.text}</span></div>`)}

    <section class="rsec">
      <div class="rsec-head"><h3><span class="dot pass"></span>Show spots that match <em>any</em> of these</h3><span class="muted small">${pass.length} rule${pass.length === 1 ? "" : "s"}</span></div>
      ${pass.map((c, i) => html`${i > 0 ? html`<div class="or-between">or</div>` : null}<${RuleCard} key=${c.id} card=${c} stats=${stats} snapCards=${snapCards} />`)}
      <button type="button" class="add-rule pass" onClick=${() => addCard("pass")}><${Icon} name="plus" size=${16} /> Add a Show rule</button>
    </section>
    <section class="rsec">
      <div class="rsec-head"><h3><span class="dot reject"></span>Always hide spots that match <em>any</em> of these</h3><span class="muted small">${rej.length} rule${rej.length === 1 ? "" : "s"}</span></div>
      ${rej.map((c, i) => html`${i > 0 ? html`<div class="or-between">or</div>` : null}<${RuleCard} key=${c.id} card=${c} stats=${stats} snapCards=${snapCards} />`)}
      <button type="button" class="add-rule reject" onClick=${() => addCard("reject")}><${Icon} name="plus" size=${16} /> Add a Hide rule</button>
    </section>
    ${removed.length ? html`<div class="removed-note">${removed.map(c => html`<div class="lint warn"><${Icon} name="trash" size=${15} /><span>Rule ${c.slot} (“${RULES.cardSentence(c)}”) will be removed when you apply.</span></div>`)}</div>` : null}

    <section class=${cls("recipes", recipes && "open")}>
      <button type="button" class="howto-head" aria-expanded=${recipes ? "true" : "false"} onClick=${() => setRecipes(!recipes)}><${Icon} name="spark" size=${16} /> Start from a recipe <${Icon} name="chevronDown" size=${16} /></button>
      ${recipes ? html`<div class="recipe-grid">${RULES.RECIPES.map(r => html`<button type="button" class=${"recipe " + r.action} onClick=${() => { addCard(r.action, r.expr); toast(`Added “${r.title}”. Apply to send it to the node.`); }}>
        <span class=${"act-tag " + r.action}>${r.action === "pass" ? "Show" : "Hide"}</span><b>${r.title}</b><span>${r.desc}</span><code>${r.expr}</code></button>`)}</div>` : null}
    </section>
    <p class="slots muted small">${cards.length} of 10 rule numbers used. Rule numbers identify rules; they never change priority.</p>
  </div>`;
}

// ---------------------------------------------------------------- switches
function SwitchesTab() {
  const sw = workingSwitches();
  return html`<div class="tab-body">
    <p class="lede">These separate controls work alongside your rules. Bulletins and announcements are messages, not spots, so rules never filter them.</p>
    <div class="sw-list">${DATA.SWITCHES.filter(([k]) => !S.draft?.conflicts.has(k)).map(([k, name, label, desc]) => {
      const on = switchOn(sw, k), def = sw[k] === "DEFAULT";
      return html`<div class="sw-row">
        <div class="sw-text"><label for=${"sw-" + k}>${label}</label><span>${desc}</span>
          <span class="sw-state">${def ? `Node default (${on ? "on" : "off"})` : `Your choice`}${!def ? html` · <button type="button" class="link sm" onClick=${() => editDraft(d => { d.switches = { ...d.switches, [k]: "DEFAULT" }; })}>Use node default</button>` : null}</span></div>
        <${Toggle} id=${"sw-" + k} on=${on} label=${label} onChange=${v => editDraft(d => { d.switches = { ...d.switches, [k]: v }; })} />
      </div>`;
    })}</div>
  </div>`;
}

// ---------------------------------------------------------------- presets
function PresetsTab() {
  const snap = S.snap;
  const [name, setName] = useState("");
  const [err, setErr] = useState("");
  const [confirm, setConfirm] = useState(null);
  const dirty = isDirty();
  const [busy, setBusy] = useState("");
  const live = S.snap.conn.canWrite;
  const run = async (fn, ok, what) => { setBusy(what || "x"); try { await fn(); if (ok) toast(ok); setConfirm(null); } catch (e) { toast(e.message, "error"); } finally { setBusy(""); } };
  const save = () => {
    const N = name.trim().toUpperCase();
    if (!/^[A-Z0-9][A-Z0-9-]{0,31}$/.test(N)) { setErr("Use 1 to 32 letters, digits or hyphens, starting with a letter or digit."); return; }
    if (snap.presets.some(p => p.name === N) && confirm !== "over:" + N) { setConfirm("over:" + N); return; }
    run(() => cluster.savePreset(N), `Saved preset ${N}.`); setName(""); setErr("");
  };
  return html`<div class="tab-body">
    ${dirty ? html`<div class="lint warn"><${Icon} name="warn" size=${15} /><span>You have unapplied rule edits. Presets save and load what is on the node, so apply or discard your edits first.</span></div>` : null}
    ${!live ? html`<div class="lint warn"><${Icon} name="info" size=${15} /><span>${S.snap.conn.mode === "replay" ? "Replay is read-only." : "Connect Live to use presets."}</span></div>` : null}
    <section class="pcur">
      <div class="muted small">Active preset</div>
      <div class="pcur-row"><span class="pname">${snap.preset.name || "None"}</span>${snap.preset.modified ? html`<span class="badge warn">modified</span>` : snap.preset.name ? html`<span class="badge ok">matches</span>` : null}</div>
      ${snap.preset.name && snap.preset.modified ? html`<div class="btn-row">
        <button type="button" class="btn" disabled=${dirty || !live || busy} onClick=${() => run(() => cluster.savePreset(snap.preset.name), `Updated ${snap.preset.name} with your current setup.`)}><${Icon} name="save" size=${15} /> Update ${snap.preset.name}</button>
        <button type="button" class="btn ghost" disabled=${dirty || !live || busy} onClick=${() => run(() => cluster.loadPreset(snap.preset.name), `Reverted to ${snap.preset.name}.`)}><${Icon} name="undo" size=${15} /> Revert</button>
      </div>` : null}
    </section>
    <section class="psave">
      <label class="lbl" for="preset-name">Save the current setup as a new preset</label>
      <div class="inline-form">
        <input id="preset-name" class="txt mono" maxlength="32" placeholder="e.g. WEEKEND" value=${name} disabled=${dirty || !live} onInput=${e => { setName(e.target.value.toUpperCase()); setErr(""); setConfirm(null); }} onKeyDown=${e => e.key === "Enter" && save()} />
        <button type="button" class="btn primary" disabled=${dirty || !live || !name.trim() || busy} onClick=${save}>${confirm && confirm.startsWith("over:") ? "Replace" : "Save"}</button>
      </div>
      ${confirm && confirm.startsWith("over:") ? html`<div class="lint warn"><${Icon} name="warn" size=${15} /><span>${confirm.slice(5)} already exists. Select Replace to overwrite it.</span></div>` : null}
      ${err ? html`<div class="field-err">${err}</div>` : null}
    </section>
    <div class="plist-head"><h3>Your presets</h3><span class="muted small">${snap.presets.length} of 20 · shared by all your SSIDs</span></div>
    ${!snap.presets.length ? html`<p class="muted small">${S.snap.conn.phase === "live" ? "No saved presets, or the list has not arrived yet." : "The list loads with LIST PRESET after you connect."}</p>` : null}
    <ul class="plist">${snap.presets.map(p => {
      const active = p.name === snap.preset.name;
      return html`<li class=${cls("pitem", active && "active")}>
        <div class="pi-main">
          <div class="pi-name">${p.name}${active ? html`<span class="badge">active</span>` : null}<span class=${cls("badge", p.dialect === "cc" && "warn")}>${(p.dialect || "go").toUpperCase()}</span></div>
          <div class="pi-meta muted small">${p.dialect === "cc" ? "Loading a CC preset switches this login to the CC dialect." : "GO rules, switches and shared settings."}</div>
        </div>
        <div class="pi-act">
          ${confirm === "del:" + p.name
            ? html`<button type="button" class="btn danger sm" disabled=${!live || busy} onClick=${() => run(() => cluster.deletePreset(p.name), `Deleted ${p.name}.`)}>Delete</button><button type="button" class="btn ghost sm" onClick=${() => setConfirm(null)}>Keep</button>`
            : html`<button type="button" class="btn sm" disabled=${dirty || !live || busy} onClick=${() => run(() => cluster.loadPreset(p.name), `Loaded ${p.name}. Its filters and settings are now active.`)}>Load</button>
              <button type="button" class="icon-btn sm" aria-label=${"Delete " + p.name} disabled=${!live} onClick=${() => setConfirm("del:" + p.name)}><${Icon} name="trash" size=${15} /></button>`}
        </div>
      </li>`;
    })}</ul>
    <p class="muted small">A preset holds your rules, the six switches and shared settings (grid, noise, repeats, path samples, solar, CW speed). Loading one also makes it your default for ${snap.loginCall}.</p>
  </div>`;
}

// ---------------------------------------------------------------- apply bar
function resolveConflict(key, update) {
  const draft = S.draft;
  if (!draft?.conflicts.has(key) || draft.base.revision !== S.snap.revision || S.snap.conn.cfgError) return;
  editDraft(d => {
    update(d);
    d.conflicts.delete(key);
  });
}
const describeRule = r => r ? `${r.action.toUpperCase()} ${r.expr}` : "Deleted";
const describeSwitch = v => v === "DEFAULT" ? "Node default" : v ? "On" : "Off";
function RuleConflict({ slot, mine, node }) {
  const [expr, setExpr] = useState("");
  const [action, setAction] = useState(node?.action || mine?.action || "pass");
  const checked = RULES.checkCard({ expert: true, expr, action });
  function finish(remove) {
    resolveConflict(slot, d => {
      d.cards = d.cards.filter(c => c.slot !== slot);
      if (!remove) d.cards.push(RULES.cardFromRule({ slot, action, expr: checked.canonical }));
    });
  }
  return html`<fieldset class="rsec">
    <legend>Re-enter rule ${slot}</legend>
    <p class="canon"><span>On the node</span><code>${describeRule(node)}</code><span>Your earlier edit</span><code>${describeRule(mine)}</code></p>
    <select class="op" aria-label=${"Action for rule " + slot} value=${action} onChange=${e => setAction(e.currentTarget.value)}>
      <option value="pass">Show</option><option value="reject">Hide</option>
    </select>
    <textarea class="expr" rows="2" aria-label=${"Re-enter rule " + slot} placeholder="Rule expression" value=${expr} onInput=${e => setExpr(e.currentTarget.value)}></textarea>
    ${expr && !checked.ok ? html`<p class="field-err">${checked.error}</p>` : null}
    <div class="btn-row"><button type="button" class="btn" disabled=${!checked.ok} onClick=${() => finish(false)}>Set rule ${slot}</button>
      <button type="button" class="btn ghost" onClick=${() => finish(true)}>Delete rule ${slot}</button></div>
  </fieldset>`;
}
function FilterConflicts() {
  const draft = S.draft;
  if (!draft?.conflicts.size || draft.base.revision !== S.snap.revision || S.snap.conn.cfgError) return null;
  return html`<section class="tab-body" aria-label="Conflicting edits"><h3>Re-enter conflicting edits</h3>
    ${Array.from(draft.conflicts, ([key, mine]) => {
      if (typeof key === "number") return html`<${RuleConflict} key=${draft.base.revision + "/" + key} slot=${key} mine=${mine} node=${draft.base.rules.find(r => r.slot === key)} />`;
      const label = DATA.SWITCHES.find(([k]) => k === key)[2];
      return html`<label class="rsec" key=${key}>${label} · On the node: ${describeSwitch(draft.base.switches[key])}. Your earlier edit: ${describeSwitch(mine)}.
        <select class="op" aria-label=${"Re-enter " + key + " switch"} value="" onChange=${e => {
          const v = e.currentTarget.value;
          resolveConflict(key, d => { d.switches[key] = v === "DEFAULT" ? v : v === "on"; });
        }}><option value="" disabled>Select a value</option><option value="on">On</option><option value="off">Off</option><option value="DEFAULT">Node default</option></select>
      </label>`;
    })}
  </section>`;
}
function ApplyBar() {
  const [open, setOpen] = useState(false);
  const [fmt, setFmt] = useState("cmd");
  const d = draftChanges();
  if (!d) return null;
  const conflicts = S.draft.conflicts.size;
  const stale = S.draft.base.revision !== S.snap.revision;
  const reload = S.snap.conn.cfgError;
  if (!d.count && !conflicts && !stale && !reload) return html`<div class="applybar idle"><span class="muted small">No changes to apply.</span><button type="button" class="btn ghost sm" onClick=${discardDraft}>Close draft</button></div>`;
  const errs = draftErrors();
  const cmds = [];
  for (const ch of d.changes) {
    if (ch.kind === "remove") cmds.push(`RESET FILTER RULE ${ch.slot}`);
    else cmds.push(`${ch.r.action === "pass" ? "PASS" : "REJECT"} RULE ${ch.slot} ${ch.r.expr}`);
  }
  for (const k of d.sw) { const v = S.draft.switches[k]; const name = DATA.SWITCHES.find(x => x[0] === k)[1]; cmds.push(v === "DEFAULT" ? `# ${name}: node default (YAML only)` : `${v ? "PASS" : "REJECT"} ${name}`); }
  let yaml;
  try {
    yaml = GCP.patchFilterText({ requestId: "flt-<new id>", revision: S.draft.base.revision, ...filterPatch(S.draft, d) });
  } catch (e) { yaml = "Fix the draft first: " + e.message; }
  const writing = S.snap.conn.writing === "filters";
  const canApply = S.snap.conn.canWrite && S.snap.conn.activeDialect === "go";
  return html`<div class=${cls("applybar", open && "open")}>
    ${reload ? html`<div class="lint warn">The profile could not be loaded. Your draft is still here.
      <button type="button" class="link sm" disabled=${!cluster.live || writing} onClick=${() => cluster.refreshConfig().catch(e => toast(e.message, "error"))}>Reload profile</button></div>` : null}
    ${stale && !reload ? html`<div class="lint warn"><${Icon} name="warn" size=${15} /><span>The profile changed after you started editing. Review your edits against revision ${S.snap.revision || "unknown"}.</span><button type="button" class="link sm" disabled=${!S.snap.conn.configLoaded || writing} onClick=${rebaseDraft}>Keep my edits</button><button type="button" class="link sm" onClick=${discardDraft}>Start over</button></div>` : null}
    ${conflicts && !stale && !reload ? html`<div class="lint warn">Re-enter ${plural(conflicts, "conflicting edit")} before applying.</div>` : null}
    ${open && !stale && !conflicts && !reload ? html`<div class="review">
      <ul class="chlist">${d.changes.map(ch => html`<li class=${ch.kind}><span class="chk">${ch.kind === "add" ? "Add" : ch.kind === "remove" ? "Remove" : "Change"}</span> rule ${ch.slot}: ${ch.kind === "remove" ? RULES.cardSentence(RULES.cardFromRule(ch.b)) : RULES.cardSentence(RULES.cardFromRule(ch.r))}</li>`)}
        ${d.sw.map(k => html`<li class="edit"><span class="chk">Switch</span> ${DATA.SWITCHES.find(x => x[0] === k)[2]}: ${S.draft.switches[k] === "DEFAULT" ? "node default" : S.draft.switches[k] ? "on" : "off"}</li>`)}</ul>
      <div class="send-head"><${Seg} small label="Show as" value=${fmt} onChange=${setFmt} options=${[["cmd", "Telnet commands"], ["yaml", "What the console sends"]]} />
        <button type="button" class="icon-btn sm" aria-label="Copy" onClick=${() => copyText(fmt === "cmd" ? cmds.join("\n") : yaml, "Copied")}><${Icon} name="copy" size=${14} /></button></div>
      <pre class="sends">${fmt === "cmd" ? cmds.join("\n") : yaml}</pre>
    </div>` : null}
    <div class="ab-row">
      <button type="button" class="ab-sum" aria-expanded=${open ? "true" : "false"} onClick=${() => setOpen(!open)}>
        <span class="pulse-dot"></span><b>${plural(d.count, "unapplied change")}</b>${errs.length ? html`<span class="badge err">${errs.length} to fix</span>` : null}
        <${Icon} name="chevronDown" size=${15} />
      </button>
      <span class="grow"></span>
      <button type="button" class="btn ghost" onClick=${discardDraft}>Discard</button>
      <button type="button" class="btn primary" disabled=${!d.count || errs.length > 0 || !!conflicts || stale || !!reload || !canApply} title=${canApply ? "" : S.snap.conn.mode === "replay" ? "Replay is read-only" : "Connect Live with your profile loaded to apply"} onClick=${applyDraft}><${Icon} name="check" size=${16} /> ${writing ? "Applying…" : "Apply"}</button>
    </div>
  </div>`;
}

function FiltersDrawer() {
  const tab = S.filterTab;
  const snap = S.snap;
  return html`<aside class="drawer left" role="dialog" aria-label="Filters">
    <header class="dr-head">
      <div><h2>Filters</h2><div class="muted small">${snap.conn.configLoaded ? `Applied on ${snap.nodeCall} for ${snap.loginCall} · revision ${snap.revision}` : snap.conn.mode === "replay" ? "Replay carries no filter profile" : "Your rules load after you connect"}</div></div>
      <button type="button" class="icon-btn" aria-label="Close filters" onClick=${() => set({ drawer: null })}><${Icon} name="close" /></button>
    </header>
    <nav class="tabs" role="tablist">
      ${[["rules", "Rules"], ["presets", "Presets"], ["switches", "Switches"]].map(([k, l]) => html`<button type="button" role="tab" aria-selected=${tab === k ? "true" : "false"} class=${tab === k ? "on" : ""} onClick=${() => set({ filterTab: k })}>${l}${k === "presets" && snap.preset.name ? html`<span class="tab-note">${snap.preset.name}${snap.preset.modified ? "*" : ""}</span>` : null}</button>`)}
    </nav>
    <div class="dr-scroll" key=${tab}>
      ${tab !== "presets" ? html`<${FilterConflicts} />` : null}
      ${tab === "rules" ? html`<${RulesTab} />` : tab === "presets" ? html`<${PresetsTab} />` : html`<${SwitchesTab} />`}
    </div>
    ${tab !== "presets" ? html`<${ApplyBar} />` : null}
  </aside>`;
}

// GoCluster UI — layout, spot views, drawers and boot.
"use strict";

const cluster = new CONN.Cluster();

// ---------------------------------------------------------------- live data plumbing
cluster.on("reset", () => { S.spots = []; S.rate = []; S.selSpot = null; S.draft = null; S.editing = null; S.messages = []; if (S.drawer === "spot") S.drawer = null; notify(true); });
cluster.on("spots", batch => {
  S.spots.push(...batch);
  if (S.spots.length > 1500) S.spots.splice(0, S.spots.length - 1500);
  S.rate.push(...batch.map(s => s.t));
  notify();
});
cluster.on("sample", () => notify());
cluster.on("state", snap => { S.snap = snap; notify(true); });
cluster.on("message", m => { S.messages = [m, ...S.messages].slice(0, 60); notify(); });
cluster.on("notice", n => toast(n.text || String(n), n.kind || "info"));
cluster.on("line", l => { S.lines = [...S.lines, l].slice(-400); notify(); });
cluster.on("history", () => notify(true));
cluster.on("show-history", () => { location.hash = "history"; set({ view: "history" }); });

function spotsPerMinute() {
  const cut = Date.now() - 60e3;
  while (S.rate.length && S.rate[0] < cut) S.rate.shift();
  return S.rate.length;
}

// ---------------------------------------------------------------- derived lists
function rowsForView(focusBand = S.focusBand) {
  const dirty = isDirty();
  const preview = dirty || S.showHidden;
  let base;
  if (preview) {
    const built = workingSet();
    const sw = workingSwitches();
    const src = cluster.sample.slice(-1800);
    base = src.map(s => {
      const d = built.error ? { pass: false, by: "draft has errors" } : decideWith(built.set, sw, s);
      return { s, pass: d.pass, by: d.by };
    });
    if (!S.showHidden) base = base.filter(r => r.pass);
  } else base = S.spots.map(s => ({ s, pass: true, by: s.by }));
  const q = S.search.trim().toUpperCase();
  return base.filter(({ s }) =>
    (focusBand === "ALL" || s.band === focusBand) &&
    (!S.focusCont || s.dxCont === S.focusCont) &&
    (!q || s.dxCall.includes(q) || s.deCall.includes(q) || (s.dxEntity || "").toUpperCase().includes(q)));
}

// ---------------------------------------------------------------- top bar
function Sparkline({ values, w = 64, h = 20 }) {
  const max = Math.max(1, ...values);
  const pts = values.map((v, i) => [(i / Math.max(1, values.length - 1)) * w, h - 2 - (v / max) * (h - 4)]);
  const line = pts.map(([x, y]) => `${x.toFixed(1)},${y.toFixed(1)}`).join(" ");
  const last = pts[pts.length - 1] || [w, h];
  return html`<svg class="spark" width=${w} height=${h} viewBox=${`0 0 ${w} ${h}`} aria-hidden="true">
    <defs><linearGradient id="spk" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="currentColor" stop-opacity=".35"/><stop offset="1" stop-color="currentColor" stop-opacity="0"/></linearGradient></defs>
    <polygon points=${`0,${h} ${line} ${w},${h}`} fill="url(#spk)" />
    <polyline points=${line} fill="none" stroke="currentColor" stroke-width="1.5" stroke-linejoin="round" />
    <circle cx=${last[0]} cy=${last[1]} r="2.2" fill="currentColor" /></svg>`;
}
function perMinute(spots, minutes = 15) {
  const out = new Array(minutes).fill(0), now = Date.now();
  for (let i = spots.length - 1; i >= 0; i--) {
    const age = Math.floor((now - spots[i].t) / 60e3);
    if (age >= minutes) break;
    out[minutes - 1 - age]++;
  }
  return out;
}
const PHASE_WORD = { idle: "offline", connecting: "connecting", login: "logging in", negotiating: "JSON…", disconnected: "offline", failed: "failed", replaying: "replay", "replay-done": "replay" };
function NodeStatus() {
  const snap = S.snap, c = snap.conn;
  const paused = snap.session.pausedUntil > Date.now();
  const rate = spotsPerMinute();
  const live = c.phase === "live";
  const word = live ? (paused ? "paused" : `${rate}`) : PHASE_WORD[c.phase] || c.phase;
  const title = c.mode === "replay" ? "Replaying a captured session. Nothing here is live." : live ? `Live JSON session on ${snap.nodeCall}` : "Not connected. Select to connect.";
  return html`<button type="button" class=${cls("node", paused && "paused", !live && "off", c.mode === "replay" && "replay")} title=${title} onClick=${() => set({ connectOpen: true })}>
    <span class="live-dot"></span>
    <span class="node-call mono">${snap.nodeCall}</span>
    <${Sparkline} values=${perMinute(S.spots).slice(0, -1)} />
    <span class="node-rate mono">${word}<small>${live && !paused ? "/min" : ""}</small></span>
  </button>`;
}
function IconBtn({ icon, label, onClick, active, badge, dot, kbd }) {
  return html`<button type="button" class=${cls("icon-btn", "top", active && "on")} aria-label=${label} title=${kbd ? `${label} (${kbd})` : label} aria-pressed=${active ? "true" : "false"} onClick=${onClick}>
    <${Icon} name=${icon} size=${19} />${badge ? html`<span class="badge-n">${badge}</span>` : null}${dot ? html`<span class="badge-dot"></span>` : null}</button>`;
}
function ToolbarIcon({ icon, hint, dot }) {
  return html`<span class="tool-glyph" aria-hidden="true"><${Icon} name=${icon} />${dot ? html`<span class="badge-dot"></span>` : null}</span>
    <span class="tool-tip" aria-hidden="true">${hint}</span>`;
}

function SpotControls({ filterCount, toggleFilters }) {
  const pausedUntil = S.snap.session.pausedUntil;
  const seconds = Math.max(0, Math.ceil((pausedUntil - Date.now()) / 1000));
  const paused = seconds > 0;
  const pauseLabel = paused ? "Resume live spots" : "Pause live spots";
  return html`<div class="spot-tools" role="group" aria-label="Spot controls">
    <button type="button" class="tool-btn filters-btn" aria-pressed=${S.drawer === "filters" ? "true" : "false"}
        aria-label=${filterCount ? `Filters (${plural(filterCount, "rule")})` : "Filters"} onClick=${toggleFilters}>
      <${ToolbarIcon} icon="filter" hint="Filters (F)" dot=${isDirty()} />
    </button>
    ${S.view === "stream" ? html`<button type="button" class="tool-btn" role="switch" aria-label="Group repeats"
        aria-checked=${S.prefs.groupRepeats ? "true" : "false"} onClick=${() => setPrefs({ groupRepeats: !S.prefs.groupRepeats })}>
      <${ToolbarIcon} icon="layers" hint="Group repeats" />
    </button>` : null}
    <button type="button" class="tool-btn" aria-label="Hidden" aria-pressed=${S.showHidden ? "true" : "false"} onClick=${() => set({ showHidden: !S.showHidden })}>
      <${ToolbarIcon} icon=${S.showHidden ? "eyeOff" : "eye"} hint=${S.showHidden ? "Stop showing hidden spots" : "Show hidden spots"} />
    </button>
    <button type="button" class=${cls("tool-btn", paused && "on")} aria-label=${pauseLabel}
        aria-description=${paused ? `${seconds} seconds remaining.` : "Pauses for 60 seconds. Missed spots are not replayed."}
        disabled=${S.snap.conn.phase !== "live"} onClick=${() => paused ? cluster.resume() : cluster.pause(60)}>
      <${ToolbarIcon} icon=${paused ? "play" : "pause"} hint=${paused ? `${pauseLabel} · ${seconds}s` : "Pause live spots for 60s"} />
    </button>
  </div>`;
}

function TopBar() {
  const snap = S.snap;
  const n = workingCards().length;
  const toggleDrawer = d => set({ drawer: S.drawer === d ? null : d });
  const { buildVersion, visitorIP, loggerConnected } = snap.conn;
  const versionLabel = buildVersion.replace(/^v?/i, "v");
  const subtitle = visitorIP ? `${versionLabel} ${visitorIP}` : versionLabel;
  const presenceLabel = loggerConnected ? "Browser and Telnet share this profile" : "Browser only";
  return html`<header class="topbar">
    <div class="brand"><${BrandMark} size=${30} /><div class="bwords"><span class="bname">GoCluster by N2WQ</span>${buildVersion ? html`<span class="bsub mono" title=${subtitle}>
      <span class="bsub-version">${versionLabel}</span>
      <span class="bsub-presence" role="img" aria-label=${presenceLabel} title=${presenceLabel}><${Icon} name=${loggerConnected ? "sync" : "globe"} size=${13} /></span>
      ${visitorIP ? html`<span class="bsub-ip">${visitorIP}</span>` : null}
    </span>` : null}</div></div>
    <${NodeStatus} />
    <nav class="views" aria-label="View">
      ${[["stream", "Spots", "list"], ["history", "History", "history"], ["bandmap", "Band map", "map"], ["openings", "Openings", "grid"]].map(([v, l, ic]) => html`<a href=${"#" + v} class="tool-btn" aria-label=${l} aria-current=${S.view === v ? "page" : undefined} onClick=${e => { e.preventDefault(); location.hash = v; set({ view: v }); }}><${ToolbarIcon} icon=${ic} hint=${l} /></a>`)}
    </nav>
    <div class="search"><${Icon} name="search" size=${16} /><input id="q" type="search" placeholder="Call, prefix, country" aria-label="Find a call, prefix or country" value=${S.search} onInput=${e => set({ search: e.target.value })} /><kbd>/</kbd></div>
    <div class="actions">
      <${SpotControls} filterCount=${n} toggleFilters=${() => toggleDrawer("filters")} />
      <button type="button" class="preset-pill" onClick=${() => set({ drawer: "filters", filterTab: "presets" })} title=${snap.preset.name ? `Preset: ${snap.preset.name}` : "Presets"}>
        <${Icon} name="bookmark" size=${14} /><span class="mono">${snap.preset.name || "No preset"}</span>${snap.preset.modified ? html`<span class="mod" title="Changed since loaded or saved">modified</span>` : null}
      </button>
      <button type="button" class="spot-btn" onClick=${() => set({ postOpen: true })} aria-label="Post a spot" title="Post a spot"><${Icon} name="send" size=${15} /><span>Spot</span></button>
      <span class="deck-sep"></span>
      <${IconBtn} icon="pulse" label="Activity and bulletins" active=${S.drawer === "activity"} onClick=${() => toggleDrawer("activity")} dot=${S.messages.length && S.messages[0].t > Date.now() - 20e3} />
      <${IconBtn} icon="gear" label="Settings" kbd="S" active=${S.drawer === "settings"} onClick=${() => toggleDrawer("settings")} />
      <button type="button" class="account" onClick=${() => set({ connectOpen: true })} aria-label="Cluster login and connection" title="Cluster login and connection"><span class="avatar mono">${(snap.baseCall || "?").slice(0, 2)}</span><span class="mono">${snap.loginCall || "Connect"}</span></button>
    </div>
  </header>`;
}

// ---------------------------------------------------------------- sub bar: spectrum strip + status
function stripProfile() {
  const conn = S.snap.conn;
  if (conn.mode === "replay") return { kind: "replay", label: "Replay · no filter profile" };
  if (!conn.configLoaded) return { kind: "unknown", label: "Band counts awaiting filter profile" };

  const dirty = isDirty(), draft = S.draft;
  if (dirty && !draft.conflicts.size && draft.base.revision === S.snap.revision && !conn.cfgError && !conn.needsReview) {
    const built = workingSet();
    if (!built.error) return { kind: "preview", set: built.set, cards: draft.cards, switches: draft.switches };
  }
  if (conn.activeDialect !== "go") return { kind: "unknown", label: "GO rules inactive · band counts unavailable" };
  const cards = currentCards();
  const built = preparedCards(cards, _appliedSetMemo);
  if (built.error) return { kind: "unknown", label: "Applied rules unavailable · band counts unavailable" };
  return { kind: "applied", set: built.set, cards, switches: S.snap.switches, pendingDraft: dirty };
}

const stripBandsMemo = new WeakMap();
function stripBands(set) {
  if (!set) return DATA.MAIN_BANDS;
  if (stripBandsMemo.has(set)) return stripBandsMemo.get(set);
  const bands = DATA.MAIN_BANDS.filter(band => GC.mayPassOnBand(set, band));
  stripBandsMemo.set(set, bands);
  return bands;
}

// Keep decisions for the current filter context only. The existing buffers
// own the spot records, and the time window is recomputed even on a quiet feed.
let stripDecisionMemo;
function matchingStripReports({ kind, set, switches }) {
  if (kind === "replay") return () => true;
  const { beacon, toxic } = switches;
  const { nearby, operating } = cluster;
  const old = stripDecisionMemo;
  if (!old || old.set !== set || old.beacon !== beacon || old.toxic !== toxic || old.nearby !== nearby || old.operating !== operating) {
    stripDecisionMemo = { set, beacon, toxic, nearby, operating, decisions: new WeakMap() };
  }
  const { decisions } = stripDecisionMemo;
  return spot => {
    if (!decisions.has(spot)) decisions.set(spot, decideWith(set, switches, spot, false).pass);
    return decisions.get(spot);
  };
}

function stripActivity(profile, bands, now) {
  if (profile.kind === "unknown") return null;
  const bins = 12, span = 15 * 60e3;
  const byBand = new Map(bands.map(band => [band, { count: 0, hist: new Array(bins).fill(0) }]));
  const matches = matchingStripReports(profile);
  let peak = 1, total = 0;
  for (const spot of S.spots) {
    const stat = byBand.get(spot.band), age = now - spot.t;
    if (!stat || !Number.isFinite(spot.t) || age < 0 || age >= span || !matches(spot)) continue;
    const bin = bins - 1 - Math.floor(age / (span / bins));
    peak = Math.max(peak, ++stat.hist[bin]);
    stat.count++;
    total++;
  }
  return { byBand, total, peak };
}

function FilterSummary({ profile }) {
  const cards = profile.cards || [];
  const shows = cards.filter(card => card.action === "pass");
  const hides = cards.filter(card => card.action === "reject");
  const rules = shows.length && hides.length ? [shows[0], hides[0]] : cards.slice(0, 2);
  const more = cards.length - rules.length;
  return html`<div class="status-line">
    ${profile.kind === "preview" ? html`<span class="pill draft" role="status"><${Icon} name="eye" size=${14} /> ${S.snap.conn.activeDialect === "go" ? "Previewing unapplied rules" : "Previewing unapplied GO rules"}</span>`
      : profile.pendingDraft ? html`<span class="pill draft" role="status">Showing applied rules</span>` : null}
    ${S.showHidden ? html`<span class="pill hid"><${Icon} name="eyeOff" size=${14} /> Showing hidden spots, dimmed</span>` : null}
    ${S.focusCont ? html`<button type="button" class="pill focus" onClick=${() => set({ focusCont: "" })}>DX in ${DATA.CONT_NAME[S.focusCont]} <${Icon} name="close" size=${12} /></button>` : null}
    <button type="button" class="summary" onClick=${() => set({ drawer: "filters", filterTab: "rules" })} title="Open filters">
      ${rules.length ? rules.map(card => {
        const description = card.expert ? RULES.cardExpr(card) : RULES.cardSentence(card);
        return html`<span class="summary-rule" key=${card.id} title=${description}><b>${card.action === "pass" ? "Show" : "Hide"}</b> <span>${description}</span></span>`;
      }) : html`<span>${profile.label || "No Show/Hide rules"}</span>`}
      ${more > 0 ? html`<span class="summary-more" title=${plural(more, "more rule")}>+${more}</span>` : null}
    </button>
  </div>`;
}

function SubBar({ profile, bands, focusBand }) {
  const activity = stripActivity(profile, bands, Date.now());
  return html`<div class="subbar">
    <${FilterSummary} profile=${profile} />
    <div class="bandstrip" role="tablist" aria-label="Focus on a band (display only)">
      <button type="button" role="tab" aria-selected=${focusBand === "ALL" ? "true" : "false"} class=${cls("bt", "all", focusBand === "ALL" && "on")} onClick=${() => set({ focusBand: "ALL" })}>
        <span class="bt-top"><span class="bl">All</span><span class="bc mono">${activity?.total ?? ""}</span></span><span class="bt-sub">15 min</span></button>
      ${bands.map(b => {
        const stat = activity?.byBand.get(b);
        return html`<button type="button" role="tab" key=${b} aria-selected=${focusBand === b ? "true" : "false"} title=${stat ? `${b}: ${stat.count} spots in 15 minutes` : `${b}: band count unavailable`}
            class=${cls("bt", focusBand === b && "on", !stat?.count && "quiet")} style=${`--bc:${BAND_COLOR[b]}`} onClick=${() => set({ focusBand: focusBand === b ? "ALL" : b })}>
          <span class="bt-top"><span class="bl">${b}</span><span class="bc mono">${stat?.count || ""}</span></span>
          ${stat ? html`<span class="bt-bars" aria-hidden="true">${stat.hist.map(v => html`<i style=${`height:${v ? 12 + (v / activity.peak) * 88 : 6}%`} class=${v ? "" : "z"}></i>`)}</span>` : null}</button>`;
      })}
    </div>
  </div>`;
}

// ---------------------------------------------------------------- stream columns
// Each column owns its header, width and cell. Order and visibility are per-browser
// preferences; optional columns also step aside when the list gets narrow.
const CONT_HUE = { NA: "#3b82f6", SA: "#10b981", EU: "#8b5cf6", AF: "#f59e0b", AS: "#ef4444", OC: "#06b6d4", AN: "#94a3b8" };
function snrMeter(s) {
  if (s.snr == null) return 0;
  const ft = ["FT8", "FT4", "FT2", "JS8"].includes(s.mode);
  return Math.max(0.06, Math.min(1, ft ? (s.snr + 24) / 36 : s.snr / 40));
}
function repeatCount(call, { group, expanded, toggle }) {
  const label = `${expanded ? "Collapse" : "Expand"} ${call}, ${plural(group.length, "report")}`;
  return html`<button type="button" class="repeat-count mono" title=${label} aria-label=${label}
    aria-expanded=${expanded ? "true" : "false"} aria-controls=${"repeat-" + group[0].s.id} onClick=${toggle}>×${group.length}</button>`;
}
function locatorCenter(grid) {
  if (typeof grid !== "string") return null;
  const locator = grid.trim().toUpperCase();
  if (!/^[A-R]{2}[0-9]{2}(?:[A-X]{2})?$/.test(locator)) return null;
  const west = (locator.charCodeAt(0) - 65) * 20 - 180 + Number(locator[2]) * 2;
  const south = (locator.charCodeAt(1) - 65) * 10 - 90 + Number(locator[3]);
  const fine = locator.length === 6;
  const lon = west + (fine ? (locator.charCodeAt(4) - 65 + 0.5) / 12 : 1);
  const lat = south + (fine ? (locator.charCodeAt(5) - 65 + 0.5) / 24 : 0.5);
  return { lat: lat * Math.PI / 180, lon: lon * Math.PI / 180 };
}
function shortPath(userGrid, dxGrid) {
  const from = locatorCenter(userGrid);
  const to = locatorCenter(dxGrid);
  if (!from || !to) return null;
  const deltaLon = to.lon - from.lon;
  const sinFrom = Math.sin(from.lat), cosFrom = Math.cos(from.lat);
  const sinTo = Math.sin(to.lat), cosTo = Math.cos(to.lat);
  const east = Math.sin(deltaLon) * cosTo;
  const north = cosFrom * sinTo - sinFrom * cosTo * Math.cos(deltaLon);
  const dot = sinFrom * sinTo + cosFrom * cosTo * Math.cos(deltaLon);
  const cross = Math.hypot(east, north);
  // Coincident and antipodal centers have no unique initial direction.
  const bearing = cross < 1e-12 ? null : (Math.round(Math.atan2(east, north) * 180 / Math.PI) + 360) % 360;
  return { km: 6371.0088 * Math.atan2(cross, dot), bearing };
}
function pathMetric(label, kind, value, derived) {
  const mark = value !== null && derived;
  return html`<span class=${"c-metric c-" + kind} title=${mark ? `Estimate using ${derived}` : undefined}>
    <span class="metric-label">${label} </span><span class="mono">${mark ? "≈" : ""}${value ?? "—"}</span></span>`;
}
function viaLabel(s) {
  switch (s.source) {
    case "RBN": case "FT8": case "FT4": return "RBN";
    case "PSKREPORTER": return "PSKr";
    case "UPSTREAM": return s.node?.toUpperCase() === "DXSUMMIT" ? "DXs" : s.node || s.feed;
    case "PEER": return s.node || s.feed;
    default: return s.feed;
  }
}

const COLUMNS = {
  time: { label: "UTC", tpl: "58px", minW: 0, cell: s => html`<span class="c-time mono" title=${s.t != null ? new Date(s.t).toUTCString() : "No time"}>${fmtTime(s.t)}<i class="age">${fmtAge(s.t)}</i></span>` },
  dx: { label: "DX", tpl: "minmax(104px,148px)", fixed: true, cell: (s, r, x) => html`<span class="c-dx">
    ${x.group ? html`<button type="button" class="repeat-dx mono" title=${"Spot details for " + s.dxCall} onClick=${x.openDetails}>${s.dxCall}</button>` : html`<b class="mono">${s.dxCall}</b>`}
    ${x.group && !x.showSpotter ? repeatCount(s.dxCall, x) : null}${x.watch ? html`<span class="wtag" title="On your watch list">watch</span>` : null}${x.own ? html`<span class="wtag own">you</span>` : null}${!r.pass ? html`<span class="why" title=${reasonWords(r.by, workingCards())}><${Icon} name="eyeOff" size=${12} />${r.by && r.by.startsWith("REJECT") ? "Hide #" + r.by.slice(7) : r.by === "no PASS matched" ? "no Show" : r.by}</span>` : null}${x.member && !x.showSpotter ? html`<span class="repeat-member-call mono" title=${"Heard by " + s.deCall}>${s.deCall}</span>` : null}</span>` },
  freq: { label: "kHz", tpl: "94px", align: "r", cell: s => html`<span class="c-freq mono">${fmtFreq(s.freq)}</span>` },
  mode: { label: "Mode", align: "c", tpl: "62px", cell: s => html`<span class="c-mode"><${ModeBadge} mode=${s.mode} /></span>` },
  rep: { label: "SNR", align: "c", tpl: "72px", minW: 720, cell: (s, r, x) => html`<span class="c-rep" title=${x.best?.snr ? `Best SNR: ${fmtReport(x.best.snr)} dB · ${x.best.snr.deCall}` : undefined}>${s.snr != null ? html`<span class="mono">${fmtReport(s)}</span><span class="meter-s"><i style=${`width:${Math.round(snrMeter(s) * 100)}%`}></i></span>` : null}</span>` },
  where: { label: "Where", tpl: "minmax(150px,1.3fr)", minW: 600, cell: s => html`<span class="c-where"><span class="cont" style=${`--ch:${CONT_HUE[s.dxCont] || "#94a3b8"}`}>${s.dxCont || "?"}</span><span class="ent">${s.dxEntity || ""}</span><span class="grid mono">${s.dxGrid || ""}</span>${s.events.map(e => html`<span class="ev">${e}</span>`)}</span>` },
  azimuth: { label: "Azimuth", tpl: "74px", align: "r", title: "Initial short-path bearing from true north", cell: (s, r, x) =>
    pathMetric("Azimuth", "azimuth", x.geometry?.bearing == null ? null : `${String(x.geometry.bearing).padStart(3, "0")}°`, x.derived) },
  distance: { label: "Distance", tpl: "96px", align: "r", defaultHidden: true, title: "Short-path distance", cell: (s, r, x) => {
    const miles = S.prefs.distanceUnit === "mi";
    const km = x.geometry?.km;
    return pathMetric("Distance", "distance", km == null ? null : `${Math.round(miles ? km / 1.609344 : km)} ${miles ? "mi" : "km"}`, x.derived);
  } },
  cmt: { label: "Comment", tpl: "minmax(100px,1fr)", minW: 1040, cell: (s, r, x) => html`<span class="c-cmt" title=${x.group ? undefined : x.cmt}>${x.group ? "" : x.cmt}</span>` },
  de: { label: "Spotter", tpl: "minmax(96px,132px)", minW: 860, cell: (s, r, x) => html`<span class="c-de"><span class="mono" title=${s.deCall || undefined}>${s.deCall}</span>${x.group ? repeatCount(s.dxCall, x) : null}</span>` },
  via: { label: "Via", tpl: "minmax(66px,96px)", minW: 860, cell: s => {
    const label = viaLabel(s);
    return html`<span class="c-via" title=${label || undefined}>${label}</span>`;
  } },
  path: { label: "Path", tpl: "44px", align: "c", title: "Path to your grid", cell: (s, r, x) => html`<span class="c-path"><${PathGlyph} cls=${x.group ? x.best.path?.path : s.path} reportedBy=${x.best?.path?.deCall} /></span>` },
  conf: { label: "CONF", tpl: "48px", align: "c", title: "Callsign confidence", cell: (s, r, x) => html`<span class="c-conf"><${ConfBadge} conf=${x.group ? x.best.conf?.conf : s.conf} reportedBy=${x.best?.conf?.deCall} /></span>` },
};
const DEFAULT_ORDER = ["time", "dx", "freq", "mode", "rep", "where", "cmt", "de", "via", "path", "conf", "azimuth", "distance"];
function colOrder() {
  const o = (S.prefs.colOrder || []).filter(k => COLUMNS[k]);
  for (const k of DEFAULT_ORDER) {
    if (o.includes(k)) continue;
    // Older saved orders keep their Spotter position and gain Via beside it.
    if (k === "via") o.splice(o.indexOf("de") + 1, 0, k);
    else o.push(k);
  }
  return o;
}
function colVisible(k) {
  const saved = S.prefs.colShow[k];
  return COLUMNS[k].fixed || (typeof saved === "boolean" ? saved : !COLUMNS[k].defaultHidden);
}
function moveCol(from, to, after) {
  const o = colOrder().filter(k => k !== from);
  let i = o.indexOf(to); if (after) i++;
  o.splice(i, 0, from);
  setPrefs({ colOrder: o });
}

function SpotRow({ r, sel, fresh, cols, group, best, member, expanded, toggle }) {
  const s = r.s;
  const userGrid = S.snap.operatingGrid;
  const dxGrid = s.dxGrid || "";
  const showGeometry = cols.includes("azimuth") || cols.includes("distance");
  const geometry = useMemo(() => showGeometry ? shortPath(userGrid, dxGrid) : null, [showGeometry, userGrid, dxGrid]);
  const derived = S.snap.operatingGridDerived ? (s.dx?.gridDerived ? "derived user and DX locators" : "a derived user locator")
    : s.dx?.gridDerived ? "a derived DX locator" : "";
  const openDetails = () => set({ selSpot: best?.snr || s, drawer: "spot" });
  const reported = best?.snr || s;
  const x = {
    own: SIM.baseCallOf(s.dxCall) === S.snap.baseCall, watch: watchHit(s.dxCall),
    cmt: (s.cwSpeeds && s.cwSpeeds.length ? fmtCw(s) + " " : "") + (s.comment || ""),
    group, best, member, expanded, toggle, openDetails, showSpotter: cols.includes("de"), geometry, derived,
  };
  return html`<div role="row" tabindex=${group ? undefined : "0"} class=${cls("row", group && "repeat-summary", member && "repeat-member", !r.pass && "hidden", sel && "sel", fresh && "fresh", x.own && "own", x.watch && "watch", s.beacon && "beacon")} style=${`--bc:${BAND_COLOR[s.band] || "var(--line)"}`}
      onClick=${group ? undefined : openDetails} onKeyDown=${group ? undefined : e => { if (e.key === "Enter") openDetails(); }}>
    ${cols.map(k => COLUMNS[k].cell(k === "freq" || k === "rep" || k === "de" || k === "via" ? reported : s, r, x))}
  </div>`;
}

const REPEAT_FREQUENCY_SPAN = 10; // Table frequency in hundredths of a kHz (100 Hz total).
const REPEAT_TIME_SPAN = 60_000_000_000n;
// Only the current display window is grouped. Original reports remain owned by the stream.
function repeatGroups(rows) {
  const groups = [];
  const bySignal = new Map();
  for (const row of rows) {
    const s = row.s;
    if (!row.pass || !s.automated || !s.band || s.band === "???" || s.mode === "UNKNOWN" ||
        !/^-?\d{1,25}$/.test(s.timeNs)) {
      groups.push([row]);
      continue;
    }
    const key = JSON.stringify([s.dxCall, s.band, s.mode]);
    const freq = Math.round(Number(fmtFreq(s.freq)) * 100);
    const time = BigInt(s.timeNs);
    let candidates = bySignal.get(key);
    if (!candidates) {
      candidates = [];
      bySignal.set(key, candidates);
    }
    // Compare to both extremes. Comparing only adjacent reports can hide a QSY
    // or merge several minutes of activity into one continuously growing group.
    let group = candidates.find(g => freq >= g.freqMax - REPEAT_FREQUENCY_SPAN &&
      freq <= g.freqMin + REPEAT_FREQUENCY_SPAN && time >= g.timeMax - REPEAT_TIME_SPAN &&
      time <= g.timeMin + REPEAT_TIME_SPAN);
    if (!group) {
      group = { rows: [], freqMin: freq, freqMax: freq, timeMin: time, timeMax: time };
      candidates.unshift(group);
      groups.push(group.rows);
    }
    group.rows.push(row);
    group.freqMin = Math.min(group.freqMin, freq);
    group.freqMax = Math.max(group.freqMax, freq);
    if (time < group.timeMin) group.timeMin = time;
    if (time > group.timeMax) group.timeMax = time;
  }
  return groups.sort((a, b) => b[b.length - 1].s.id - a[a.length - 1].s.id);
}

const CONFIDENCE_ORDER = ["?", "S", "P", "V"];
function bestRepeatReports(group) {
  const best = {};
  let confidenceRank = -1, pathRank = -Infinity;
  for (let i = group.length - 1; i >= 0; i--) {
    const s = group[i].s;
    if (s.snr != null && (!best.snr || s.snr > best.snr.snr)) best.snr = s;
    const conf = CONFIDENCE_ORDER.indexOf(s.conf);
    if (conf > confidenceRank) {
      best.conf = s;
      confidenceRank = conf;
    }
    const path = DATA.PATH[s.path]?.[2] ?? -Infinity;
    if (path > pathRank) {
      best.path = s;
      pathRank = path;
    }
  }
  return best;
}

function RepeatGroup({ group, cols, fresh }) {
  const [expanded, setExpanded] = useState(false);
  return html`<div role="presentation" class="repeat-group">
    <${SpotRow} r=${group[group.length - 1]} cols=${cols} group=${group} best=${bestRepeatReports(group)}
      fresh=${fresh} expanded=${expanded} toggle=${() => setExpanded(!expanded)} />
    <div role="presentation" class="repeat-members" id=${"repeat-" + group[0].s.id} hidden=${!expanded}>
      ${expanded ? group.slice().reverse().map(r => html`<${SpotRow} key=${r.s.id} r=${r} cols=${cols}
        member=${true} sel=${S.selSpot && S.selSpot.id === r.s.id} />`) : null}
    </div>
  </div>`;
}
// CW speeds arrive already filtered by the user's CW speed setting (D6).
function fmtCw(s) { return (s.cwSpeeds || []).map(x => (/^\d+$/.test(x) ? x + " WPM" : x)).join(" "); }
function diagText(s, mode) {
  switch (mode) {
    case "SOURCE": return `${s.feed}${s.human ? " human" : " skimmer"}`;
    case "DEDUPE": return `${s.band}|${s.deAdif}|${S.snap.settings.dedupe === "SLOW" ? "Z" + s.deZone : String(s.deGrid).slice(0, 2)}|${S.snap.settings.dedupe}`;
    case "CONF": return `conf ${s.conf || "-"} ${s.conf === "V" ? "92%" : s.conf === "P" ? "61%" : s.conf === "S" ? "38%" : "12%"}`;
    case "PATH": return `path ${s.path} n=${20 + (s.id % 30)}`;
    case "MODE": return `mode ${s.mode} via ${s.human ? "comment" : s.feed === "RBN" ? "rbn" : "pskr"}`;
    default: return s.comment;
  }
}
function columnLayout(cols) {
  const widths = S.prefs.colWidths || {};
  let minimum = 35 + 12 * (cols.length - 1);
  const tracks = cols.map(k => {
    const width = widths[k];
    if (Number.isFinite(width) && width >= 32 && width <= 1600) {
      minimum += width;
      return width + "px";
    }
    const track = COLUMNS[k].tpl;
    minimum += Number(track.match(/\d+/)[0]);
    return track;
  });
  return `--cols:${tracks.join(" ")};--grid-min:${minimum}px`;
}
function ColumnHead({ k, drag, setDrag }) {
  const c = COLUMNS[k];
  const gesture = useRef(null);
  function start(e) {
    if (e.button !== 0 || !e.isPrimary) return;
    e.preventDefault();
    const header = e.currentTarget.parentNode;
    const grid = header.closest(".spot-grid");
    const widths = Object.fromEntries(Array.from(header.parentNode.children, el =>
      [el.dataset.column, Math.round(el.getBoundingClientRect().width)]));
    gesture.current = { grid, widths, x: e.clientX, start: widths[k], id: e.pointerId };
    e.currentTarget.setPointerCapture(e.pointerId);
  }
  function move(e) {
    const g = gesture.current;
    if (!g || g.id !== e.pointerId) return;
    g.widths[k] = Math.max(32, Math.min(1600, Math.round(g.start + e.clientX - g.x)));
    const sizes = Object.values(g.widths);
    g.grid.style.setProperty("--cols", sizes.map(n => n + "px").join(" "));
    g.grid.style.minWidth = (35 + 12 * (sizes.length - 1) + sizes.reduce((a, b) => a + b, 0)) + "px";
  }
  function finish(e, save) {
    const g = gesture.current;
    if (!g || g.id !== e.pointerId) return;
    if (save) move(e);
    gesture.current = null;
    g.grid.style.removeProperty("--cols");
    g.grid.style.removeProperty("min-width");
    if (save && g.widths[k] !== g.start)
      setPrefs({ colWidths: { ...S.prefs.colWidths, ...g.widths } });
  }
  const over = drag && drag.over === k && drag.from !== k ? (drag.after ? "drop-after" : "drop-before") : "";
  return html`<span data-column=${k} class=${cls("th", "th-" + k, c.align === "r" && "r", c.align === "c" && "c", drag && drag.from === k && "dragging", over)} title=${c.title || "Drag to reorder"}

    onDragOver=${e => { e.preventDefault(); const rc = e.currentTarget.getBoundingClientRect(); const after = e.clientX > rc.left + rc.width / 2; if (!drag || drag.over !== k || drag.after !== after) setDrag({ ...(drag || {}), over: k, after }); }}
    onDrop=${e => { e.preventDefault(); if (drag && drag.from && drag.from !== k) moveCol(drag.from, k, drag.after); setDrag(null); }}
    onDragEnd=${() => setDrag(null)}><span class="col-label" draggable="true" onDragStart=${e => { e.dataTransfer.effectAllowed = "move"; try { e.dataTransfer.setData("text/plain", k); } catch (x) { /* ignore */ } setDrag({ from: k }); }}><span class="grip" aria-hidden="true">⋮⋮</span>${c.label}</span><span class="resize-grip" role="separator" aria-orientation="vertical" aria-label=${"Resize " + c.label} title=${"Drag to resize " + c.label}
      onPointerDown=${start} onPointerMove=${move} onPointerUp=${e => finish(e, true)} onLostPointerCapture=${e => finish(e, false)} /></span>`;
}
function StreamView({ focusBand }) {
  const ref = useRef(null);
  const secRef = useRef(null);
  const lastScrollSize = useRef(null);
  const [scrolled, setScrolled] = useState(false);
  const [w, setW] = useState(1400);
  const [drag, setDrag] = useState(null);
  useEffect(() => {
    if (!secRef.current || typeof ResizeObserver === "undefined") return;
    const ro = new ResizeObserver(es => setW(es[0].contentRect.width));
    ro.observe(secRef.current);
    return () => ro.disconnect();
  }, []);
  let rows = rowsForView(focusBand);
  const total = rows.length;
  let newer = 0;
  if (scrolled && S.freezeId) { const before = rows.length; rows = rows.filter(r => r.s.id <= S.freezeId); newer = before - rows.length; }
  rows = rows.slice(-S.prefs.rowCap);
  const grouped = S.prefs.groupRepeats;
  const display = grouped ? repeatGroups(rows) : rows.slice().reverse().map(r => [r]);
  const mobile = window.matchMedia && window.matchMedia("(max-width: 760px)").matches; // phone layout stacks cells itself
  const cols = colOrder().filter(k => colVisible(k) && (mobile || !COLUMNS[k].minW || w >= COLUMNS[k].minW));
  const layout = columnLayout(cols);
  const updatePause = paused => {
    setScrolled(paused);
    S.freezeId = paused && rows.length ? rows[rows.length - 1].s.id : 0;
  };
  const onScroll = e => {
    const { scrollTop, scrollHeight, clientHeight, clientWidth } = e.currentTarget;
    const previous = lastScrollSize.current;
    lastScrollSize.current = { height: clientHeight, width: clientWidth };
    const paused = scrollTop > 60;
    // A viewport change can clamp the offset to the end of a now shorter scroll range.
    const resized = previous && (previous.height !== clientHeight || previous.width !== clientWidth);
    if (scrolled && !paused && resized && scrollTop >= scrollHeight - clientHeight - 1) return;
    if (paused !== scrolled) updatePause(paused);
  };
  const resume = () => {
    // A clamped scrollport may already be at the top and emit no scroll event.
    if (ref.current.scrollTop <= 60) updatePause(false);
    ref.current.scrollTo({ top: 0, behavior: "smooth" });
  };
  const mountT = useRef(Date.now());
  const preview = isDirty() || S.showHidden;
  const hiddenN = S.showHidden ? rows.filter(r => !r.pass).length : 0;
  const auto = mobile ? [] : colOrder().filter(k => colVisible(k) && COLUMNS[k].minW && w < COLUMNS[k].minW);
  return html`<section ref=${secRef} class=${cls("stream", S.prefs.density, drag && "is-dragging")} style=${layout} aria-label="Spots">
    ${newer > 0 ? html`<button type="button" class="newer" onClick=${resume}><${Icon} name="chevronDown" size=${14} /> ${plural(newer, "newer spot")}</button>` : null}
    <div class="spot-scroll" ref=${ref} onScroll=${onScroll}><div class="spot-grid">
    <div class="thead" role="row">${cols.map(k => html`<${ColumnHead} key=${k} k=${k} drag=${drag} setDrag=${setDrag} />`)}</div>
    <div class="tbody" role="rowgroup">
      ${display.map(group => {
        const r = group[group.length - 1];
        const fresh = r.s.t > mountT.current && Date.now() - r.s.t < 2500;
        return group.length > 1
          ? html`<${RepeatGroup} key=${"group-" + group[0].s.id} group=${group} cols=${cols} fresh=${fresh} />`
          : html`<${SpotRow} key=${r.s.id} cols=${cols} r=${r} sel=${S.selSpot && S.selSpot.id === r.s.id} fresh=${fresh} />`;
      })}
      ${!rows.length ? html`<div class="empty"><${Icon} name="radio" size=${28} /><b>${S.snap.conn.phase === "live" || S.snap.conn.mode === "replay" || S.spots.length ? "No spots match right now" : "Not connected"}</b><span>${focusBand !== "ALL" || S.search || S.focusCont ? "Clear the band focus or search to see more." : S.snap.conn.phase === "live" ? "Spots appear here as the node sends them. Quiet periods are normal; if it stays empty, open Filters to check your rules." : S.snap.conn.mode === "replay" ? "The capture's spots appear as it plays." : "Connect to your cluster to see live spots."}</span>
        <button type="button" class="btn" onClick=${() => set(focusBand !== "ALL" || S.search || S.focusCont ? { focusBand: "ALL", search: "", focusCont: "" } : S.snap.conn.phase === "live" ? { drawer: "filters" } : { connectOpen: true })}>${focusBand !== "ALL" || S.search || S.focusCont ? "Show all" : S.snap.conn.phase === "live" ? "Open filters" : "Connect"}</button></div>` : null}
    </div></div></div>
    <footer class="tfoot muted small"><span>${preview ? `Preview over the ${plural(Math.min(cluster.sample.length, 1800), "spot")} you have received; spots your current rules already hide are not here${S.showHidden ? ` · ${hiddenN} hidden shown dimmed` : ""}` : `${plural(total, "spot")} received${focusBand !== "ALL" ? ` on ${focusBand}` : ""}${S.snap.conn.mode === "replay" ? " from the capture" : ""}`} · newest first</span>
      <span class="grow"></span><span>${auto.length ? `${auto.map(k => COLUMNS[k].label).join(", ")} hidden to fit · ` : ""}Drag titles to reorder or edges to resize</span></footer>
  </section>`;
}

// Apply the map views' rules and local focus to reports already received.
function matchingMapSpots(sample, set, sw) {
  const q = S.search.trim().toUpperCase();
  return sample.filter(s => decideWith(set, sw, s).pass &&
    (!S.focusCont || s.dxCont === S.focusCont) &&
    (!q || s.dxCall.includes(q) || s.deCall.includes(q) || (s.dxEntity || "").toUpperCase().includes(q)));
}

const _passMemo = { key: "", val: [] };
function passingSpots(minutes) {
  const built = workingSet();
  const sw = workingSwitches();
  const key = `${minutes}|${_setMemo.key}|${JSON.stringify(sw)}|${cluster.sample.length}|${S.focusCont}|${S.search}`;
  if (_passMemo.key === key) return _passMemo.val;
  const val = built.error ? [] : matchingMapSpots(recentSample(minutes), built.set, sw);
  _passMemo.key = key; _passMemo.val = val;
  return val;
}

// ---------------------------------------------------------------- band map
function BandMapView({ focusBand }) {
  const counts = {};
  const pass = passingSpots(10);
  for (const s of pass) counts[s.band] = (counts[s.band] || 0) + 1;
  const band = focusBand !== "ALL" ? focusBand : (Object.entries(counts).sort((a, b) => b[1] - a[1])[0] || ["20m"])[0];
  const [lo, hi] = DATA.BAND_PLAN[band] || [14000, 14350];
  const span = Math.min(hi - lo, band === "10m" ? 700 : band === "6m" ? 400 : band === "2m" ? 500 : hi - lo);
  const hiShown = lo + span;
  const latest = new Map();
  for (const s of pass) if (s.band === band && s.freq >= lo && s.freq <= hiShown) latest.set(s.dxCall, s);
  const spots = [...latest.values()].sort((a, b) => a.freq - b.freq);
  const W = 1000, LABEL = 86;
  const lanes = [];
  const placed = spots.map(s => {
    const x = ((s.freq - lo) / span) * W;
    let lane = lanes.findIndex(end => end < x - 2);
    if (lane < 0) { lane = lanes.length; lanes.push(0); }
    lanes[lane] = x + LABEL;
    return { s, x, lane };
  });
  const step = span <= 60 ? 5 : span <= 150 ? 10 : span <= 400 ? 25 : 50;
  const ticks = [];
  for (let f = Math.ceil(lo / step) * step; f <= hiShown; f += step) ticks.push(f);
  return html`<section class="bandmap" aria-label=${"Band map for " + band}>
    <div class="bm-head">
      <h2>${band}</h2><span class="muted">${plural(spots.length, "station")} passing your ${isDirty() ? "edited " : ""}filters in the last 10 minutes · ${lo}–${hiShown} kHz</span>
      <span class="grow"></span>
      <div class="legend"><span class="lg cw">CW</span><span class="lg data">Digital</span><span class="lg phone">Phone</span></div>
    </div>
    <div class="bm-scroll">
      <div class="bm-canvas" style=${`--lanes:${Math.max(lanes.length, 6)}`}>
        <div class="bm-segs">${(DATA.SEGMENTS[band] || []).filter(([a]) => a < hiShown).map(([a, b, k]) => html`<div class=${"seg-" + k} style=${`left:${((a - lo) / span) * 100}%;width:${((Math.min(b, hiShown) - a) / span) * 100}%`}><span>${k === "data" ? "digital" : k}</span></div>`)}</div>
        <div class="bm-axis">${ticks.map(f => html`<span style=${`left:${((f - lo) / span) * 100}%`}>${f}</span>`)}</div>
        <div class="bm-spots">${placed.map(({ s, x, lane }) => {
          const age = (Date.now() - s.t) / 600e3;
          return html`<button type="button" class=${cls("bm-spot", "m-" + (DATA.MODE_FAMILY[s.mode] || "unk"), watchHit(s.dxCall) && "watch", S.selSpot && S.selSpot.id === s.id && "sel")} style=${`left:${(x / W) * 100}%;top:${lane * 34}px;opacity:${effectiveContrast() === "high" ? 1 : Math.max(0.72, 1 - age * 0.4)}`}
            title=${`${s.dxCall} ${s.freq} ${s.mode} · ${s.dxEntity || ""}`} onClick=${() => set({ selSpot: s, drawer: "spot" })}>
            <b class="mono">${s.dxCall}</b><span>${s.mode}</span><${PathGlyph} cls=${s.path} size="sm" /></button>`;
        })}</div>
      </div>
    </div>
    <p class="muted small bm-note">Choose a band in the strip above. Labels fade with age; color shows the mode family.</p>
  </section>`;
}

// ---------------------------------------------------------------- openings
function OpeningsView({ bands }) {
  const conts = ["NA", "SA", "EU", "AF", "AS", "OC"];
  const bandSet = new Set(bands);
  const now = Date.now();
  const sample = cluster.sample, built = workingSet(), sw = workingSwitches();
  // Batches replace the last report even when the retained sample is full.
  // Window membership is recomputed as time advances, including on idle feeds.
  const matches = useMemo(() => built.error ? [] : matchingMapSpots(sample, built.set, sw),
    [sample, sample.at(-1), built, sw.beacon, sw.self, sw.toxic, cluster.baseCall, cluster.nearby, cluster.operating, S.focusCont, S.search]);
  const grid = {};
  const rank = { HIGH: 4, MEDIUM: 3, LOW: 2, UNLIKELY: 1, CLOSED: 0 };
  let max = 1;
  for (const s of matches) {
    const age = now - s.t;
    if (!(age >= 0 && age < 15 * 60e3) || !bandSet.has(s.band) || !conts.includes(s.dxCont)) continue;
    const k = s.band + "|" + s.dxCont;
    const g = grid[k] = grid[k] || { calls: new Set(), countries: new Map(), p: 0, pn: 0 };
    g.calls.add(s.dxCall);
    const prefix = RULES.ADIF_LABEL.get(s.dxAdif);
    if (prefix) g.countries.set(prefix, (g.countries.get(prefix) || 0) + 1);
    if (s.path in rank) { g.p += rank[s.path]; g.pn++; }
    max = Math.max(max, g.calls.size);
  }
  return html`<section class="openings" aria-label="Openings">
    <div class="op-head"><h2>Where the bands are open</h2><span class="muted">Stations that pass your ${isDirty() ? "edited " : ""}filters in the last 15 minutes, by band and DX continent. Shade shows the average path from ${S.snap.settings.grid || "your grid"}.</span></div>
    <div class="op-scroll">${bands.length ? html`<table class="op-table"><colgroup><col class="bandcol" />${conts.map(() => html`<col />`)}</colgroup>
      <thead><tr><th></th>${conts.map(c => html`<th scope="col">${DATA.CONT_NAME[c]}</th>`)}</tr></thead>
      <tbody>${bands.map(b => html`<tr><th scope="row"><span class="bdot" style=${`--bc:${BAND_COLOR[b]}`}></span>${b}</th>${conts.map(c => {
        const g = grid[b + "|" + c];
        const n = g ? g.calls.size : 0;
        const avg = g && g.pn ? g.p / g.pn : -1;
        const tone = avg < 0 ? "none" : avg >= 3.2 ? "high" : avg >= 2.4 ? "med" : avg >= 1.6 ? "low" : "poor";
        const prefixes = g ? [...g.countries].sort(([a, ac], [b, bc]) => bc - ac || a.localeCompare(b, "en"))
          .slice(0, 5).map(([prefix]) => prefix) : [];
        const label = `${b} to ${DATA.CONT_NAME[c]}: ${plural(n, "station")}` + (prefixes.length ? `. Top DX countries: ${prefixes.join(", ")}` : "");
        return html`<td><button type="button" class=${cls("cell", "t-" + tone, !n && "zero")} style=${`--fill:${(n / max).toFixed(2)}`} disabled=${!n}
          aria-label=${label} onClick=${() => { location.hash = "stream"; set({ view: "stream", focusBand: b, focusCont: c }); }}>
          <span class="n">${n || "·"}</span>${prefixes.length ? html`<span class="op-prefixes">${prefixes.map(prefix => html`<span>${prefix}</span>`)}</span>` : null}
          ${n ? html`<span class="bar"><i style=${`width:${Math.round((n / max) * 100)}%`}></i></span>` : null}</button></td>`;
      })}</tr>`)}</tbody>
    </table>` : html`<p class="muted">No bands match your filters.</p>`}</div>
    ${bands.length ? html`<div class="op-legend muted small"><span class="sw t-high"></span>favorable <span class="sw t-med"></span>workable <span class="sw t-low"></span>weak <span class="sw t-poor"></span>poor <span class="sw t-none"></span>not rated · Select a cell to see those spots.</div>` : null}
  </section>`;
}

// ---------------------------------------------------------------- spot drawer
function SpotDrawer() {
  const s = S.selSpot;
  if (!s) return null;
  const conf = DATA.CONF[s.conf];
  const pth = DATA.PATH[s.path] || DATA.PATH.INSUFFICIENT;
  const prop = useMemo(() => cluster.prop(s.dxCont, s.band, s.mode), [s.id, S.snap.settings.grid, S.snap.settings.noise]);
  const hist = cluster.sampleFor(s.dxCall).filter(h => h.id !== s.id).slice().reverse().slice(0, 8);
  const live = S.snap.conn.phase === "live";
  const showHistory = () => { try { cluster.historySearch({ selector: s.dxCall, count: 10 }); location.hash = "history"; set({ view: "history", drawer: null }); } catch (e) { toast(e.message, "error"); } };
  const d = decideWith(workingSet().set, workingSwitches(), s);
  const close = () => set({ drawer: null, selSpot: null });
  return html`<aside class="drawer right spot-dr" role="dialog" aria-label=${"Spot details for " + s.dxCall} style=${`--bc:${BAND_COLOR[s.band] || "var(--accent)"}`}>
    <header class="dr-head">
      <div><h2 class="mono callsign">${s.dxCall}</h2><div class="sd-sub"><span class="cont" style=${`--ch:${CONT_HUE[s.dxCont] || "#94a3b8"}`}>${s.dxCont}</span><span>${s.dxEntity || "Unknown entity"}</span>${s.dxZone != null ? html`<span>·</span><span>CQ ${s.dxZone}</span>` : null}${s.dxState ? html`<span>· ${s.dxState}</span>` : null}${s.kind === "history" ? html`<span class="ev">history</span>` : null}${s.events.map(e => html`<span class="ev">${e}</span>`)}</div></div>
      <button type="button" class="icon-btn" aria-label="Close" onClick=${close}><${Icon} name="close" /></button>
    </header>
    <div class="dr-scroll">
      <div class="sd-freq">
        <div><div class="fbig mono">${fmtFreq(s.freq)}<span>kHz</span></div><div class="muted">${s.band} · <${ModeBadge} mode=${s.mode} />${s.snr != null ? html` · <span class="mono">${fmtReport(s)} dB</span>` : null}${s.cwSpeeds && s.cwSpeeds.length ? ` · ${fmtCw(s)}` : ""}${s.obsFreq != null ? html` · <span class="mono" title="Frequency as observed">obs ${fmtFreq(s.obsFreq)}</span>` : null}</div></div>
        <button type="button" class="btn" onClick=${() => copyText(fmtFreq(s.freq), "Frequency copied")}><${Icon} name="copy" size=${15} /> Copy</button>
      </div>
      ${s.comment ? html`<p class="sd-cmt">“${s.comment}”</p>` : null}

      <dl class="facts">
        <div><dt>Heard by</dt><dd><span class="mono">${s.deCall}</span>${s.deEntity ? ` · ${s.deEntity}` : ""}${s.deCont ? ` (${s.deCont})` : ""}${s.deGrid ? html` · <span class="mono">${s.deGrid}</span>` : null}</dd></div>
        <div><dt>Source</dt><dd>${s.feed || "Unknown"}${s.human ? ", posted by a person" : ", automated skimmer"}${s.beacon ? ", beacon" : ""}${s.node ? ` · node ${s.node}` : ""}</dd></div>
        <div><dt>Time</dt><dd class="mono" title=${s.timeNs ? s.timeNs + " ns" : ""}>${fmtTime(s.t, "utc")} · ${fmtAge(s.t)} ago</dd></div>
        <div><dt>Grid</dt><dd class="mono">${s.dxGrid ? s.dxGrid + (s.dx && s.dx.gridDerived ? " (derived)" : "") : "unknown"}${s.dx && s.dx.iaru ? html` <span class="muted">· IARU ${s.dx.iaru}</span>` : null}</dd></div>
      </dl>

      <section class="sd-card">
        <div class="sd-row"><${ConfBadge} conf=${s.conf} big /><div><b>${conf ? conf[0] : "Not rated"}</b><span class="muted">${conf ? conf[1] : "Confidence is not rated for this mode."}</span></div></div>
      </section>
      <section class="sd-card">
        <div class="sd-row"><${PathGlyph} cls=${s.path} size="lg" /><div><b>${s.path ? `Path ${pth[1].toLowerCase()}` : "Path not shown"}</b><span class="muted">${!s.path ? (s.kind === "history" ? "History rows carry no path rating." : "Path display is off for this login.") : s.path === "INSUFFICIENT" ? "Not enough recent evidence to rate this path. Blank does not mean closed." : `From ${S.snap.settings.grid || "your grid"}${S.snap.settings.noise ? ` with ${S.snap.settings.noise.toLowerCase()} noise` : ""}. A hint, not a promise.`}</span></div></div>
        ${prop ? html`<div class="prop" aria-label="Next 12 hours"><div class="muted small">Next 12 hours on ${s.band} ${s.mode === "FT8" ? "FT8" : "CW"}</div>
          <div class="prop-row">${prop.slice(0, 12).map(p => html`<div class=${"ph p-" + p.cls.toLowerCase()} title=${`${pad2(p.hour)}Z ${p.cls}`}><i></i><span class="mono">${pad2(p.hour)}</span></div>`)}</div></div>` : null}
      </section>

      <section class=${cls("sd-why", d.pass ? "pass" : "hide")}>
        <div class="muted small">${d.pass ? (isDirty() ? "Your edited rules would show this" : "Why you see this, as this browser reads your rules") : (isDirty() ? "Your edited rules would hide this" : "This browser reads your current rules as hiding this")}</div>
        <b>${reasonWords(d.by, workingCards())}</b>
      </section>

      <div class="sd-actions">
        <button type="button" class="btn" onClick=${() => hideFromSpot("DXCALL", s.dxCall)}><${Icon} name="eyeOff" size=${15} /> Hide ${s.dxCall}</button>
        <button type="button" class="btn" onClick=${() => hideFromSpot("DXDXCC", String(s.dxAdif))}><${Icon} name="eyeOff" size=${15} /> Hide ${s.dxEntity}</button>
        <button type="button" class="btn" onClick=${() => { const w = S.prefs.watch.includes(s.dxCall) ? S.prefs.watch.filter(x => x !== s.dxCall) : [...S.prefs.watch, s.dxCall]; setPrefs({ watch: w }); toast(w.includes(s.dxCall) ? `Watching ${s.dxCall}. It will be highlighted.` : `Stopped watching ${s.dxCall}.`); }}>
          <${Icon} name="bookmark" size=${15} /> ${S.prefs.watch.includes(s.dxCall) ? "Unwatch" : "Watch"}</button>
      </div>

      <section class="sd-hist">
        <div class="sd-hist-head"><h3>Other reports you received</h3><button type="button" class="link sm" disabled=${!live} title=${live ? "Search the node's archive" : "Connect Live to search the archive"} onClick=${showHistory}>SH DX ${s.dxCall} 10</button></div>
        ${hist.length ? html`<ul>${hist.map(h => html`<li><span class="mono">${fmtTime(h.t)}</span><span class="mono">${fmtFreq(h.freq)}</span><span class="mono de">${h.deCall}</span><span class="muted">${h.deCont || ""}</span><span class="mono">${fmtReport(h)}</span></li>`)}</ul>` : html`<p class="muted small">None in this session yet.</p>`}
      </section>
    </div>
  </aside>`;
}

// ---------------------------------------------------------------- settings drawer
function ColumnEditor() {
  const order = colOrder();
  const mv = (i, d) => { const o = order.slice(); const [k] = o.splice(i, 1); o.splice(i + d, 0, k); setPrefs({ colOrder: o }); };
  return html`<ol class="col-ed">${order.map((k, i) => {
    const c = COLUMNS[k], vis = colVisible(k);
    return html`<li class=${vis ? "" : "off"}>
      <span class="ce-n mono">${i + 1}</span><span class="ce-l">${k === "time" ? (S.prefs.timeMode === "local" ? "Time (local)" : "Time (UTC)") : c.label}</span>
      <button type="button" class="icon-btn sm" aria-label=${"Move " + c.label + " left"} disabled=${i === 0} onClick=${() => mv(i, -1)}><${Icon} name="chevronDown" size=${15} /></button>
      <button type="button" class="icon-btn sm down" aria-label=${"Move " + c.label + " right"} disabled=${i === order.length - 1} onClick=${() => mv(i, 1)}><${Icon} name="chevronDown" size=${15} /></button>
      ${c.fixed ? html`<span class="ce-fixed muted small">always</span>` : html`<${Toggle} on=${vis} label=${"Show " + c.label} onChange=${v => setPrefs({ colShow: { ...S.prefs.colShow, [k]: v } })} />`}
    </li>`;
  })}</ol>
  <button type="button" class="link sm" onClick=${() => setPrefs({ colOrder: DEFAULT_ORDER.slice(), colShow: {}, colWidths: {} })}>Reset columns</button>`;
}
function SettingsDrawer() {
  const snap = S.snap, st = snap.settings, c = snap.conn;
  const [grid, setGrid] = useState(st.grid);
  const [gridErr, setGridErr] = useState("");
  const [watch, setWatch] = useState((S.prefs.watch || []).join(" "));
  const [yamlOpen, setYamlOpen] = useState(false);
  useEffect(() => setGrid(st.grid), [st.grid]);
  const can = c.canWrite;
  const why = c.mode === "replay" ? "Replay is read-only." : c.phase !== "live" ? "Connect to change node settings." : !c.configLoaded ? "Your profile is still loading." : c.writing ? "Saving…" : "";
  const patch = async (p, msg) => {
    try {
      const r = await cluster.patchSettings(p, snap.revision);
      toast(r && r.confirmed === false ? "The node accepted the change, but its readback differs. Check the value shown." : msg || "Saved on the node and confirmed by readback.", r && r.confirmed === false ? "error" : "ok");
      return true;
    } catch (e) { toast(e.message, "error"); return false; }
  };
  const saveGrid = () => {
    const g = GC.validGrid(grid);
    if (!g) { setGridErr("Enter a 4 or 6 character Maidenhead locator, such as FN31 or FN31PR."); return; }
    setGridErr(""); patch({ grid: g }, `Grid set to ${g}. Path hints and Near me now use it.`);
  };
  return html`<aside class="drawer right" role="dialog" aria-label="Settings">
    <header class="dr-head"><div><h2>Settings</h2><div class="muted small">${c.configLoaded ? `Saved on ${snap.nodeCall} for ${snap.loginCall} unless marked otherwise` : "Node settings load after you connect"}</div></div>
      <button type="button" class="icon-btn" aria-label="Close settings" onClick=${() => set({ drawer: null })}><${Icon} name="close" /></button></header>
    <div class="dr-scroll set-body">
      ${why ? html`<div class="lint warn"><${Icon} name="info" size=${15} /><span>${why}</span>${c.phase !== "live" && c.mode !== "replay" ? html`<button type="button" class="link sm" onClick=${() => set({ connectOpen: true })}>Connect</button>` : null}</div>` : null}
      <section class="sgroup">
        <h3>Readability <span class="muted small">this browser</span></h3>
        <div class="field"><span class="flabel">Theme</span><${Seg} label="Theme" value=${S.prefs.theme} onChange=${v => setPrefs({ theme: v })} options=${[["system", "System"], ["light", "Light"], ["dark", "Dark"]]} /></div>
        <div class="field"><span class="flabel">Contrast</span><${Seg} label="Contrast" value=${S.prefs.contrast} onChange=${v => setPrefs({ contrast: v })} options=${[["system", "System"], ["standard", "Standard"], ["high", "Maximum"]]} />
          <p class="hint">Maximum uses black and white text, heavier borders and no faded items. System follows your device's “increase contrast” setting.</p></div>
        <div class="field"><span class="flabel">Text size</span><${Seg} label="Text size" value=${S.prefs.textSize} onChange=${v => setPrefs({ textSize: v })} options=${[["normal", "Normal"], ["large", "Large"], ["xl", "Extra large"]]} /></div>
      </section>
      <section class="sgroup">
        <h3>Your station</h3>
        <div class="field row">
          <div><span class="flabel">Login</span><p class="hint mono">${snap.loginCall || "not set"}${c.phase === "live" ? ` · live on ${snap.nodeCall}` : ""}</p></div>
          <button type="button" class="btn" onClick=${() => set({ connectOpen: true })}>${c.phase === "live" ? "Change" : "Connect"}</button>
        </div>
        <div class="field">
          <label for="grid">Grid locator</label>
          <div class="inline-form"><input id="grid" class="txt mono" maxlength="6" value=${grid} placeholder="FN31PR" disabled=${!can} onInput=${e => { setGrid(e.target.value.toUpperCase()); setGridErr(""); }} onKeyDown=${e => e.key === "Enter" && saveGrid()} />
            <button type="button" class="btn" disabled=${!can || grid === st.grid} onClick=${saveGrid}>Save</button></div>
          ${gridErr ? html`<div class="field-err">${gridErr}</div>` : html`<p class="hint">Used for path hints and the “Near me” condition.${!st.grid && c.configLoaded ? " Not set: the node derives one from your call where it can." : ""}</p>`}
        </div>
        <div class="field">
          <span class="flabel" id="noise-l">Receive noise at your location</span>
          <div class="cards-3" role="radiogroup" aria-labelledby="noise-l">${DATA.NOISE.map(([k, l, db, d]) => html`<button type="button" role="radio" aria-checked=${st.noise === k ? "true" : "false"} class=${cls("opt", st.noise === k && "on")} disabled=${!can} onClick=${() => patch({ noise: k }, `Noise set to ${l.toLowerCase()}.`)}><b>${l}</b><span class="mono">${db ? "−" + db : "0"} dB</span><small>${d}</small></button>`)}</div>
          <p class="hint">Lowers receive-path ratings by your local noise. It describes your site, not the band.</p>
        </div>
      </section>

      <section class="sgroup">
        <h3>Spot feed</h3>
        <div class="field">
          <span class="flabel" id="dd-l">Repeated spots</span>
          <div class="cards-3" role="radiogroup" aria-labelledby="dd-l">${DATA.DEDUPE.map(([k, l, sec, d]) => html`<button type="button" role="radio" aria-checked=${st.dedupe === k ? "true" : "false"} class=${cls("opt", st.dedupe === k && "on")} disabled=${!can || !snap.server.dedupe[k]} onClick=${() => patch({ dedupe: k }, `Repeats set to ${l.toLowerCase()}.`)}><b>${l}</b><span class="mono">${Math.round(sec / 60)} min</span><small>${d}</small></button>`)}</div>
        </div>
        <div class="field row">
          <div><label for="cws">Show CW speed</label><p class="hint">Adds WPM from the Reverse Beacon Network to CW spots. The node applies it.</p></div>
          <${Toggle} id="cws" on=${st.cwSpeed} disabled=${!can} label="Show CW speed" onChange=${v => patch({ cwSpeed: v })} />
        </div>
        <div class="field">
          <label for="ps">Path evidence required</label>
          <div class="inline-form"><select id="ps" class="txt" value=${String(st.pathSamples)} disabled=${!can} onChange=${e => patch({ pathSamples: Number(e.target.value) })}>
            <option value="0">Node default${snap.server.pathMin.stations != null ? ` (${snap.server.pathMin.stations} reports)` : ""}</option>${[25, 30, 40, 60].concat(st.pathSamples && ![25, 30, 40, 60].includes(st.pathSamples) ? [st.pathSamples] : []).map(n => html`<option value=${n}>${n} reports (stricter)</option>`)}</select></div>
          <p class="hint">A higher number shows fewer path ratings but trusts them more. You can only make it stricter than the node.</p>
        </div>
        <div class="field">
          <span class="flabel" id="sol-l">Solar summaries</span>
          <${Seg} label="Solar summaries" value=${st.solar} onChange=${v => can && patch({ solar: v })} options=${[[0, "Off"], [15, "15 min"], [30, "30 min"], [60, "60 min"]]} />
          <p class="hint">Sent while space weather is active.</p>
        </div>
      </section>

      <section class="sgroup">
        <h3>Display <span class="muted small">this browser</span></h3>
        <div class="field"><span class="flabel">Row density</span><${Seg} label="Density" value=${S.prefs.density} onChange=${v => setPrefs({ density: v })} options=${[["comfortable", "Comfortable"], ["compact", "Compact"]]} /></div>
        <div class="field"><span class="flabel">Time</span><${Seg} label="Time" value=${S.prefs.timeMode} onChange=${v => setPrefs({ timeMode: v })} options=${[["utc", "UTC"], ["local", "Local"]]} /></div>
        <div class="field"><span class="flabel">Columns</span><${ColumnEditor} /><p class="hint">Or drag the column titles in the spot list. Narrow desktop windows hide some columns to fit.</p></div>
        <div class="field"><span class="flabel">Distance units</span><${Seg} label="Distance units" value=${S.prefs.distanceUnit} onChange=${v => setPrefs({ distanceUnit: v })} options=${[["km", "Kilometers"], ["mi", "Miles"]]} /></div>
        <div class="field">
          <label for="watch">Watch list</label>
          <div class="inline-form"><input id="watch" class="txt mono" value=${watch} placeholder="VP8 ZD7 K1ABC" onInput=${e => setWatch(e.target.value.toUpperCase())} />
            <button type="button" class="btn" onClick=${() => { setPrefs({ watch: watch.split(/[\s,]+/).filter(Boolean) }); toast("Watch list saved in this browser."); }}>Save</button></div>
          <p class="hint">Calls or prefixes to highlight. Display only; it does not change what the node sends.</p>
        </div>
      </section>

      <section class="sgroup">
        <h3>Advanced</h3>
        <div class="field row"><div><span class="flabel">Command dialect</span><p class="hint">${!c.configLoaded ? "Shown after your profile loads." : c.activeDialect === "go" ? "GO is active, so numbered rules and every console command work. Your native CC filters are kept and not changed here." : `This login uses ${c.activeDialect.toUpperCase()}. The console edits GO rules; switching keeps your CC filters and is saved for ${snap.loginCall}.`}</p></div>
          ${c.configLoaded ? (c.activeDialect === "go" ? html`<span class="badge ok">GO</span>` : html`<button type="button" class="btn" disabled=${!can} onClick=${() => patch({ dialect: "go" }, "Switched this login to GO. Your CC filters are kept.")}>Switch to GO</button>`) : null}</div>
        <div class="field">
          <span class="flabel">Backup</span>
          <div class="btn-row"><button type="button" class="btn" disabled=${!c.configLoaded} onClick=${() => setYamlOpen(!yamlOpen)}><${Icon} name="code" size=${15} /> ${yamlOpen ? "Hide" : "Show"} configuration</button>
            <button type="button" class="btn ghost" disabled=${!c.configLoaded} onClick=${() => copyText(cluster.configYaml(), "Configuration copied")}><${Icon} name="copy" size=${15} /> Copy</button>
            <button type="button" class="btn ghost" disabled=${!can} onClick=${() => cluster.refreshConfig().then(() => toast("Profile reloaded from the node."), e => toast(e.message, "error"))}><${Icon} name="undo" size=${15} /> Reload</button></div>
          ${yamlOpen ? html`<pre class="sends">${cluster.configYaml()}</pre>` : null}
          <p class="hint">The schema 7 document exactly as GET YAML CONFIG SCHEMA 7 returned it${c.configLoaded ? `, revision ${snap.revision}` : ""}. Keep it to restore both filter banks and your settings.</p>
        </div>
        <div class="field"><button type="button" class="btn ghost danger" onClick=${() => { try { for (const k of ["gcui.prefs", "gcui.login", "gcui.hist"]) localStorage.removeItem(k); } catch (e) { /* ignore */ } location.reload(); }}>Forget this browser's display settings</button>
          <p class="hint">Clears theme, columns, watch list, saved login and console history here. Nothing on the node changes.</p></div>
      </section>
    </div>
  </aside>`;
}

// ---------------------------------------------------------------- activity drawer
function ActivityDrawer() {
  const live = S.snap.conn.phase === "live";
  const last = S.messages.find(m => m.solar);
  const sol = cluster.solar;
  const v = x => (x == null ? "—" : x);
  return html`<aside class="drawer right" role="dialog" aria-label="Activity">
    <header class="dr-head"><div><h2>Activity</h2><div class="muted small">Space weather, who hears you and messages</div></div>
      <button type="button" class="icon-btn" aria-label="Close" onClick=${() => set({ drawer: null })}><${Icon} name="close" /></button></header>
    <div class="dr-scroll">
      <section class="sw-tiles">
        <div class="tile"><span class="muted small">Solar flux</span><b class="mono">${v(sol.sfi)}</b></div>
        <div class="tile"><span class="muted small">A index</span><b class="mono">${v(sol.a)}</b></div>
        <div class=${cls("tile", sol.k >= 4 && "warn")}><span class="muted small">K index</span><b class="mono">${v(sol.k)}</b></div>
      </section>
      <p class="muted small">${last ? `From the latest ${last.kind} bulletin at ${fmtTime(last.t)}.` : "Filled in from the next WWV or WCY bulletin the node sends you."}</p>

      <section class="who">
        <div class="who-head"><h3>Who hears ${S.snap.baseCall || "you"}</h3>
          <button type="button" class="btn sm" disabled=${!live} onClick=${() => { cluster.command("WHOSPOTSME"); set({ consoleOpen: true }); }}>Run WHOSPOTSME</button></div>
        <p class="muted small">The node answers in text, so the reply appears in the console. Add a band there, for example WHOSPOTSME 20M.</p>
      </section>

      <section class="msgs">
        <h3>Bulletins and announcements</h3>
        ${S.messages.length ? html`<ul>${S.messages.slice(0, 25).map(m => html`<li class=${"k-" + m.kind.toLowerCase()}><span class="mk">${m.kind}</span><div><span class="muted small mono">${fmtTime(m.t)}${m.from ? " de " + m.from : ""}</span><p>${m.text}</p></div></li>`)}</ul>`
          : html`<p class="muted">Nothing yet. WWV, WCY and announcements appear here when your switches allow them.</p>`}
      </section>
    </div>
  </aside>`;
}

// ---------------------------------------------------------------- console dock
function ConsoleDock() {
  const [val, setVal] = useState("");
  const hist = useRef({ list: LS.get("gcui.hist", []), i: -1 });
  const logRef = useRef(null);
  useEffect(() => { if (logRef.current) logRef.current.scrollTop = logRef.current.scrollHeight; }, [S.lines.length, S.consoleOpen]);
  const send = () => {
    const v = val.trim(); if (!v) return;
    cluster.command(v);
    const h = hist.current; h.list = [v, ...h.list.filter(x => x !== v)].slice(0, 50); h.i = -1; LS.set("gcui.hist", h.list);
    setVal(""); if (!S.consoleOpen) set({ consoleOpen: true });
  };
  const quick = ["SHOW DX 10", "SHOW FILTER", "SHOW SETTINGS", "WHOSPOTSME", "LIST PRESET", "SHOW HOLD", "HELP"];
  return html`<section class=${cls("console", S.consoleOpen && "open")} aria-label="Console">
    ${S.consoleOpen ? html`<div class="con-log mono" ref=${logRef} aria-live="polite">${S.lines.map(l => html`<div class=${"ln " + l.dir}><span class="pfx">${l.dir === "out" ? ">" : l.dir === "sys" ? "·" : ""}</span><pre>${l.text}</pre></div>`)}</div>
      <div class="con-quick">${quick.map(q => html`<button type="button" class="chip sm" onClick=${() => { cluster.command(q); }}>${q}</button>`)}</div>` : null}
    <form class="con-in" onSubmit=${e => { e.preventDefault(); send(); }}>
      <span class="mono prompt">${(S.snap.loginCall || "offline") + ">"}</span>
      <input id="cmd" class="mono" autocomplete="off" spellcheck="false" placeholder="Type a cluster command, such as SHOW DX 10 or DX 14025.00 K1ABC" aria-label="Cluster command" value=${val}
        onInput=${e => setVal(e.target.value)}
        onKeyDown=${e => {
          const h = hist.current;
          if (e.key === "ArrowUp" && h.list.length) { e.preventDefault(); h.i = Math.min(h.list.length - 1, h.i + 1); setVal(h.list[h.i]); }
          else if (e.key === "ArrowDown") { e.preventDefault(); h.i = Math.max(-1, h.i - 1); setVal(h.i < 0 ? "" : h.list[h.i]); }
          else if (e.key === "Escape") set({ consoleOpen: false });
        }} />
      ${!S.consoleOpen && S.lines.length ? html`<span class="last mono muted">${S.lines[S.lines.length - 1].text.split("\n")[0].slice(0, 70)}</span>` : null}
      <button type="button" class="icon-btn sm" aria-label=${S.consoleOpen ? "Collapse console" : "Expand console"} onClick=${() => set({ consoleOpen: !S.consoleOpen })}><${Icon} name="chevronDown" size=${16} /></button>
    </form>
  </section>`;
}

// Native modal focus and background isolation also apply when opened from a drawer.
function ModalFrame({ label, labelledBy, onClose, escape = true, children }) {
  const ref = useRef(null);
  useLayoutEffect(() => {
    const dialog = ref.current;
    const previous = document.activeElement;
    dialog.showModal();
    return () => {
      dialog.close();
      if (document.activeElement === document.body && previous?.isConnected) previous.focus({ preventScroll: true });
    };
  }, []);
  return html`<dialog class="modal-back" ref=${ref} aria-label=${label} aria-labelledby=${labelledBy}
    onCancel=${e => { e.preventDefault(); if (escape) onClose(); }}
    onClick=${e => { if (e.target === e.currentTarget) onClose(); }}>${children}</dialog>`;
}

// ---------------------------------------------------------------- post spot dialog
function PostSpotDialog() {
  const sel = S.selSpot;
  const [f, setF] = useState(sel ? fmtFreq(sel.freq) : "");
  const [c, setC] = useState(sel ? sel.dxCall : "");
  const [t, setT] = useState("");
  const [err, setErr] = useState("");
  const close = () => set({ postOpen: false });
  const [busy, setBusy] = useState(false);
  const submit = async e => {
    e.preventDefault();
    setBusy(true);
    try {
      const d = await cluster.postSpot(f, c, t);
      toast(d.queued ? `The node queued your spot of ${c.toUpperCase()}.` : `Sent DX ${c.toUpperCase()}. No reply yet; check the console.`);
      close();
    } catch (x) { setErr(x.message); } finally { setBusy(false); }
  };
  const len = `DX ${f} ${c} ${t}`.trim().length;
  return html`<${ModalFrame} label="Post a spot" onClose=${close}>
    <form class="modal" onSubmit=${submit}>
      <header class="dr-head"><h2>Post a spot</h2><button type="button" class="icon-btn" aria-label="Close" onClick=${close}><${Icon} name="close" /></button></header>
      <div class="modal-body">
        <div class="two">
          <div class="field"><label for="pf">Frequency (kHz)</label><input id="pf" class="txt mono" inputmode="decimal" placeholder="14025.00" value=${f} onInput=${e => { setF(e.target.value); setErr(""); }} /></div>
          <div class="field"><label for="pc">Callsign heard</label><input id="pc" class="txt mono" placeholder="K1ABC" value=${c} onInput=${e => { setC(e.target.value.toUpperCase()); setErr(""); }} /></div>
        </div>
        <div class="field"><label for="pt">Comment <span class="muted">optional</span></label><input id="pt" class="txt" maxlength="60" placeholder="UP 2, loud in FN31" value=${t} onInput=${e => { setT(e.target.value); setErr(""); }} /></div>
        <div class="cmdline"><code>DX ${f || "<kHz>"} ${c || "<call>"}${t ? " " + t : ""}</code><span class=${cls("muted small", len > 128 && "err")}>${len}/128</span></div>
        ${err ? html`<div class="field-err" role="alert">${err}</div>` : S.snap.conn.phase !== "live" ? html`<p class="hint">Connect Live to post a spot.</p>` : html`<p class="hint">Only spot stations you actually heard. Your spot goes to every user of this node and its peers.</p>`}
      </div>
      <footer class="modal-foot"><button type="button" class="btn ghost" onClick=${close}>Cancel</button><button type="submit" class="btn primary" disabled=${!f || !c || busy || S.snap.conn.phase !== "live"}><${Icon} name="send" size=${15} /> ${busy ? "Posting…" : "Post spot"}</button></footer>
    </form>
  <//>`;
}

function Toasts() {
  return html`<div class="toasts" role="status" aria-live="polite">${S.toasts.map(t => html`<div class=${"toast " + t.kind}><${Icon} name=${t.kind === "error" ? "error" : t.kind === "info" ? "info" : "check"} size=${16} /><span>${t.text}</span></div>`)}</div>`;
}

// ---------------------------------------------------------------- history (SH DX pages)
const HIST_COUNTS = [5, 10, 25, 50, 100, 250];
function HistoryView() {
  const h = cluster.history, c = S.snap.conn;
  const live = c.phase === "live";
  const secRef = useRef(null);
  const [w, setW] = useState(1400);
  const [drag, setDrag] = useState(null);
  const [sel, setSel] = useState("");
  const [n, setN] = useState("25");
  useEffect(() => {
    if (!secRef.current || typeof ResizeObserver === "undefined") return;
    const ro = new ResizeObserver(es => setW(es[0].contentRect.width));
    ro.observe(secRef.current);
    return () => ro.disconnect();
  }, []);
  const loading = h.status === "loading" || h.status === "retrying";
  const run = fn => { try { fn(); } catch (x) { toast(x.message, "error"); } };
  const search = e => { e.preventDefault(); run(() => cluster.historySearch({ selector: sel, count: n })); };
  // Each page is chronological; show newest first, older pages below.
  const rows = [];
  for (const p of h.pages) for (let i = p.rows.length - 1; i >= 0; i--) rows.push(p.rows[i]);
  const last = h.pages[h.pages.length - 1];
  const mobile = window.matchMedia && window.matchMedia("(max-width: 760px)").matches;
  const cols = colOrder().filter(k => colVisible(k) && (mobile || !COLUMNS[k].minW || w >= COLUMNS[k].minW));
  const layout = columnLayout(cols);
  const mismatch = h.pages.filter(p => p.countMismatch);
  const status = loading ? (h.note || "Searching the node's archive…")
    : h.status === "failed" || h.status === "interrupted" ? h.note
    : last ? `${plural(rows.length, "spot")}${h.pages.length > 1 ? ` across ${h.pages.length} pages` : ""} · ${CONN.END_WORDS[last.end] || last.end}`
    : live ? "Search the node's archive with your current filters." : c.mode === "replay" ? "The capture's history page appears here when it plays." : "Connect to search the archive.";
  return html`<section ref=${secRef} class=${cls("stream", "hist", S.prefs.density)} style=${layout} aria-label="History">
    <div class="hist-head">
      <form class="hist-form" onSubmit=${search}>
        <label class="sr" for="hsel">Call, prefix or country</label>
        <input id="hsel" class="txt mono" placeholder="Call or prefix (optional)" value=${sel} disabled=${!live} onInput=${e => setSel(e.target.value.toUpperCase())} />
        <label class="sr" for="hn">How many</label>
        <select id="hn" class="txt" value=${n} disabled=${!live} onChange=${e => setN(e.target.value)}>${HIST_COUNTS.map(x => html`<option value=${x}>${x} spots</option>`)}</select>
        <button type="submit" class="btn primary" disabled=${!live || loading}><${Icon} name="history" size=${15} /> ${loading ? "Searching…" : "Search"}</button>
      </form>
      <div class=${cls("hist-status", (h.status === "failed" || h.status === "interrupted") && "bad")} role="status">
        ${loading ? html`<span class="spin" aria-hidden="true"></span>` : null}<span>${status}</span>
        ${h.command && !loading ? html`<code class="muted">${h.command}</code>` : null}
        ${last && last.replay ? html`<span class="badge warn">replay</span>` : null}
        <span class="grow"></span>
        ${h.status === "failed" || h.status === "interrupted" ? html`<button type="button" class="btn sm" disabled=${!live || !h.command} onClick=${() => run(() => cluster.historyCommand(h.command))}>Search again</button>` : null}
      </div>
      ${last && last.unreadable ? html`<div class="lint warn"><${Icon} name="warn" size=${15} /><span>Some archived records could not be read and were skipped by the node.</span></div>` : null}
      ${mismatch.length ? html`<div class="lint warn"><${Icon} name="warn" size=${15} /><span>The node announced ${mismatch.map(p => p.count).join(", ")} row(s) but sent ${mismatch.map(p => p.rows.length).join(", ")}. All received rows are shown.</span></div>` : null}
    </div>
    <div class="spot-scroll"><div class="spot-grid">
    <div class="thead" role="row">${cols.map(k => html`<${ColumnHead} key=${k} k=${k} drag=${drag} setDrag=${setDrag} />`)}</div>
    <div class="tbody" role="rowgroup">
      ${rows.map(s => html`<${SpotRow} key=${s.id} cols=${cols} r=${{ s, pass: true, by: "" }} sel=${S.selSpot && S.selSpot.id === s.id} fresh=${false} />`)}
      ${!rows.length && !loading ? html`<div class="empty"><${Icon} name="history" size=${28} /><b>${last ? "No matching spots" : "No history loaded"}</b><span>${last ? "Try a different call or a larger count." : status}</span></div>` : null}
    </div></div></div>
    <footer class="tfoot muted small"><span>Newest first. Rows come from SH DX with your current filters; history rows carry no path rating.</span><span class="grow"></span>
      ${last && last.token ? html`<button type="button" class="btn sm" disabled=${!live || loading} onClick=${() => run(() => cluster.historyOlder())}><${Icon} name="chevronDown" size=${14} /> Older spots</button>` : null}</footer>
  </section>`;
}

// ---------------------------------------------------------------- connection banner
function ConnectionBanner() {
  const c = S.snap.conn;
  const [, tick] = useState(0);
  useEffect(() => { if (!c.retryAt) return; const id = setInterval(() => tick(x => x + 1), 1000); return () => clearInterval(id); }, [c.retryAt]);
  const openPanel = () => set({ connectOpen: true });
  let kind = "info", body = null, acts = [];
  if (c.mode === "replay") {
    kind = "replay";
    body = html`<b>Replay</b> of <span class="mono">${c.replay ? c.replay.name : "a capture"}</span>: recorded data, not live.${c.replay ? ` ${c.replay.done} of ${c.replay.total} records${c.phase === "replay-done" ? ", finished" : ""}.` : ""}`;
    acts = [["Connect Live", openPanel], ["Stop replay", () => cluster.disconnect()]];
  } else if (c.accessExpired) {
    kind = "error";
    body = html`Your Cloudflare sign-in has expired. <a href=${location.origin + "/"} target="_top" rel="noopener">Reopen the pilot page</a> to sign in again, then connect.`;
    acts = [["Retry", () => cluster.retryNow()]];
  } else if (c.mode === "live" && c.phase === "failed") {
    kind = "error";
    body = html`${(c.error && c.error.message) || "The connection failed."}`;
    acts = [["Change login", openPanel], ["Retry", () => cluster.retryNow()]];
  } else if (c.mode === "live" && c.phase === "disconnected") {
    kind = "warn";
    const secs = c.retryAt ? Math.max(0, Math.ceil((c.retryAt - Date.now()) / 1000)) : 0;
    body = html`<b>Disconnected.</b> ${c.stale ? "The spots and settings shown are from before the drop. " : ""}${c.retryAt ? `Reconnecting in ${secs} s (attempt ${c.attempt} of ${c.maxAttempts}).` : c.error && c.error.code === "gave-up" ? "Automatic retries have stopped." : ""}`;
    acts = [["Retry now", () => cluster.retryNow()]].concat(c.retryAt ? [["Stop", () => cluster.disconnect()]] : []);
  } else if (c.mode === "live" && ["connecting", "login", "negotiating"].includes(c.phase)) {
    body = html`${c.stale ? "Reconnecting. The data shown is stale until the node confirms the new session." : "Connecting to the cluster…"}`;
  } else if (c.phase === "live" && !c.configLoaded) {
    kind = c.cfgError ? "warn" : "info";
    body = html`${c.cfgError ? `Could not load your profile: ${c.cfgError}` : "Loading your profile from the node…"}`;
    if (c.cfgError) acts = [["Try again", () => cluster.refreshConfig().catch(() => {})]];
  } else if (c.phase === "live" && c.activeDialect && c.activeDialect !== "go") {
    kind = "warn";
    body = html`This login uses the ${c.activeDialect.toUpperCase()} dialect. Editing rules here needs GO; your CC filters are kept.`;
    acts = [["Switch to GO", () => cluster.switchToGo().then(() => toast("Switched to GO."), e => toast(e.message, "error"))]];
  } else if (c.needsReview) {
    kind = "warn";
    body = html`A change was not confirmed before the connection dropped. Nothing was resent. Your profile has been reloaded from the node; check it before trying again.`;
    acts = [["Dismiss", () => { cluster.needsReview = false; cluster.changed(); }]];
  } else return null;
  return html`<div class=${"conn-banner " + kind} role=${kind === "error" ? "alert" : "status"}>
    <${Icon} name=${kind === "error" ? "error" : kind === "warn" ? "warn" : kind === "replay" ? "play" : "info"} size=${16} />
    <span class="cb-text">${body}</span>
    ${acts.map(([l, fn]) => html`<button type="button" class="btn sm" onClick=${fn}>${l}</button>`)}
  </div>`;
}

// ---------------------------------------------------------------- connect panel
const CONNECT_STEPS = [["connecting", "Open the secure connection"], ["login", "Log in with your callsign"], ["negotiating", "Switch to JSON (SET JSON ON VERSION 1)"], ["profile", "Load your profile"]];
const ERROR_HELP = {
  evicted: call => `Another login with ${call} replaced this one, usually your logger. Use an SSID that no other program uses.`,
  "login-refused": call => `The cluster did not accept ${call}.`,
  "json-refused": () => "The cluster refused JSON mode.",
  version: () => "This cluster speaks a different JSON version.",
  access: () => "Your Cloudflare sign-in has expired.",
  "gave-up": () => "The pilot's WebSocket bridge may not be running yet.",
};
function ConnectPanel() {
  const c = S.snap.conn;
  const [call, setCall] = useState(S.snap.loginCall || "");
  const [warnSsid, setWarnSsid] = useState(false);
  const [err, setErr] = useState("");
  const [busy, setBusy] = useState(false);
  const started = useRef(false);
  const valid = /^[A-Z0-9]{1,3}[0-9][A-Z0-9]{0,4}[A-Z](\/[A-Z0-9]+)?(-\d{1,2})?$/.test(call) || /^[A-Z0-9/]{3,}-\d{1,2}$/.test(call);
  const hasSsid = /-\d{1,2}$/.test(call);
  const connecting = c.mode === "live" && ["connecting", "login", "negotiating"].includes(c.phase);
  useEffect(() => { if (started.current && c.phase === "live" && c.configLoaded) { started.current = false; set({ connectOpen: false }); toast(`Live as ${S.snap.loginCall}.`); } }, [c.phase, c.configLoaded]);
  const close = () => set({ connectOpen: false });
  const connect = force => {
    setErr("");
    if (!valid) { setErr("Enter a callsign with its SSID, for example N2WQ-9."); return; }
    if (!hasSsid && !force) { setWarnSsid(true); return; }
    setWarnSsid(false);
    try { started.current = true; cluster.connectLive(call); } catch (x) { setErr(x.message); }
  };
  const replay = async () => { setErr(""); setBusy(true); try { await cluster.startReplayFromUrl(); set({ connectOpen: false, view: "stream" }); } catch (x) { setErr(x.message); } finally { setBusy(false); } };
  const replayFile = async e => {
    const f = e.target.files && e.target.files[0];
    e.target.value = "";
    if (!f) return;
    setErr("");
    try { if (f.size > 8 << 20) throw new Error("That file is over 8 MB."); cluster.startReplay(await f.text(), f.name); set({ connectOpen: false, view: "stream" }); }
    catch (x) { setErr(x.message); }
  };
  const order = ["connecting", "login", "negotiating", "live"];
  const at = c.mode === "live" ? order.indexOf(c.phase) : -1;
  const failed = c.mode === "live" && (c.phase === "failed" || c.phase === "disconnected") && c.error;
  const stepState = i => {
    if (c.mode !== "live" || (at < 0 && !failed)) return "todo";
    if (at === 3) return i < 3 || c.configLoaded ? "done" : "now";
    if (at >= 0) return i < at ? "done" : i === at ? "now" : "todo";
    return "todo";
  };
  const code = c.error && c.error.code;
  return html`<${ModalFrame} labelledBy="cp-title" onClose=${close} escape=${c.mode !== "off"}>
    <div class="modal connect">
      <header class="dr-head"><div><h2 id="cp-title">Connect to GoCluster</h2><div class="muted small">Use the same callsign and SSID as your logger to share its profile.</div></div>
        <button type="button" class="icon-btn" aria-label="Close" onClick=${close}><${Icon} name="close" /></button></header>
      <div class="modal-body">
        <form class="field" onSubmit=${e => { e.preventDefault(); connect(false); }}>
          <label for="cp-call">Callsign with SSID</label>
          <div class="inline-form"><input id="cp-call" class="txt mono" autocomplete="off" autocapitalize="characters" spellcheck="false" placeholder="N2WQ-9" value=${call} onInput=${e => { setCall(e.target.value.toUpperCase().replace(/\s/g, "")); setErr(""); setWarnSsid(false); }} />
            <button type="submit" class="btn primary" disabled=${!call || connecting}>${connecting ? "Connecting…" : c.phase === "live" && call === S.snap.loginCall ? "Reconnect" : "Connect"}</button></div>
          <p class="hint">Use an SSID your logger does not use, such as ${(call.replace(/-\d{1,2}$/, "") || "N2WQ")}-9. The cluster allows one login per SSID, so reusing your logger's SSID disconnects your logger.</p>
        </form>
        ${warnSsid ? html`<div class="lint warn"><${Icon} name="warn" size=${15} /><span>${call} has no SSID. If your logger logs in as ${call}, connecting here will disconnect it.</span><button type="button" class="link sm" onClick=${() => connect(true)}>Connect anyway</button></div>` : null}
        ${err ? html`<div class="field-err" role="alert">${err}</div>` : null}
        ${c.mode === "live" ? html`<ol class="steps">${CONNECT_STEPS.map(([k, label], i) => {
          const st = stepState(i);
          return html`<li class=${"st-" + st}><span class="st-ic">${st === "done" ? html`<${Icon} name="check" size=${13} stroke=${2.6} />` : st === "now" ? html`<span class="spin"></span>` : null}</span>${label}</li>`;
        })}</ol>` : null}
        ${failed || (c.mode === "live" && c.error) ? html`<div class="lint error" role="alert"><${Icon} name="error" size=${15} /><span>${ERROR_HELP[code] ? ERROR_HELP[code](S.snap.loginCall) + " " : ""}${c.error.message}${c.retryAt ? " Retrying automatically." : ""}${code === "access" ? html` <a href=${location.origin + "/"} target="_top" rel="noopener">Reopen the pilot page</a>.` : null}</span></div>` : null}
        ${c.phase === "live" ? html`<div class="btn-row"><span class="badge ok">Live</span><span class="muted small">as ${S.snap.loginCall} on ${S.snap.nodeCall}</span><span class="grow"></span><button type="button" class="btn ghost sm" onClick=${() => cluster.disconnect()}>Disconnect</button></div>` : null}
        <section class="cp-replay">
          <h3>Replay a capture</h3>
          <p class="hint">Plays recorded JSON through the same decoder and views. Nothing is sent, and the console labels it as replay.</p>
          <div class="btn-row">
            <button type="button" class="btn" disabled=${busy} onClick=${replay}><${Icon} name="play" size=${14} /> Replay the pilot capture</button>
            <label class="btn ghost file-btn"><${Icon} name="download" size=${14} /> Choose a .jsonl file<input type="file" accept=".jsonl,.json,.txt,application/json,text/plain" onChange=${replayFile} /></label>
          </div>
        </section>
      </div>
      <footer class="modal-foot cp-foot"><span class="muted small mono" title="Set by the gocluster-socket-path deployment setting">Live socket: ${c.socketUrl}</span></footer>
    </div>
  <//>`;
}

// ---------------------------------------------------------------- root
function useVisibleViewport() {
  useLayoutEffect(() => {
    const viewport = window.visualViewport;
    if (!viewport) return;
    const style = document.documentElement.style;
    let frame = 0;
    let revealFocus = false;
    const update = () => {
      frame = 0;
      const resized = revealFocus;
      revealFocus = false;
      // At pinch zoom, let the browser pan the page normally.
      if (viewport.scale !== 1) {
        style.removeProperty("--visible-top");
        style.removeProperty("--visible-height");
        return;
      }
      style.setProperty("--visible-top", viewport.offsetTop + "px");
      style.setProperty("--visible-height", viewport.height + "px");
      // Reveal focus after a resize and let operators scroll freely afterward.
      if (!resized) return;
      const focused = document.activeElement;
      const overlay = focused?.closest(".modal-back, .drawer[aria-modal=true]");
      if (overlay) {
        const panel = overlay.getBoundingClientRect(), field = focused.getBoundingClientRect();
        if (field.bottom > panel.bottom - 8) overlay.scrollTop += field.bottom - panel.bottom + 8;
        else if (field.top < panel.top + 8) overlay.scrollTop -= panel.top + 8 - field.top;
      }
    };
    const schedule = event => {
      if (event.type === "resize") revealFocus = true;
      if (!frame) frame = requestAnimationFrame(update);
    };
    update();
    viewport.addEventListener("resize", schedule);
    viewport.addEventListener("scroll", schedule);
    return () => {
      viewport.removeEventListener("resize", schedule);
      viewport.removeEventListener("scroll", schedule);
      cancelAnimationFrame(frame);
      style.removeProperty("--visible-top");
      style.removeProperty("--visible-height");
    };
  }, []);
}

function useDrawerOverlay(name) {
  const query = "(max-width: 1180px), (max-height: 680px), (pointer: coarse)";
  const [overlay, setOverlay] = useState(() => window.matchMedia(query).matches);
  const opener = useRef(null);
  useLayoutEffect(() => {
    const media = window.matchMedia(query);
    const update = () => setOverlay(media.matches);
    media.addEventListener("change", update);
    return () => media.removeEventListener("change", update);
  }, []);
  useLayoutEffect(() => {
    const focused = document.activeElement;
    if (name) {
      if (focused !== document.body && !focused?.closest(".drawer")) opener.current = focused;
    } else {
      if (focused === document.body && opener.current?.isConnected && !S.connectOpen && !S.postOpen) {
        opener.current.focus({ preventScroll: true });
      }
      opener.current = null;
    }
  }, [name]);
  useLayoutEffect(() => {
    if (!name || !overlay) return;
    const drawer = document.querySelector(".workspace > .drawer");
    if (!drawer) return;
    const background = document.querySelectorAll(".app > :is(.topbar, .subbar, .console), .workspace > .main");
    drawer.setAttribute("aria-modal", "true");
    for (const region of background) region.inert = true;
    // Keep an in-progress edit focused when a docked drawer becomes an overlay.
    const focused = document.activeElement;
    if (drawer.contains(focused)) focused.scrollIntoView({ block: "nearest", inline: "nearest" });
    else if (!S.connectOpen && !S.postOpen) drawer.querySelector(".dr-head button")?.focus();
    return () => {
      for (const region of background) region.inert = false;
      drawer.removeAttribute("aria-modal");
      const focused = document.activeElement;
      if (drawer.contains(focused)) focused.scrollIntoView({ block: "nearest", inline: "nearest" });
    };
  }, [name, overlay]);
}

function App() {
  useStore();
  useVisibleViewport();
  useDrawerOverlay(S.drawer);
  const profile = stripProfile();
  const bands = stripBands(profile.set);
  const selectedBand = S.focusBand;
  const focusBand = bands.includes(selectedBand) ? selectedBand : "ALL";
  useEffect(() => {
    if (focusBand !== selectedBand && S.focusBand === selectedBand) set({ focusBand });
  }, [focusBand, selectedBand]);
  const left = S.drawer === "filters";
  const right = ["settings", "activity", "spot"].includes(S.drawer);
  return html`<div class=${cls("app", left && "has-left", right && "has-right", S.consoleOpen && "con-open")}>
    <${TopBar} />
    <${SubBar} profile=${profile} bands=${bands} focusBand=${focusBand} />
    <div class="workspace">
      ${left ? html`<${FiltersDrawer} />` : null}
      <main class="main" id="main">
        <${ConnectionBanner} />
        ${S.view === "bandmap" ? html`<${BandMapView} focusBand=${focusBand} />` : S.view === "openings" ? html`<${OpeningsView} bands=${bands} />` : S.view === "history" ? html`<${HistoryView} />` : html`<${StreamView} focusBand=${focusBand} />`}
      </main>
      ${S.drawer === "settings" ? html`<${SettingsDrawer} />` : S.drawer === "activity" ? html`<${ActivityDrawer} />` : S.drawer === "spot" ? html`<${SpotDrawer} key=${S.selSpot && S.selSpot.id} />` : null}
      ${(left || right) ? html`<button type="button" class="scrim" tabindex="-1" aria-hidden="true" onClick=${() => set({ drawer: null })}></button>` : null}
    </div>
    <${ConsoleDock} />
    ${S.postOpen ? html`<${PostSpotDialog} />` : null}
    ${S.connectOpen ? html`<${ConnectPanel} />` : null}
    <${Toasts} />
  </div>`;
}

// ---------------------------------------------------------------- boot
function routeFromHash() {
  const h = location.hash.replace("#", "");
  if (["stream", "history", "bandmap", "openings"].includes(h)) set({ view: h });
}
window.addEventListener("hashchange", routeFromHash);
window.addEventListener("keydown", e => {
  const tag = (e.target.tagName || "").toLowerCase();
  const typing = tag === "input" || tag === "textarea" || tag === "select";
  if (S.connectOpen || S.postOpen) return; // The native dialog owns Escape and focus.
  if (e.key === "Escape") { if (S.drawer) set({ drawer: null }); return; }
  if (document.querySelector(".drawer[aria-modal=true]")) return;
  if (typing || e.metaKey || e.ctrlKey || e.altKey) return;
  if (e.key === "/") { e.preventDefault(); const q = document.getElementById("q"); if (q) q.focus(); }
  else if (e.key === "f" || e.key === "F") set({ drawer: S.drawer === "filters" ? null : "filters" });
  else if (e.key === "s" || e.key === "S") set({ drawer: S.drawer === "settings" ? null : "settings" });
  else if (e.key === "`") { set({ consoleOpen: true }); setTimeout(() => { const c = document.getElementById("cmd"); if (c) c.focus(); }, 30); }
});
try { window.matchMedia("(prefers-color-scheme: dark)").addEventListener("change", applyTheme); window.matchMedia("(prefers-contrast: more)").addEventListener("change", applyTheme); } catch (e) { /* old browsers */ }
new MutationObserver(m => { if (m.some(x => x.attributeName === "data-theme")) applyTheme(); }).observe(document.documentElement, { attributes: true });
setInterval(() => notify(), 1000);

applyTheme();
routeFromHash();
S.snap = cluster.snapshot();
S.connectOpen = true;
render(html`<${App} />`, document.getElementById("root"));
applyTheme();

// The network switcher both pages share. It owns which bigname API the site
// talks to: every API request the pages make goes through apiBase(), never
// the page's own origin, because the site is hosted apart from the API.
//
// Selection lives in the URL so a link says which network it means:
//   ?network=sepolia|mainnet   pick a network from the table below
//   ?api=http://127.0.0.1:3000 point at any API instead (local development);
//                              it wins over the table and shows as "custom"
// Other query parameters are left alone. Links marked data-carry get the
// selection merged into their own query when moving between the two pages.
//
// The first half of this file is plain logic with no DOM, exported for the
// behavioural tests in scripts/tests/site.test.mjs. The second half wires it
// to the page and only runs in a browser.
(function (root) {
  'use strict';

  // A network with coming: true is listed but not selectable until its API exists.
  const NETWORKS = [
    { id: 'sepolia', label: 'sepolia', api: 'https://sepolia.api.bigname.sh' },
    { id: 'mainnet', label: 'mainnet', api: 'https://api.bigname.sh', coming: true },
  ];
  const DEFAULT_NETWORK = 'sepolia';
  const CHAIN_NAMES = { '1': 'ethereum', '11155111': 'sepolia', '8453': 'base', '84532': 'base sepolia' };
  const STATUS_EVERY_MS = 30000;
  // A status request that has not answered by then is abandoned, so a host that
  // accepts the connection and never replies cannot hold the single slot.
  const STATUS_TIMEOUT_MS = 10000;

  const esc = s => String(s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const selectable = id => NETWORKS.find(n => n.id === id && !n.coming) || null;
  const defaultNetwork = () => selectable(DEFAULT_NETWORK);

  // An override must be an absolute http(s) URL; anything else is ignored. A
  // value with a quote, backslash, whitespace, semicolon, angle bracket, `$`
  // or parenthesis is rejected outright rather than cleaned, since the pages
  // print the base into shell commands and markup; no real API base needs them.
  function parseApi(raw) {
    if (!raw || /['"`\\\s;<>$()]/.test(raw)) return null;
    try {
      const u = new URL(raw);
      if (u.protocol !== 'http:' && u.protocol !== 'https:') return null;
      return (u.origin + u.pathname).replace(/\/+$/, '');
    } catch (e) { return null; }
  }

  // The selection a query string asks for. `rewrite` is true when the address
  // bar should change to say what was actually chosen: an unknown or coming
  // ?network= falls back to the default, and a rejected ?api= is dropped.
  function initialSelection(search) {
    const q = new URLSearchParams(search);
    const custom = parseApi(q.get('api'));
    const network = selectable(q.get('network')) || defaultNetwork();
    const rewrite = (q.has('network') && q.get('network') !== network.id) || (q.has('api') && !custom);
    return { network, custom, rewrite };
  }

  function applySelection(params, network, custom) {
    params.set('network', network.id);
    if (custom) params.set('api', custom); else params.delete('api');
    return params;
  }
  // The page's query with the selection written into it; other parameters survive.
  function buildQuery(search, network, custom) {
    const s = applySelection(new URLSearchParams(search), network, custom).toString();
    return s ? '?' + s : '';
  }
  // A page-relative link with the selection merged into its own query,
  // keeping its #fragment. Carrying an already carried link changes nothing.
  function carryHref(href, network, custom) {
    const h = href.indexOf('#');
    const hash = h < 0 ? '' : href.slice(h), rest = h < 0 ? href : href.slice(0, h);
    const q = rest.indexOf('?');
    const path = q < 0 ? rest : rest.slice(0, q), search = q < 0 ? '' : rest.slice(q + 1);
    return path + buildQuery(search, network, custom) + hash;
  }

  const hostOf = base => { try { return new URL(base).host; } catch (e) { return base; } };
  const chainName = id => CHAIN_NAMES[id] || `chain ${id}`;
  const fmtLag = s => s < 90 ? `${Math.round(s)}s` : s < 5400 ? `${Math.round(s / 60)}m` : `${(s / 3600).toFixed(1)}h`;
  const fmtBlock = n => typeof n === 'number' ? n.toLocaleString('en-US') : '?';

  // The switcher's buttons.
  function controlHtml(network, custom) {
    const opts = NETWORKS.map(n => {
      const on = !custom && n === network;
      return n.coming
        ? `<button type="button" disabled title="${esc(n.label)} is coming; its API is not live yet">${esc(n.label)}<small>coming</small></button>`
        : `<button type="button" data-net="${esc(n.id)}" aria-pressed="${on}" title="${esc(n.label)}: ${esc(n.api)}">${esc(n.label)}</button>`;
    });
    if (custom) opts.push(`<button type="button" data-net="custom" aria-pressed="true" title="custom: ${esc(custom)}">custom</button>`);
    return opts.join('');
  }
  // The detail line: which API answered and each chain's status and head block.
  function statusDetailHtml(data, base, label) {
    const api = `<span title="${esc(base)}">api ${esc(hostOf(base))}</span>`;
    if (!data) return `${api} · ${esc(label)} unreachable`;
    const chains = Object.entries(data.chains || {}).map(([id, c]) =>
      `<span title="indexed ${esc(fmtBlock(c.indexed_block))}, head ${esc(fmtBlock(c.latest_block))}">${esc(chainName(id))} ${esc(c.status)} at ${esc(fmtBlock(c.latest_block))}</span>`);
    return [api, ...chains].join(' · ');
  }

  // Status of the selected API. At most one request is out at a time: a timer
  // tick while one is pending does nothing. A request is aborted after
  // timeoutMs and reads as unreachable, which frees the slot for the next tick.
  // A selection change aborts the pending request, forgets the previous
  // network's answer, and asks the new one. Only the newest request may
  // publish, and an old request finishing never clears the newer one's
  // in-flight mark.
  function createStatus({ fetchImpl, getBase, AbortCtl, setTimer, clearTimer, timeoutMs }) {
    const Ctl = AbortCtl || root.AbortController;
    const after = setTimer || ((fn, ms) => root.setTimeout(fn, ms));
    const cancel = clearTimer || (t => root.clearTimeout(t));
    const limit = timeoutMs || STATUS_TIMEOUT_MS;
    let gen = 0, active = null;
    let cache = { pending: true, base: getBase(), data: null };
    const listeners = [];
    function start() {
      const g = ++gen, base = getBase(), controller = new Ctl(), req = { controller };
      active = req;
      const timer = after(() => controller.abort(), limit);
      return Promise.resolve()
        .then(() => fetchImpl(base + '/v1/status', { headers: { accept: 'application/json' }, signal: controller.signal }))
        .then(r => (r && r.ok ? r.json().then(j => (j && j.data && j.data.status ? j.data : null)) : null))
        .catch(() => null)
        .then(data => {
          if (g !== gen) return;
          cache = { pending: false, base, data };
          for (const cb of listeners) cb(data, base);
        })
        .finally(() => { cancel(timer); if (active === req) active = null; });
    }
    return {
      tick() { return active ? null : start(); },
      restart() {
        if (active) active.controller.abort();
        active = null;
        cache = { pending: true, base: getBase(), data: null };
        return start();
      },
      subscribe(cb) { listeners.push(cb); if (!cache.pending) cb(cache.data, cache.base); },
      get inFlight() { return active !== null; },
      get cache() { return cache; },
    };
  }

  const core = {
    NETWORKS, DEFAULT_NETWORK, STATUS_TIMEOUT_MS, esc, selectable, parseApi, initialSelection, buildQuery, carryHref,
    hostOf, controlHtml, statusDetailHtml, createStatus,
  };
  if (typeof module !== 'undefined' && module.exports) module.exports = core;
  if (typeof document === 'undefined' || typeof location === 'undefined') return;

  // ---- in the page ------------------------------------------------------
  const initial = initialSelection(location.search);
  let custom = initial.custom, network = initial.network;
  const apiBase = () => custom || network.api;
  const apiHost = () => hostOf(apiBase());
  const label = () => custom ? 'custom' : network.label;
  const labelFor = base => base === custom ? 'custom' : (NETWORKS.find(n => n.api === base) || network).label;
  const query = () => buildQuery(location.search, network, custom);
  const carry = href => carryHref(href, network, custom);
  const writeUrl = () => { try { history.replaceState(history.state, '', location.pathname + query() + location.hash); } catch (e) {} };
  if (initial.rewrite) writeUrl();

  function refreshLinks(scope) {
    for (const a of (scope || document).querySelectorAll('a[data-carry]')) {
      const href = carry(a.getAttribute('data-carry'));
      if (a.getAttribute('href') !== href) a.setAttribute('href', href);
    }
  }

  const changeHandlers = [];
  const status = createStatus({ fetchImpl: root.fetch.bind(root), getBase: apiBase });
  function select(id) {
    const next = id === 'custom' ? network : selectable(id);
    if (!next || (next === network && (id === 'custom') === !!custom)) return;
    if (id !== 'custom') custom = null;
    network = next;
    writeUrl();
    refreshLinks();
    renderControls();
    for (const cb of changeHandlers) cb();
    status.restart();
  }

  // ---- the control ---------------------------------------------------
  const mounts = [];
  const STYLE = `
    .bn-net { display: inline-flex; align-items: center; gap: 2px; font-family: var(--mono); font-size: 12.5px; }
    .bn-net button { border: 0; background: none; padding: 2px 7px; border-radius: 6px; cursor: pointer; color: var(--ink-soft); }
    .bn-net button[aria-pressed="true"] { background: var(--wash); color: var(--ink); }
    .bn-net button:disabled { cursor: default; color: var(--ink-faint); }
    .bn-net button:disabled small { font-size: 10.5px; margin-left: 4px; }
    .bn-net button:not(:disabled):hover { color: var(--ink); }
    @media (max-width: 560px) { .bn-net button:disabled { display: none; } .bn-net button { padding: 2px 5px; } }
  `;
  function renderControls() { for (const el of mounts) el.innerHTML = controlHtml(network, custom); }
  function mount(el) {
    if (!el) return;
    if (!document.getElementById('bn-net-style')) {
      const s = document.createElement('style');
      s.id = 'bn-net-style'; s.textContent = STYLE;
      document.head.appendChild(s);
    }
    el.classList.add('bn-net');
    el.setAttribute('role', 'group');
    el.setAttribute('aria-label', 'network');
    el.addEventListener('click', e => { const b = e.target.closest('button[data-net]'); if (b) select(b.dataset.net); });
    mounts.push(el);
    renderControls();
  }

  // ---- status of the selected network ----------------------------------
  // The header pill: overall readiness, with every chain in its tooltip.
  function statusPill(el) {
    if (!el) return;
    const t = el.querySelector('.t');
    changeHandlers.push(() => { el.className = 'status'; t.textContent = 'checking'; el.title = `checking ${apiHost()}`; });
    status.subscribe((d, base) => {
      if (!d) { el.className = 'status bad'; t.textContent = `${labelFor(base)} unreachable`; el.title = `GET ${base}/v1/status did not answer`; return; }
      const chains = Object.entries(d.chains || {});
      const lag = chains.map(([, c]) => c.lag_seconds).filter(x => typeof x === 'number');
      el.className = 'status ' + (d.status === 'ready' ? 'ok' : d.status === 'stale' ? 'bad' : '');
      t.textContent = `${labelFor(base)} ${d.status}${lag.length ? `, ${fmtLag(Math.max(...lag))} behind` : ''}`;
      el.title = chains.map(([id, c]) => `${chainName(id)}: ${c.status}, indexed ${fmtBlock(c.indexed_block)} of ${fmtBlock(c.latest_block)}`).join('\n');
    });
  }
  function statusDetail(el) {
    if (!el) return;
    const pending = () => { el.innerHTML = `<span>api ${esc(apiHost())}</span>`; };
    pending();
    changeHandlers.push(pending);
    status.subscribe((d, base) => { el.innerHTML = statusDetailHtml(d, base, labelFor(base)); });
  }

  root.BignameNetwork = {
    NETWORKS, apiBase, apiHost, label, query, carry, refreshLinks, mount, statusPill, statusDetail,
    onChange: cb => { changeHandlers.push(cb); },
  };

  // Links rendered later (cards, manual pages) are picked up as they appear.
  const start = () => {
    refreshLinks();
    new MutationObserver(() => refreshLinks()).observe(document.body, { childList: true, subtree: true });
    status.tick();
    setInterval(() => status.tick(), STATUS_EVERY_MS);
  };
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', start); else start();
})(typeof globalThis !== 'undefined' ? globalThis : this);

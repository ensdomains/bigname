// The network switcher both pages share. It owns which bigname API the site
// talks to: every request the pages make goes through apiBase(), never the
// page's own origin, because the site is hosted apart from the API.
//
// Selection lives in the URL so a link says which network it means:
//   ?network=sepolia|mainnet   pick a network from the table below
//   ?api=http://127.0.0.1:3000 point at any API instead (local development);
//                              it wins over the table and shows as "custom"
// Links marked data-carry keep the query when moving between the two pages.
(function () {
  'use strict';

  // A network with coming: true is listed but not selectable until its API exists.
  const NETWORKS = [
    { id: 'sepolia', label: 'sepolia', api: 'https://sepolia.api.bigname.sh' },
    { id: 'mainnet', label: 'mainnet', api: 'https://api.bigname.sh', coming: true },
  ];
  const DEFAULT_NETWORK = 'sepolia';
  const CHAIN_NAMES = { '1': 'ethereum', '11155111': 'sepolia', '8453': 'base', '84532': 'base sepolia' };
  const STATUS_EVERY_MS = 30000;

  const esc = s => String(s).replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
  const selectable = id => NETWORKS.find(n => n.id === id && !n.coming) || null;
  // An override must be an absolute http(s) URL; anything else is ignored.
  function parseApi(raw) {
    if (!raw) return null;
    try {
      const u = new URL(raw);
      if (u.protocol !== 'http:' && u.protocol !== 'https:') return null;
      return (u.origin + u.pathname).replace(/\/+$/, '');
    } catch (e) { return null; }
  }

  const initial = new URLSearchParams(location.search);
  let custom = parseApi(initial.get('api'));
  let network = selectable(initial.get('network')) || selectable(DEFAULT_NETWORK);

  const apiBase = () => custom || network.api;
  const apiHost = () => { try { return new URL(apiBase()).host; } catch (e) { return apiBase(); } };
  const label = () => custom ? 'custom' : network.label;
  function query() {
    const q = new URLSearchParams();
    q.set('network', network.id);
    if (custom) q.set('api', custom);
    return '?' + q.toString();
  }
  // Add the current query to a page-relative link, keeping its #fragment.
  function carry(href) {
    const i = href.indexOf('#');
    const path = i < 0 ? href : href.slice(0, i), hash = i < 0 ? '' : href.slice(i);
    return path.split('?')[0] + query() + hash;
  }
  function refreshLinks(root) {
    for (const a of (root || document).querySelectorAll('a[data-carry]')) {
      const href = carry(a.getAttribute('data-carry'));
      if (a.getAttribute('href') !== href) a.setAttribute('href', href);
    }
  }

  const changeHandlers = [], statusHandlers = [];
  let lastStatus = { data: null, base: null, pending: true };
  function select(id) {
    const next = id === 'custom' ? network : selectable(id);
    if (!next || (next === network && (id === 'custom') === !!custom)) return;
    if (id !== 'custom') custom = null;
    network = next;
    try { history.replaceState(history.state, '', location.pathname + query() + location.hash); } catch (e) {}
    refreshLinks();
    renderControls();
    for (const cb of changeHandlers) cb();
    loadStatus();
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
  function renderControls() {
    const opts = NETWORKS.map(n => {
      const on = !custom && n === network;
      return n.coming
        ? `<button type="button" disabled title="${esc(n.label)} is coming; its API is not live yet">${esc(n.label)}<small>coming</small></button>`
        : `<button type="button" data-net="${esc(n.id)}" aria-pressed="${on}" title="${esc(n.label)}: ${esc(n.api)}">${esc(n.label)}</button>`;
    });
    if (custom) opts.push(`<button type="button" data-net="custom" aria-pressed="true" title="custom: ${esc(custom)}">custom</button>`);
    for (const el of mounts) el.innerHTML = opts.join('');
  }
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
  const chainName = id => CHAIN_NAMES[id] || `chain ${id}`;
  const fmtLag = s => s < 90 ? `${Math.round(s)}s` : s < 5400 ? `${Math.round(s / 60)}m` : `${(s / 3600).toFixed(1)}h`;
  const fmtBlock = n => typeof n === 'number' ? n.toLocaleString('en-US') : '?';
  let statusGen = 0;
  async function loadStatus() {
    const gen = ++statusGen, base = apiBase();
    let data = null;
    try {
      const r = await fetch(base + '/v1/status', { headers: { accept: 'application/json' } });
      if (r.ok) { const j = await r.json(); data = j && j.data && j.data.status ? j.data : null; }
    } catch (e) {}
    if (gen !== statusGen) return;
    lastStatus = { data, base, pending: false };
    for (const cb of statusHandlers) cb(data, base);
  }
  function onStatus(cb) { statusHandlers.push(cb); if (!lastStatus.pending) cb(lastStatus.data, lastStatus.base); }
  // The header pill: overall readiness, with every chain in its tooltip.
  function statusPill(el) {
    if (!el) return;
    const t = el.querySelector('.t');
    changeHandlers.push(() => { el.className = 'status'; t.textContent = 'checking'; el.title = `checking ${apiHost()}`; });
    onStatus(d => {
      if (!d) { el.className = 'status bad'; t.textContent = `${label()} unreachable`; el.title = `GET ${apiBase()}/v1/status did not answer`; return; }
      const chains = Object.entries(d.chains || {});
      const lag = chains.map(([, c]) => c.lag_seconds).filter(x => typeof x === 'number');
      el.className = 'status ' + (d.status === 'ready' ? 'ok' : d.status === 'stale' ? 'bad' : '');
      t.textContent = `${label()} ${d.status}${lag.length ? `, ${fmtLag(Math.max(...lag))} behind` : ''}`;
      el.title = chains.map(([id, c]) => `${chainName(id)}: ${c.status}, indexed ${fmtBlock(c.indexed_block)} of ${fmtBlock(c.latest_block)}`).join('\n');
    });
  }
  // The detail line: which API the page is talking to and each chain's head.
  function statusDetail(el) {
    if (!el) return;
    const show = d => {
      const api = `<span title="${esc(apiBase())}">api ${esc(apiHost())}</span>`;
      if (!d) { el.innerHTML = `${api} · ${esc(label())} unreachable`; return; }
      const chains = Object.entries(d.chains || {}).map(([id, c]) =>
        `<span title="indexed ${esc(fmtBlock(c.indexed_block))}, head ${esc(fmtBlock(c.latest_block))}">${esc(chainName(id))} ${esc(c.status)} at ${esc(fmtBlock(c.latest_block))}</span>`);
      el.innerHTML = [api, ...chains].join(' · ');
    };
    el.innerHTML = `<span>api ${esc(apiHost())}</span>`;
    onStatus(show);
    changeHandlers.push(() => { el.innerHTML = `<span>api ${esc(apiHost())}</span>`; });
  }

  window.BignameNetwork = {
    NETWORKS, apiBase, apiHost, label, query, carry, refreshLinks, mount, statusPill, statusDetail, onStatus,
    onChange: cb => { changeHandlers.push(cb); },
  };

  // Links rendered later (cards, manual pages) are picked up as they appear.
  const start = () => {
    refreshLinks();
    new MutationObserver(() => refreshLinks()).observe(document.body, { childList: true, subtree: true });
    loadStatus();
    setInterval(loadStatus, STATUS_EVERY_MS);
  };
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', start); else start();
})();

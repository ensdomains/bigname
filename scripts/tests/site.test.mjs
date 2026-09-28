// Behavioural tests for the site's network switcher logic (site/network.js).
// Run with `node --test scripts/tests/`; no dependencies. network.js exports
// its DOM-free logic when loaded outside a browser, so these tests exercise
// the same code the pages run.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);
const net = require('../../site/network.js');
const sepolia = net.selectable('sepolia');

test('mainnet is listed but cannot be selected until its API exists', () => {
  assert.equal(sepolia.id, 'sepolia');
  assert.equal(net.selectable('mainnet'), null);
  assert.equal(net.selectable('nope'), null);
  assert.ok(net.NETWORKS.find(n => n.id === 'mainnet' && n.coming));
  const html = net.controlHtml(sepolia, null);
  assert.match(html, /<button type="button" disabled[^>]*>mainnet<small>coming<\/small><\/button>/);
  assert.doesNotMatch(html, /data-net="mainnet"/);
});

test('an unknown, coming or missing network falls back to Sepolia and says so', () => {
  assert.deepEqual(net.initialSelection(''), { network: sepolia, custom: null, rewrite: false });
  assert.deepEqual(net.initialSelection('?network=sepolia'), { network: sepolia, custom: null, rewrite: false });
  assert.deepEqual(net.initialSelection('?network=mainnet'), { network: sepolia, custom: null, rewrite: true });
  assert.deepEqual(net.initialSelection('?network=nope&x=1'), { network: sepolia, custom: null, rewrite: true });
});

test('parseApi accepts absolute http(s) URLs and rejects the rest outright', () => {
  assert.equal(net.parseApi('http://127.0.0.1:3000'), 'http://127.0.0.1:3000');
  assert.equal(net.parseApi('http://127.0.0.1:3000/'), 'http://127.0.0.1:3000');
  assert.equal(net.parseApi('https://api.example.test/base/?q=1#f'), 'https://api.example.test/base');
  assert.equal(net.parseApi('https://user:pw@api.example.test'), 'https://api.example.test');
  for (const bad of [
    null, '', '/v1', 'api.example.test', 'javascript:alert(1)', 'ftp://example.test',
    "https://x/';/usr/bin/id;'", 'https://x/a b', 'https://x/a\tb', 'https://x/a;b', 'https://x/a"b',
    'https://x/a\\b', 'https://x/a`b', 'https://x/<b>',
    'https://example.com/x$(id)', 'https://example.com/$HOME', 'https://example.com/x(1)',
  ]) assert.equal(net.parseApi(bad), null, String(bad));
  assert.deepEqual(net.initialSelection("?api=https://x/';id;'"), { network: sepolia, custom: null, rewrite: true });
  assert.equal(net.initialSelection('?api=http://127.0.0.1:3000').custom, 'http://127.0.0.1:3000');
});

test('writing the selection keeps unrelated query parameters', () => {
  const q = net.buildQuery('?utm_source=review&network=nope&flag=1&api=http://127.0.0.1:3000', sepolia, null);
  assert.equal(q, '?utm_source=review&network=sepolia&flag=1');
  assert.equal(net.buildQuery('?utm_source=review', sepolia, 'http://127.0.0.1:3000'),
    '?utm_source=review&network=sepolia&api=http%3A%2F%2F127.0.0.1%3A3000');
  assert.equal(net.buildQuery('', sepolia, null), '?network=sepolia');
});

test('carrying merges into the link query, keeps the fragment, and is idempotent', () => {
  const custom = 'http://127.0.0.1:3000';
  assert.equal(net.carryHref('docs/#name', sepolia, null), 'docs/?network=sepolia#name');
  assert.equal(net.carryHref('../', sepolia, custom), '../?network=sepolia&api=http%3A%2F%2F127.0.0.1%3A3000');
  assert.equal(net.carryHref('docs/?tab=2#records', sepolia, null), 'docs/?tab=2&network=sepolia#records');
  for (const href of ['docs/#name', '../', 'docs/?tab=2#records', './?a=1&b=2']) {
    const once = net.carryHref(href, sepolia, custom);
    assert.equal(net.carryHref(once, sepolia, custom), once, href);
    assert.equal(once.split('?').length, 2, once);
    // dropping the custom API later removes it from an already carried link
    assert.doesNotMatch(net.carryHref(once, sepolia, null), /api=/);
  }
});

test('query-derived and API-derived text is escaped before it becomes markup', () => {
  assert.equal(net.esc(`<a href="x">'&'</a>`), '&lt;a href=&quot;x&quot;&gt;&#39;&amp;&#39;&lt;/a&gt;');
  const hostile = '"><img src=x onerror=alert(1)>';
  const control = net.controlHtml(sepolia, hostile);
  assert.doesNotMatch(control, /<img/);
  assert.match(control, /title="custom: &quot;&gt;&lt;img/);
  assert.match(net.controlHtml(sepolia, 'http://x/a&b'), /a&amp;b/);
  const detail = net.statusDetailHtml(
    { status: 'ready', chains: { '11155111': { status: '<script>x</script>', latest_block: 5, indexed_block: 4 } } },
    hostile, 'custom');
  assert.doesNotMatch(detail, /<script>|<img/);
  assert.match(detail, /sepolia &lt;script&gt;x&lt;\/script&gt; at 5/);
  assert.match(net.statusDetailHtml(null, 'https://sepolia.api.bigname.sh', '<b>'), /&lt;b&gt; unreachable/);
});

// A fetch whose answers the test releases by hand, and which rejects on abort
// like the browser's does.
function manualFetch() {
  const calls = [];
  const fetchImpl = (url, init) => new Promise((resolve, reject) => {
    const call = {
      url, init, aborted: false,
      answer: data => resolve({ ok: true, json: async () => ({ data }) }),
      fail: () => reject(new TypeError('failed to fetch')),
    };
    init.signal.addEventListener('abort', () => { call.aborted = true; reject(new DOMException('aborted', 'AbortError')); });
    calls.push(call);
  });
  return { calls, fetchImpl };
}
const flush = () => new Promise(r => setImmediate(r));
const ready = block => ({ status: 'ready', chains: { '11155111': { status: 'ready', latest_block: block } } });

test('at most one status request is outstanding; timer ticks while one is out do nothing', async () => {
  const { calls, fetchImpl } = manualFetch();
  const status = net.createStatus({ fetchImpl, getBase: () => 'https://a.test' });
  const seen = [];
  status.subscribe((d, base) => seen.push([d && d.chains['11155111'].latest_block, base]));
  const first = status.tick();
  assert.ok(first);
  assert.equal(status.tick(), null);
  assert.equal(status.tick(), null);
  await flush();
  assert.equal(calls.length, 1);
  assert.equal(calls[0].url, 'https://a.test/v1/status');
  assert.equal(status.inFlight, true);
  calls[0].answer(ready(1));
  await first;
  assert.equal(status.inFlight, false);
  assert.deepEqual(seen, [[1, 'https://a.test']]);
  status.tick();
  await flush();
  assert.equal(calls.length, 2);
});

test('a selection change aborts the old request, resets the cache, and drops a late answer', async () => {
  const { calls, fetchImpl } = manualFetch();
  let base = 'https://a.test';
  const status = net.createStatus({ fetchImpl, getBase: () => base });
  const seen = [];
  status.subscribe((d, b) => seen.push([d && d.chains['11155111'].latest_block, b]));

  // A answers first and is cached.
  const a1 = status.tick();
  await flush();
  calls[0].answer(ready(100));
  await a1;
  assert.deepEqual(status.cache, { pending: false, base: 'https://a.test', data: ready(100) });

  // A second request to A is out when the selection moves to B.
  status.tick();
  await flush();
  base = 'https://b.test';
  const b1 = status.restart();
  assert.equal(calls[1].aborted, true);
  assert.deepEqual(status.cache, { pending: true, base: 'https://b.test', data: null });

  // A listener registered now must not receive A's cached answer.
  const late = [];
  status.subscribe((d, b) => late.push(b));
  assert.deepEqual(late, []);

  // The old request finishing (here: its abort) must not clear the new one's mark.
  await flush();
  assert.equal(calls.length, 3);
  assert.equal(calls[2].url, 'https://b.test/v1/status');
  assert.equal(status.inFlight, true);
  assert.equal(status.tick(), null);

  calls[2].answer(ready(7));
  await b1;
  assert.equal(status.inFlight, false);
  assert.deepEqual(seen, [[100, 'https://a.test'], [7, 'https://b.test']]);
  assert.deepEqual(late, ['https://b.test']);
});

test('out-of-order answers: only the newest request publishes', async () => {
  const calls = [];
  // A fetch that ignores abort, as a slow network might, so the old answer does arrive late.
  const fetchImpl = url => new Promise(resolve => calls.push({ url, answer: data => resolve({ ok: true, json: async () => ({ data }) }) }));
  let base = 'https://a.test';
  const status = net.createStatus({ fetchImpl, getBase: () => base });
  const seen = [];
  status.subscribe((d, b) => seen.push(b));
  const a = status.tick();
  await flush();
  base = 'https://b.test';
  const b = status.restart();
  await flush();
  calls[1].answer(ready(2));
  await b;
  calls[0].answer(ready(1));
  await a;
  assert.deepEqual(seen, ['https://b.test']);
  assert.equal(status.cache.base, 'https://b.test');
  assert.equal(status.inFlight, false);
});

test('an unreachable API publishes null for the base that was asked', async () => {
  const { calls, fetchImpl } = manualFetch();
  const status = net.createStatus({ fetchImpl, getBase: () => 'https://down.test' });
  const seen = [];
  status.subscribe((d, b) => seen.push([d, b]));
  const p = status.tick();
  await flush();
  calls[0].fail();
  await p;
  assert.deepEqual(seen, [[null, 'https://down.test']]);
});

// Timers the test fires by hand.
function manualTimers() {
  const timers = new Map();
  let next = 1;
  return {
    timers,
    setTimer: (fn, ms) => { const id = next++; timers.set(id, { fn, ms }); return id; },
    clearTimer: id => { timers.delete(id); },
    fire: () => { for (const [id, t] of [...timers]) { timers.delete(id); t.fn(); } },
  };
}

test('a status request that never answers is aborted after the bound and frees the slot', { timeout: 5000 }, async () => {
  const { calls, fetchImpl } = manualFetch();
  const clock = manualTimers();
  const status = net.createStatus({ fetchImpl, getBase: () => 'https://hang.test', setTimer: clock.setTimer, clearTimer: clock.clearTimer });
  const seen = [];
  status.subscribe((d, b) => seen.push([d, b]));
  const p = status.tick();
  await flush();
  assert.equal(calls.length, 1);
  assert.deepEqual([...clock.timers.values()].map(t => t.ms), [net.STATUS_TIMEOUT_MS]);
  assert.equal(net.STATUS_TIMEOUT_MS, 10000);
  assert.equal(status.tick(), null, 'still in flight before the bound');
  clock.fire();
  await p;
  assert.equal(calls[0].aborted, true);
  assert.equal(status.inFlight, false);
  assert.deepEqual(seen, [[null, 'https://hang.test']], 'reads as unreachable');
  const again = status.tick();
  assert.ok(again, 'the next tick starts a new request');
  await flush();
  assert.equal(calls.length, 2);
  calls[1].answer(ready(9));
  await again;
  assert.equal(clock.timers.size, 0, 'an answered request clears its timer');
  assert.equal(seen.at(-1)[1], 'https://hang.test');
  assert.equal(seen.at(-1)[0].chains['11155111'].latest_block, 9);
});

test('the timeout of an aborted old request cannot touch the new one', async () => {
  const { calls, fetchImpl } = manualFetch();
  const clock = manualTimers();
  let base = 'https://a.test';
  const status = net.createStatus({ fetchImpl, getBase: () => base, setTimer: clock.setTimer, clearTimer: clock.clearTimer });
  status.tick();
  await flush();
  base = 'https://b.test';
  const b = status.restart();
  await flush();
  assert.equal(calls[0].aborted, true);
  assert.equal(clock.timers.size, 1, 'the old request cleared its timer when it ended');
  assert.equal(status.inFlight, true);
  calls[1].answer(ready(3));
  await b;
  assert.equal(status.cache.base, 'https://b.test');
});

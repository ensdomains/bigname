# site

The public face of the bigname API: a landing page (`index.html`), the
hand-written API reference (`docs/index.html`), the shared network switcher
(`network.js`), and a not-found page (`404.html`). It is a static site with no
build step, no package manager and no framework; the files here are the files
that get served.

`404.html` matters to the host: Cloudflare Pages serves it for any missing
path. Without it, Pages treats the site as a single-page app and answers every
unknown path (`/openapi.json`, say) with the landing page and `200`. It loads
no script and links to `/` and `/docs/` by absolute path, because it can be
served at any depth.

The API binary does not serve these pages. The site is hosted on its own
origin and talks to an API across origins, which the API allows (it answers
`/v1` with permissive CORS).

## Networks

`network.js` is shared by both pages. It holds the table of networks and the
API each one is served from, renders the switcher in the page header, and
fetches `<api>/v1/status` for the selected network to show each chain's head
block and status. Every API request the pages make (the status pill, the
try-it panels, the curl commands they print) goes through its `apiBase()`,
never the page's own origin. The docs page still loads its web fonts from
Google Fonts, as it did before.

- Sepolia is the default: `https://sepolia.api.bigname.sh`.
- Mainnet is listed as coming and cannot be selected until its API exists.
  When it does, drop `coming: true` from its entry in `network.js`.

The selection lives in the URL, so a link says which network it means:

- `?network=sepolia` or `?network=mainnet` picks from the table.
- `?api=<absolute url>` points the site at any API and wins over the table;
  the switcher shows it as "custom". Use it for local development. A value
  with a quote, backslash, whitespace, semicolon or angle bracket is rejected,
  not cleaned, because the pages print the base into curl commands.

An unknown `?network=`, one still marked coming, or a rejected `?api=` falls
back to Sepolia, and the address bar is rewritten to say so. Switching
networks sets `network` (and sets or removes `api`) in the query string and
preserves every other query parameter. Links between the two pages
(`data-carry` in the markup) get `network` and `api` merged into their own
query, keeping their fragment.

The status line asks `<api>/v1/status` every 30 seconds, with at most one
request outstanding. Switching networks cancels the pending request, forgets
the previous network's answer, and asks the new one.

## Preview locally

```sh
cd site
python3 -m http.server 8765
```

Then open `http://127.0.0.1:8765/`, or
`http://127.0.0.1:8765/?api=http://127.0.0.1:3000` to use a local API started
with `cargo run -p bigname-api`.

## Checks

These run in CI's `site` job and in the API crate's suite:

- `scripts/check-site` is a set of static guards over the source: the files
  exist; page fetches start from a base captured from `NET.apiBase()`; no
  root-relative link or direct use of the page's own origin; generated curl
  commands quote every value with `shq()`; no server-side placeholder or
  retired hostname. It checks patterns, so it
  catches the usual regressions rather than proving every request is right.
- `node --test scripts/tests/site.test.mjs` exercises the logic in
  `network.js` (exported when it is loaded outside a browser): query and link
  merging, the `?api=` rules, mainnet not selectable, one outstanding status
  request, late answers dropped, the cache reset on a switch, and escaping.
- The API crate's `site_pages` tests, which already held the pages to the
  router, now read these files: every `/v1` route the API serves must have a
  manual page here, and every upstream citation here must be in a contract
  doc.

## Hosting

Hosting (Cloudflare Pages) is provisioned separately, by the infrastructure
part of TYR-56, not from this repository. After the site checks pass, CI
uploads `site/` as an artifact.

The OpenAPI document will be served by the API at `/openapi.json` (TYR-18).
The public edge already admits that path; until the document ships the API
answers `404`.

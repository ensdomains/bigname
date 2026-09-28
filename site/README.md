# site

The public face of the bigname API: a landing page (`index.html`) and the
hand-written API reference (`docs/index.html`). It is a static site with no
build step, no package manager and no framework; the files here are the files
that get served.

The API binary does not serve these pages. The site is hosted on its own
origin and talks to an API across origins, which the API allows (it answers
`/v1` with permissive CORS).

## Networks

`network.js` is shared by both pages. It holds the table of networks and the
API each one is served from, renders the switcher in the page header, and
fetches `<api>/v1/status` for the selected network to show each chain's head
block and status. Every request the pages make (the status pill, the try-it
panels, the curl commands they print) goes through its `apiBase()`, never the
page's own origin.

- Sepolia is the default: `https://sepolia.api.bigname.sh`.
- Mainnet is listed as coming and cannot be selected until its API exists.
  When it does, drop `coming: true` from its entry in `network.js`.

The selection lives in the URL, so a link says which network it means:

- `?network=sepolia` or `?network=mainnet` picks from the table.
- `?api=<absolute url>` points the site at any API and wins over the table;
  the switcher shows it as "custom". Use it for local development.

Links between the two pages carry the query (`data-carry` in the markup).

## Preview locally

```sh
cd site
python3 -m http.server 8765
```

Then open `http://127.0.0.1:8765/`, or
`http://127.0.0.1:8765/?api=http://127.0.0.1:3000` to use a local API started
with `cargo run -p bigname-api`.

## Checks

`scripts/check-site` runs in CI: the files exist, no request goes to the page's
own origin, and no server-side placeholder is left. The API crate's
`site_pages` tests hold the reference to the router: every `/v1` route the API
serves must have a manual page here, and every upstream citation here must be
in a contract doc.

## Hosting

Hosting (Cloudflare Pages) is set up by the infrastructure part of TYR-56, not
from this repository. CI uploads `site/` as a build artifact on every run.

The OpenAPI document will be served by the API at `/openapi.json` (TYR-18).
The public edge already admits that path; until the document ships the API
answers `404`.

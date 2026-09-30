# Production

This page documents the public single-host deployment shape used for bigname.

The current public API hostname is `sepolia.api.bigname.sh`, serving Sepolia.
Mainnet follows at its own hostname once it is deployed.

## Public Edge

Public traffic terminates at Caddy, defined by `docker-compose.public.yml` and
`docker/caddy/Caddyfile`. Caddy forwards requests to the internal API service at
`api:3000`.

The public edge serves the API under the `/v1` prefix (#315). Its matchers
are:

- REST reads: `GET` and `HEAD` on `/v1/*`, which covers the lookup, status,
  product, and diagnostic route families in
  [`api-v1-routes.md`](api-v1-routes.md).
- REST lookup: `POST /v1/lookup` and its `OPTIONS` browser preflight.
- OpenAPI document: `GET` and `HEAD` on `/openapi.json`. The document is
  public and read-only, and the edge adds `Access-Control-Allow-Origin: *` so
  the documentation site can read it from another origin. The API embeds the
  generated OpenAPI 3.1 document for the 20 product operations, including package
  version and build SHA. Diagnostics and health remain outside the document.
  Its documentation links use that build's commit when a full Git SHA is
  available; local builds with an unknown or non-commit build label retain
  links to `main`.
  It answers without querying the database or providers, carries
  `Cache-Control: public, max-age=300` and a body-derived weak `ETag`, and answers
  a matching `If-None-Match` with a bodyless `304`. Regenerate with
  `scripts/generate-openapi`; CI checks freshness against the contract tables.

The removed `POST /v1/identity:lookup` matcher was dropped with the flip and
now falls through to the edge's `404`. The landing page and API reference no
longer live in the API binary: they are the static site in
[`site/`](../site/README.md), hosted separately, so `/`, `/docs`, and `/docs/`
answer `404` from the API and at the edge. The GraphQL matcher (`POST /graphql`
and its preflight) was dropped when the GraphQL compatibility surface was
removed (TYR-19); the API itself now answers `/graphql` like any unknown route,
and the edge returns `404`.

Requests outside these method and path matcher groups return `404` at the edge.
In particular, Caddy does not expose `/healthz`; the compose probe reaches it
at `127.0.0.1` inside the API container, while the process listens on its
configured bind address (`0.0.0.0:3000` by default in compose). This narrows the
helper allowlist introduced by #203 and prevents public traffic from competing
for the health-specific concurrency ceiling. `/v2/*` is not served by the
binary and not admitted by the edge. Phase-runner and PostgreSQL control
surfaces are not routed through Caddy.

### Internal-only versus public URLs

Not every URL the API process answers is a public one. Keep the two sets
apart when writing runbooks, dashboards, or smoke checks:

- Public (through Caddy): `GET`/`HEAD /v1/*`, `POST /v1/lookup` and its
  `OPTIONS` preflight, and `GET`/`HEAD /openapi.json`. Diagnostics under
  `/v1/diagnostics/*` are part of the public read surface by design (ADR 0006).
- Internal only (reachable on the API listener, never through Caddy):
  `GET /healthz`, process metrics, and any
  phase-runner or PostgreSQL control surface. Internal URLs are reached from
  inside the compose network (`api:3000`) or on the host loopback when
  `BIGNAME_API_HOST=127.0.0.1` publishes the port.

`scripts/public-edge-smoke` asserts both halves. Its
`BIGNAME_SMOKE_INTERNAL_API_URL` must therefore be reachable from the host
running the smoke; on a compose host that is the loopback-published API port,
not the `api` service name (#205). The smoke also replays the #205
encoded-traversal probes (`/v1/%2e%2e/v1/status`, `/v1/%2e%2e/v1/lookup`,
`/v1/%2e%2e/healthz`): Caddy matches on the cleaned path but proxies the raw
URI, so the API itself must answer `404` for a `..`-bearing path both directly
and through the edge.

## Environment

Use the normal server environment from `.env.server`, plus these production edge
settings:

```sh
BIGNAME_IMAGE=ghcr.io/ensdomains/bigname:<tag>
BIGNAME_API_HOST=127.0.0.1
BIGNAME_API_PORT=3000
BIGNAME_PUBLIC_SITE_ADDRESS=api.example.com
BIGNAME_PUBLIC_HTTP_PORT=80
BIGNAME_PUBLIC_HTTPS_PORT=443
```

The normal server environment must set `BIGNAME_API_DATABASE_URL` to the
dedicated non-owner login provisioned in
[`deployment.md`](deployment.md#surviving-services). The server Compose file
refuses to substitute the writer/owner `BIGNAME_DATABASE_URL` for the API.

`BIGNAME_API_HOST=127.0.0.1` keeps direct host access to the API on localhost
only. Public access goes through Caddy on ports 80 and 443.

### API request bounds

The API validates these process-wide bounds at startup. Durations are in
milliseconds. Defaults are deliberately generous so local and end-to-end
workloads do not need special tuning; the final column is the
recommended starting point before the public edge is undrained.

| Environment variable | Default | Undrain starting value | Mechanism |
| --- | ---: | ---: | --- |
| `BIGNAME_API_REQUEST_TIMEOUT_MS` | `30000` | `30000` | Whole-request deadline on every v2 REST, status, and health route; returns `408 request_timeout`. |
| `BIGNAME_API_DB_STATEMENT_TIMEOUT_MS` | `25000` | `25000` | PostgreSQL `statement_timeout` applied to both API request pools. The readiness pool has a fixed two-second check limit. |
| `BIGNAME_API_MAX_IN_FLIGHT` | `1024` | `256` | Shared process-wide in-flight ceiling; excess work is load-shed as `503 overloaded`. `/healthz` bypasses it. |
| `BIGNAME_API_HEALTH_MAX_IN_FLIGHT` | `4` | `4` | Independent in-flight ceiling reserved for `/healthz`; excess health work is load-shed as `503 overloaded`. |
| `BIGNAME_API_VERIFIED_EXECUTION_MAX_IN_FLIGHT` | `128` | `16` | Separate ceiling for requests that can initiate verified resolution or primary-name fallback; it must be lower than the global ceiling. |
| `BIGNAME_API_RPC_CONNECT_TIMEOUT_MS` | `2000` | `2000` | Connect deadline for API-triggered execution JSON-RPC calls; it must be less than `BIGNAME_API_RPC_TIMEOUT_MS`. |
| `BIGNAME_API_RPC_TIMEOUT_MS` | `8000` | `8000` | Total deadline for each API-triggered execution JSON-RPC call. |
| `BIGNAME_API_VERIFIED_RATE_LIMIT_PER_SECOND` | `0` (off) | `1` | Per-client token refill rate for verified-execution-triggering routes, keyed by an IPv4 address or IPv6 `/64`; excess requests return `429 rate_limited`. |
| `BIGNAME_API_VERIFIED_RATE_LIMIT_BURST` | `10` | `5` | Maximum tokens in each client bucket when rate limiting is enabled. |
| `BIGNAME_API_VERIFIED_RATE_LIMIT_MAX_CLIENTS` | `65536` | `65536` | In-memory client-bucket ceiling per API process. |
| `BIGNAME_API_TRUST_X_FORWARDED_FOR` | `false` | `true` | Whether the client-IP key may use the rightmost valid `X-Forwarded-For` address instead of the TCP peer. |

The API process attaches its two RPC deadlines when it constructs its lookup
provider configuration. Phase-runner hydration uses its own provider
configuration.

Rate limiting is off in the binary by default because the public contract has
no authenticated or otherwise stable client identity, and IP addresses may be
shared or rotate. Before undraining, set the recommended nonzero rate and burst
above, observe legitimate `429` volume, and tune them as deployment policy—not
as a stable per-user API quota. The API ignores `X-Forwarded-For` by default.
The undrain configuration explicitly trusts it because the single public path
is Caddy and binds the API's host-published port to `127.0.0.1`; in that topology
the API uses the rightmost valid address appended by Caddy. If the trusted header
is absent it uses the TCP peer address; an unidentifiable request shares one
fallback bucket. Never enable `BIGNAME_API_TRUST_X_FORWARDED_FOR` on a listener
that untrusted clients can reach directly.

When the client table remains full after reclaiming refilled buckets, unseen
keys fail closed with `429 rate_limited`; logarithmically sampled warning logs
report the rejection count without emitting one log line for every request.

When the rate limiter is enabled behind Caddy, `BIGNAME_API_TRUST_X_FORWARDED_FOR`
MUST be `true`; otherwise all clients share Caddy's single container-IP bucket
and the intended per-client limit becomes an accidental global throttle.

The undrain statement timeout remains `25000` as a conservative request-pool
ceiling. `/v1/status` no longer scans the legacy invalidation backlog: its
schema-v2 status read is bounded by the configured request timeout and uses
the phase lookup pool.

The RPC deadlines are shorter than the whole-request deadline. A hung provider
therefore becomes the route's existing in-band execution-failure result rather
than consuming an API request indefinitely. The request deadline remains a
backstop on `/healthz` and `/v1/status`; the status route remains
bounded by the phase lookup pool's statement timeout. `/healthz` alone bypasses
the process-wide concurrency limiter and load shedding. It concurrently runs
the database-identity query through a persistent one-connection readiness pool
and the normal serving pool; each probe has the same two-second limit, so their
combined wall-clock remains bounded by roughly two seconds rather than adding
the limits together. The readiness-pool result determines database
reachability, while `database.identity` is populated only when both probes
return the same opaque identity token. The
phase request pool uses `BIGNAME_DATABASE_MAX_CONNECTIONS`; the readiness
connection is additional, so the API process can open at most
`BIGNAME_DATABASE_MAX_CONNECTIONS + 1` PostgreSQL connections.
HTTP-concurrency saturation and exhaustion of the request pool therefore
cannot queue the readiness probe past the compose healthcheck's five-second
window: a healthy but busy process returns `200` with `status="ready"`, though
the serving-pool identity audit can return `null`. A readiness
connection failure or timeout instead returns `503` with `status="degraded"`,
preserving the database reachability check for a genuinely unavailable
PostgreSQL server. The health-specific ceiling prevents unbounded probe work.
The status routes retain global request admission with the other API routes;
their schema-v2 reads no longer aggregate the legacy backlog.

For a temporary HTTP-only deployment before DNS is ready, set:

```sh
BIGNAME_PUBLIC_SITE_ADDRESS=:80
```

When `BIGNAME_PUBLIC_SITE_ADDRESS` is a hostname with public DNS pointing at the
server, Caddy automatically obtains and renews TLS certificates.

## Start

Start or refresh the public stack, then recreate only the proxy so it loads the
current bind-mounted Caddyfile:

```sh
docker compose --env-file .env.server \
  -f docker-compose.server.yml \
  -f docker-compose.public.yml \
  up -d

docker compose --env-file .env.server \
  -f docker-compose.server.yml \
  -f docker-compose.public.yml \
  up -d --no-deps --force-recreate public-proxy
```

The targeted recreation is required for Caddyfile-only changes because plain
`docker compose up -d` does not recreate an otherwise unchanged proxy service.
The persistent Caddy data and configuration volumes are retained.

For a local image build on a server checkout, replace `BIGNAME_IMAGE` with
`bigname:local` in the environment used for the command.

## Verify

Check the internal API:

```sh
curl -fsS http://127.0.0.1:3000/healthz
```

Check the public edge:

```sh
test "$(curl -sS -o /dev/null -w '%{http_code}' -I http://127.0.0.1/)" = 404
test "$(curl -sS -o /dev/null -w '%{http_code}' -I http://127.0.0.1/docs)" = 404
test "$(curl -sS -o /dev/null -w '%{http_code}' -I http://127.0.0.1/openapi.json)" = 200
test "$(curl -sS -o /dev/null -w '%{http_code}' http://127.0.0.1/v1/status)" = 200
test "$(curl -sS -o /dev/null -w '%{http_code}' http://127.0.0.1/v2/status)" = 404
```

Run the positive and default-deny edge checks against Caddy and its internal API
listener. The preflight probes send the deployed Manager origin,
`https://app.ens.dev`:

```sh
BIGNAME_SMOKE_INTERNAL_API_URL=http://127.0.0.1:3000 \
BIGNAME_SMOKE_PUBLIC_EDGE_URL=https://api.example.com \
  ./scripts/public-edge-smoke
```

For hostname/TLS deployments, replace `127.0.0.1` with the public hostname and
`http` with `https`.

## Operations Notes

- Keep PostgreSQL unexposed at the host/network edge.
- Keep JSON-RPC providers reachable only from the containers that need them.
- Use [`runbooks/production-docker.md`](runbooks/production-docker.md) for
  current-host Docker operations, monitoring, pause/resume, and recovery
  checklists.
- Use the [production-scale benchmark gate](runbooks/benchmark-gate.md) before
  restoring traffic to a new generation and at every planned
  [re-derivation boundary](glossary.md#re-derivation-boundary).
- Use host firewall or cloud security groups to allow public `80/tcp` and
  `443/tcp`. Allow `443/udp` when HTTP/3 should be available. Do not publish
  database or execution-node admin ports.
- Caddy data lives in the `caddy-data` Docker volume. Preserve it across
  container recreates so certificate state survives restarts.
- Caddy sends HSTS and advertises HTTP/3 when the UDP port is published.

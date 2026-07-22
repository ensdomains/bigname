# Production

This page documents the public single-host deployment shape used for bigname.

The current production hostname is `bigname.taytems.xyz`.

## Public Edge

Public traffic terminates at Caddy, defined by `docker-compose.public.yml` and
`docker/caddy/Caddyfile`. Caddy forwards requests to the internal API service at
`api:3000`.

The public edge exposes an explicit allowlisted subset of the routes served by
the `bigname-api` process:

- Helpers: `GET` and `HEAD` on `/`, `/docs`, `/docs/`, and `/openapi.json`.
- REST: the operations published by `/openapi.json`. At the edge this is
  `GET` and `HEAD` on `/v1/*`, `POST /v1/identity:lookup`, and the
  `OPTIONS /v1/identity:lookup` browser preflight.
- GraphQL: `POST /graphql` and its `OPTIONS` browser preflight. This is an
  unauthenticated first-party ENS Manager compatibility subset governed by the
  [committed SDL fixture](../apps/api/src/tests/fixtures/subgraph_schema.graphql),
  separate from the v1 OpenAPI contract.

The Manager endpoint precondition was checked on 2026-07-21. The deployed
`https://app.ens.dev` application loaded the hashed
[`index-CKsYDyP0.js`](https://app.ens.dev/assets/index-CKsYDyP0.js) browser
bundle (SHA-256
`f45dd907511ae05efdf0fffa84ad0f53edab2b8b4530caf5cb95af9bf43b886b`),
which constructs its GraphQL client with the public
`https://graphql.ens.dev` endpoint. That is direct browser-to-indexer topology,
so the replacement endpoint must retain public `POST` and preflight access. A
same-day `OPTIONS` request to that endpoint with `Origin:
https://app.ens.dev`, requested method `POST`, and requested header
`content-type` returned `204` with permissive CORS headers; the edge smoke below
replays that real browser origin against the candidate edge. Recheck this
evidence before cutover: if Manager moves behind a private or same-origin
backend, the public GraphQL matcher can be removed. Otherwise the compatibility
endpoint sunsets when Manager migrates to the re-baselined v1 REST contract;
retaining it beyond that point requires an explicit decision to support the SDL
independently.

Requests outside these method and path matcher groups return `404` at the edge.
In particular, Caddy does not expose `/healthz`; the compose probe reaches it
at `127.0.0.1` inside the API container, while the process listens on its
configured bind address (`0.0.0.0:3000` by default in compose). This narrows the
helper allowlist introduced by #203 and prevents public traffic from competing
for the health-specific concurrency ceiling.
The API remains responsible for rejecting unknown paths or unsupported methods
inside an admitted matcher such as `GET /v1/*`. In particular, `/v2/*` is
internal parity and cutover staging and [never ships as a public
contract](adrs/0006-api-v2-product-surface.md#rollout); it returns `404`
publicly. `GET /graphql` is also denied, so GraphiQL is not exposed. Worker,
migration, PostgreSQL, and indexer control surfaces are not routed through
Caddy.

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

`BIGNAME_API_HOST=127.0.0.1` keeps direct host access to the API on localhost
only. Public access goes through Caddy on ports 80 and 443.

### API request bounds

The API validates these process-wide bounds at startup. Durations are in
milliseconds. Defaults are deliberately generous so local, end-to-end, and
conformance workloads do not need special tuning; the final column is the
recommended starting point before the public edge is undrained.

| Environment variable | Default | Undrain starting value | Mechanism |
| --- | ---: | ---: | --- |
| `BIGNAME_API_REQUEST_TIMEOUT_MS` | `30000` | `30000` | Whole-request deadline on every REST, GraphQL, docs, status, and health route; returns `408 request_timeout`. |
| `BIGNAME_API_DB_STATEMENT_TIMEOUT_MS` | `25000` | `25000` | PostgreSQL `statement_timeout` applied to primary API request-pool connections. The readiness pool has a fixed two-second check limit. |
| `BIGNAME_API_MAX_IN_FLIGHT` | `1024` | `256` | Shared process-wide in-flight ceiling; excess work is load-shed as `503 overloaded`. `/healthz` bypasses it. |
| `BIGNAME_API_HEALTH_MAX_IN_FLIGHT` | `4` | `4` | Independent in-flight ceiling reserved for `/healthz`; excess health work is load-shed as `503 overloaded`. |
| `BIGNAME_API_VERIFIED_EXECUTION_MAX_IN_FLIGHT` | `128` | `16` | Separate ceiling for requests that can initiate verified resolution or primary-name fallback; it must be lower than the global ceiling. |
| `BIGNAME_API_RPC_CONNECT_TIMEOUT_MS` | `2000` | `2000` | Connect deadline for API-triggered execution JSON-RPC calls. |
| `BIGNAME_API_RPC_TIMEOUT_MS` | `8000` | `8000` | Total deadline for each API-triggered execution JSON-RPC call. |
| `BIGNAME_API_VERIFIED_RATE_LIMIT_PER_SECOND` | `0` (off) | `1` | Per-client token refill rate for verified-execution-triggering routes, keyed by an IPv4 address or IPv6 `/64`; excess requests return `429 rate_limited`. |
| `BIGNAME_API_VERIFIED_RATE_LIMIT_BURST` | `10` | `5` | Maximum tokens in each client bucket when rate limiting is enabled. |
| `BIGNAME_API_VERIFIED_RATE_LIMIT_MAX_CLIENTS` | `65536` | `65536` | In-memory client-bucket ceiling per API process. |
| `BIGNAME_API_TRUST_X_FORWARDED_FOR` | `false` | `true` | Whether the client-IP key may use the rightmost valid `X-Forwarded-For` address instead of the TCP peer. |

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

The undrain statement timeout remains `25000` because the current status query
performs aggregate counts that can exceed five seconds under backlog. Lower it
only after status no longer depends on that expensive query.

The RPC deadlines are shorter than the whole-request deadline. A hung provider
therefore becomes the route's existing in-band execution-failure result rather
than consuming an API request indefinitely. The request deadline remains a
backstop on `/healthz`, `/v1/status`, and `/v2/status`; the status routes remain
bounded by the primary API pool's statement timeout. `/healthz` alone bypasses
the process-wide concurrency limiter and load shedding, and its `SELECT 1` uses
a persistent one-connection readiness pool with a two-second check limit. This
connection is additional to `BIGNAME_DATABASE_MAX_CONNECTIONS`. HTTP-concurrency
saturation and exhaustion of the primary API pool therefore cannot queue the
probe past the compose healthcheck's five-second window: a healthy but busy
process returns `200` with `status="ready"`. A readiness connection failure or
timeout instead returns `503` with `status="degraded"`, preserving the database
reachability check for a genuinely unavailable PostgreSQL server. The
health-specific ceiling prevents unbounded probe work. The status routes retain
global admission because their aggregate database query can be expensive under
backlog.

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
curl -fsS -I http://127.0.0.1/
curl -fsS -I http://127.0.0.1/docs
curl -fsS -I http://127.0.0.1/openapi.json
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
- Use host firewall or cloud security groups to allow public `80/tcp` and
  `443/tcp`. Allow `443/udp` when HTTP/3 should be available. Do not publish
  database or execution-node admin ports.
- Caddy data lives in the `caddy-data` Docker volume. Preserve it across
  container recreates so certificate state survives restarts.
- Caddy sends HSTS and advertises HTTP/3 when the UDP port is published. The
  docs page and OpenAPI JSON are cacheable for a short window; the `v1` API
  responses are not edge-cached by this configuration.

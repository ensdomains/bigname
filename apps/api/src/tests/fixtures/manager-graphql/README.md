# Manager GraphQL operations

These operation documents are copied from
`packages/indexer/documents` at the commit pinned by the
`manager-graphql-compat` CI job:
`759860f5acc62ea287b0feefa23c0d17aeb862a9`.

The response contract test combines each query with its referenced fragments, runs it against a
migrated scratch database, and compares the response with
`../graphql-response-contract.json`.

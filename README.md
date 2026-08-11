# Ionia

Ancient Greek texts and guided reading for Odysseia.

## Architecture

```text
Client -> Herodotos (GraphQL) -> Thoukydides/polemos (core gRPC)
                           `--> Diodoros/bibliotheke (text/corpus gRPC)

Hekataios/periodos -> seeds application data
Herakleitos -> seeds the classical text corpus
Artafernes/peira -> exercises the running system
```

- **Herodotos** is the public GraphQL gateway. It owns transport concerns and
  delegates domain work to gRPC services.
- **Thoukydides** is the core guided-reading service; its implementation lives
  in `polemos`. For the initial form design it reads opaque chapter documents
  from Elasticsearch and passes them through unchanged.
- **Diodoros** owns classical texts and corpus queries. Its `bibliotheke`
  package provides gRPC replacements for the former `options`, `_create`, and
  `_check` REST endpoints.
- **Hekataios** is the application-data seeder (the Ionia counterpart of
  Makedonia's Demokritos); its implementation lives in `periodos` and indexes
  each `rhema` chapter by its form ID.
- **Herakleitos** seeds the embedded classical corpus.
- **Artafernes** is the Ginkgo system-test suite; its scenarios live in
  `peira` and target a running Herodotos gateway.

Each component is an independent Go module. The root `go.work` makes local
development across modules convenient.

## Development

Prerequisites are Go 1.26, Buf, `protoc-gen-go`, and
`protoc-gen-go-grpc`.

```bash
make generate # regenerate protobuf bindings
make tidy     # tidy and format every module
make test     # run all tests
make build    # build all services
make test-system # run Artafernes against a live gateway
```

Default ports are `8080` for Herodotos and `50060` for both gRPC services.
Service addresses and ports can be overridden with
environment variables documented in each service's config package.

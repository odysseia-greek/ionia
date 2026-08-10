# Artafernes — Herodotos GraphQL system suite

Artafernes follows Makedonia's Dareios model: a Ginkgo v2 black-box suite that
talks to the public GraphQL gateway rather than importing service internals.

Set `HERODOTOS_URL` to the GraphQL endpoint. It defaults to
`http://localhost:8080/query`.

```bash
HERODOTOS_URL=http://localhost:8080/query make test-system
```

The initial `peira` scenarios cover service health, Elasticsearch-backed form
delivery, and the lifecycle of a guided-reading session. Run Hekataios before
the suite so at least one form exists.

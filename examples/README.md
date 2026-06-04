# Examples

The directories here are runnable, end-to-end demonstrations of
`autonomous-monitor` doing what it says on the tin. They are not
required to build or run the monitor itself.

## [`quickstart/`](./quickstart)

A `docker compose` stack that runs:

- Redpanda (single-broker Kafka replacement) on `:9092`
- The pure-Go build of `autonomous-monitor`, publishing to `k8s.namespace.findings`
- A small Go consumer that prints each finding as a human-readable line

**Start:**

```bash
cd quickstart
docker compose up --build
```

**Consume in a second terminal:**

```bash
docker compose logs -f consumer
```

You'll see a line per published finding. To trigger findings without a
real cluster, see `broken-pod.yaml` (apply it with a kubeconfig that
points at a kind/minikube cluster running alongside the compose stack).

## [`consumer/`](./consumer)

A standalone Go program that reads findings and prints them. Useful as
a starting point for any real consumer (Slack relay, PagerDuty bridge,
SIEM forwarder, etc.). Has its own `go.mod` so it can be lifted into
a separate project.

```bash
cd consumer
go mod download
CGO_ENABLED=0 go build -o consumer .
KAFKA_BROKER=localhost:9092 ./consumer
```

## [`grafana/`](./grafana)

`autonomous-monitor-dashboard.json` is a Grafana 10+ dashboard that
visualises every metric the monitor exposes. Import it via
**Dashboards → Import → Upload JSON file** and pick a Prometheus
datasource that scrapes the monitor's `:8080/metrics` endpoint.

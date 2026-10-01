# Isolate affinity

`POST /v1/invoke` supports four Graal isolate-affinity modes inside the tenant+deployment OS-process boundary.

```json
{
  "invocation_id": "inv-123",
  "tenant_id": "tenant-a",
  "deployment_id": "generation-9",
  "route_id": "orders.get",
  "session_id": "session-opaque-123",
  "affinity": "route_session",
  "payload_json": {},
  "timeout_ms": 30000
}
```

- `stateless`: no route/session key required; balanced over the pre-warmed isolate pool.
- `route`: requires `route_id`; reuses a route isolate.
- `session`: requires `session_id`; reuses a user/session isolate and defaults to a five-minute idle TTL.
- `route_session`: requires both keys; reuses the composite isolate and is intended only for routes whose contract explicitly opts into a user-private per-route heap.

The local API validates affinity/key consistency before dispatch. The trusted routing/auth layer is responsible for supplying `session_id`; guest payloads do not get to invent the authenticated affinity identity.

Each tenant process receives hard limits for stateless, route, session, and route-session isolates plus per-isolate Context concurrency. The cell runtime performs isolate-level eviction while the Rust daemon continues to own process-cell limits, generation pinning, deadlines, draining, and failure retirement.

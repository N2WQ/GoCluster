# GoCluster pilot console

The Worker serves the console and forwards a browser's WebSocket connection to GoCluster. Cloudflare Access must cover the **entire** `gocluster-pilot.n2wq-api.workers.dev` hostname, including `/ws`, with the approved pilot email policy. Keep Worker preview URLs disabled.

## Adjust the spot table

On a desktop, drag the right edge of a column heading to resize it. Drag the label to reorder it. Spots and History share the layout in this browser. Use **Reset columns** in Settings to restore the default layout. On phones, spots use stacked rows.

The call confidence badge shows `?` and `B` in red, `S` in yellow, `P` in light green, `V` in dark green, and `C` in blue. An unrated spot shows a neutral dot.

## Connect the production node

1. Run the WebSocket-capable GoCluster build on the production cluster machine. Set the `web` section's `enabled` to `true`, `listen_address` to `127.0.0.1:8080`, and `hostname` to `gocluster-pilot.n2wq-api.workers.dev`. Set `heartbeat_seconds` and `max_connections` to the configured pilot values. Supply `GOCLUSTER_WEB_EDGE_SECRET` in the cluster process environment before starting it. Use a random secret of at least 32 printable, non-space ASCII characters. Keep the value in the operator's secret store.
2. Run an authenticated Cloudflare Tunnel connector on that same machine. All connectors for this tunnel must reach the same GoCluster listener at their own loopback address. Verify its QUIC connection in Cloudflare. The private service uses this connector. [Workers VPC troubleshooting](https://developers.cloudflare.com/workers-vpc/reference/troubleshooting/)
3. From this Worker directory, register the dedicated HTTP VPC service against the actual tunnel UUID. If it already exists, inspect and reuse the matching service. The account must have permission to manage the Connectivity Directory. [VPC service configuration](https://developers.cloudflare.com/workers-vpc/configuration/vpc-services/)

   ```powershell
   npx wrangler vpc service create gocluster-pilot-origin --type http --http-port 8080 --ipv4 127.0.0.1 --tunnel-id <TUNNEL_UUID>
   ```

4. Put its returned service UUID in the deployment configuration. A VPC service binding uses the following Wrangler TOML table.

   ```toml
   [[vpc_services]]
   binding = "CLUSTER_ORIGIN"
   service_id = "<ACTUAL_VPC_SERVICE_UUID>"
   ```

5. Supply the same secret to the existing Worker at Wrangler's interactive prompt. Record the currently serving Worker version for rollback. Verify the local tests, inspect the dry run for the `ASSETS` and `CLUSTER_ORIGIN` bindings, then deploy.

   ```powershell
   npx wrangler deployments list
   npx wrangler secret put CLUSTER_EDGE_SECRET
   node --test test/worker.test.mjs
   npx wrangler deploy --dry-run
   npx wrangler deploy
   ```

To exercise the successful upgrade locally, run the Worker against a temporary loopback WebSocket server with the same workerd compatibility date. The test-only packages can live outside the checkout.

```powershell
$testDeps = Join-Path ([System.IO.Path]::GetTempPath()) "gocluster-worker-tests"
npm install --prefix $testDeps miniflare@5.20261123.0-beta ws@8.21.0
$env:NODE_PATH = Join-Path $testDeps "node_modules"
node --test test/worker.test.mjs test/workerd.cjs
```

## Verify the live session

Open the pilot in a private browser window. Confirm the Access sign-in challenge, authenticate with the approved email, select Live, and connect with the pilot callsign and SSID. In the browser network panel, the same-host `/ws` request should receive HTTP 101. Confirm the profile loads, record a new spot with its timestamp and callsign, and request History. The cluster's client address for the connected callsign should match the visitor's actual public IP. Check both IPv4 and IPv6 when available.

The Worker passes only the upgrade, the public origin, the visitor address and the private edge secret to the fixed origin. GoCluster checks the two address headers for agreement and rejects malformed addresses and mismatches. If the live handshake fails, use the HTTP status to narrow the issue. A 403 can indicate an Origin, address or secret mismatch. A 503 can indicate a missing binding or secret. A 502 can indicate that the tunnel, HTTP origin or WebSocket handshake failed. Check the tunnel health and loopback listener on the cluster host. Capture only the status and sanitized diagnostics.

To restore the previous console, run `npx wrangler rollback <PREVIOUS_WORKER_VERSION_ID>` and confirm that Access still protects the pilot. Disable the web listener in the production config and restart GoCluster during an approved maintenance window to withdraw the origin.

# GoCluster production browser console

Operators can sign in to the [production console](https://gocluster-pilot.n2wq-api.workers.dev/). Cloudflare Access checks your identity first. Choose **Live** and use a callsign with a separate SSID from your logger. Choose **Replay** to explore recorded data. The word `pilot` survives in deployed resource names and the repository path. These identifiers refer to production.

For routine changes, releases, verification and rollback, follow the [production update runbook](PRODUCTION.md). The setup instructions below are for provisioning or restoring the cluster host.

## Adjust the spot table

On a desktop, drag the right edge of a column heading to resize it. Drag the label to reorder it. Spots and History share the layout in this browser. Use **Reset columns** in Settings to restore the default layout. On phones, spots use stacked rows.

The call confidence badge shows `?` and `B` in red, `S` in yellow, `P` in light green, `V` in dark green, and `C` in blue. An unrated spot shows a neutral dot.

## Connect the production node

1. In the correct Cloudflare account, inspect the existing Access application and Allow policy. When provisioning, protect the **entire** production hostname above, including `/ws` and the assets, and allow the owner-approved identities. Keep Worker preview URLs disabled. Verify anonymous access is challenged before enabling the origin. [Cloudflare Access setup](https://developers.cloudflare.com/cloudflare-one/access-controls/applications/http-apps/self-hosted-public-app/)
2. Restore the WebSocket-capable GoCluster binary, private configuration and persistent data on the cluster host. For an authorized origin setup, the active runtime configuration uses `web.enabled: true`, `listen_address: "127.0.0.1:8080"`, and `hostname: "gocluster-pilot.n2wq-api.workers.dev"` under `web`. The shipped heartbeat is 30 seconds and the connection limit is 32. Restore the operator's values. Supply `GOCLUSTER_WEB_EDGE_SECRET` in the cluster process environment before starting it. The shared random value must be 32–256 printable non-space ASCII characters. Keep it in the operator's secret store. [Runtime configuration](../../data/config/README.md)
3. Reconnect the existing tunnel. In Cloudflare's tunnel settings select the connector instructions for the replacement host's OS and run the provided install command on the **cluster host**. Treat its token as a secret. Check connector health. Every connector for this tunnel must reach the production listener on its own loopback interface. The Worker uses a private VPC service. [Cloudflare connector installation](https://developers.cloudflare.com/cloudflare-one/networks/connectors/cloudflare-tunnel/get-started/create-remote-tunnel/)
4. From this Worker directory, inspect and reuse the configured HTTP VPC service. If rebuilding it, register the service against the actual tunnel UUID. The account must have permission to manage the Connectivity Directory. [VPC service configuration](https://developers.cloudflare.com/workers-vpc/configuration/vpc-services/)

   ```powershell
   npx wrangler vpc service create gocluster-pilot-origin --type http --http-port 8080 --ipv4 127.0.0.1 --tunnel-id <TUNNEL_UUID>
   ```

5. If the service was recreated, use its returned service UUID in the deployment configuration.

   ```toml
   [[vpc_services]]
   binding = "CLUSTER_ORIGIN"
   service_id = "<ACTUAL_VPC_SERVICE_UUID>"
   ```

6. For provisioning or authorized rotation, supply the matching secret to the Worker at Wrangler's interactive prompt. Coordinate rotation with the cluster process because both ends must match. Ordinary console releases reuse the existing secret. Follow the [production update runbook](PRODUCTION.md) for testing and deployment.

   ```powershell
   npx wrangler secret put CLUSTER_EDGE_SECRET
   ```

To exercise the successful upgrade locally, run the Worker against a temporary loopback WebSocket server with the same workerd compatibility date. The test-only packages can live outside the checkout.

```powershell
$testDeps = Join-Path ([System.IO.Path]::GetTempPath()) "gocluster-worker-tests"
npm install --prefix $testDeps miniflare@5.20261123.0-beta ws@8.21.0
$env:NODE_PATH = Join-Path $testDeps "node_modules"
node --test test/worker.test.mjs test/workerd.cjs
```

## Verify the live session

Open the production hostname in a private browser window. Confirm the Access sign-in challenge, authenticate with an approved identity, select Live, and connect with a dedicated callsign and SSID. In the browser network panel, the same-host `/ws` request should receive HTTP 101. Confirm the profile loads, record a new spot with its timestamp and callsign, and request History. The cluster's client address for the connected callsign should match the visitor's actual public IP. Check both IPv4 and IPv6 when available.

The Worker passes only the upgrade, the public origin, the visitor address and the private edge secret to the fixed origin. GoCluster checks the two address headers for agreement and rejects malformed addresses and mismatches. If the live handshake fails, use the HTTP status to narrow the issue. A 403 can indicate an Origin, address or secret mismatch. A 503 can indicate a missing binding or secret. A 502 can indicate that the tunnel, HTTP origin or WebSocket handshake failed. Check the tunnel health and loopback listener on the cluster host. Capture only the status and sanitized diagnostics.

Use the [rollback procedure](PRODUCTION.md#rollback) for a failed site deployment.

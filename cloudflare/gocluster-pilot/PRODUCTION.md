# Update the production browser console

The public entry point is [GoCluster Console](https://gocluster-pilot.n2wq-api.workers.dev/). A change deployed to this Worker reaches real users. The production Worker is named `gocluster-pilot`.

## Where a change belongs

| What you want to change | Files in this directory | What runs in production |
|---|---|---|
| Layout, spot rendering, controls or styles | `site/index.html` and `site/assets/app.*.js` or `app.*.css` | Cloudflare serves the files. The browser executes them. |
| How the Worker serves assets or relays the socket | `src/worker.mjs` | The Worker handles requests at the edge. Its `/ws` route uses the private origin binding. |
| Which Worker, assets or origin binding to deploy | `wrangler.toml` | `ASSETS` points to the uploaded `site/` directory. `CLUSTER_ORIGIN` identifies the private HTTP service. |
| Spot calculation, cluster commands, protocol or origin listener | Go source in the repository, including `telnet/` | The GoCluster executable runs on the cluster host. A backend change has its own build, test and rollout. |
| Who may sign in or which host runs the connector | Cloudflare Access and Tunnel settings | Cloudflare checks identity before requests reach the Worker. The connector reaches GoCluster at `127.0.0.1:8080` on the cluster host. |

GitHub holds the versioned Worker and deployable console. GitHub Actions runs repository checks. A Git push does **not** publish the site. Wrangler publishes files from the directory on the machine running the command. A UI-only update requires neither an edit to production YAML nor a cluster restart. A fresh browser session and the replay fixture test different things. Replay can work while the real cluster is unavailable.

## Start from a known revision

Use Git, PowerShell, Python 3, Node.js with npm, and a browser. The production deployment previously used Wrangler 4.167.0. The commands below pin that version for repeatability. On a replacement development PC, clone the repository, then enter the Worker directory. On an existing PC, start at `git status` in the checkout you intend to use.

```powershell
git clone https://github.com/N2WQ/GoCluster.git
Set-Location GoCluster
git status --short --branch
git rev-parse HEAD
Set-Location cloudflare/gocluster-pilot
```

Inspect existing edits before starting. A colleague may already have a newer UI on disk. For a release from a specific commit, use a clean checkout of that commit. Never infer the live revision from the branch name. A developer can publish local files before pushing them.

## Edit and rebuild the deployable assets

This checkout contains the browser bundles in readable form. There is no separate UI source build step in this directory. For example, change a spot heading in the tracked `site/assets/app.*.js` and its alignment in `site/assets/app.*.css`. Keep the replay and live views consistent. Vendor scripts and fonts are separate files.

For an ordinary change to those app bundles or the HTML, run the following from the Worker directory **after editing**. It renames a changed app file using the first ten hex digits of its SHA-256 digest, updates the HTML references, and records the byte length and full digest for each site file. Review the list of files in `site/` before running it because everything there is intended for upload. For changes to deployment settings in HTML, also update the corresponding `settings` in the manifest.

```powershell
@'
from pathlib import Path
import hashlib
import json

site = Path("site")
index = site / "index.html"
manifest = site / "asset-manifest.json"
document = json.loads(manifest.read_bytes())
html = index.read_bytes()
for extension in ("js", "css"):
    [old] = (site / "assets").glob(f"app.*.{extension}")
    digest = hashlib.sha256(old.read_bytes()).hexdigest()
    new = old.with_name(f"app.{digest[:10]}.{extension}")
    if html.count(old.name.encode()) != 1:
        raise SystemExit(f"Expected one HTML reference to {old.name}")
    if old != new:
        old.rename(new)
        html = html.replace(old.name.encode(), new.name.encode())
index.write_bytes(html)
files = {}
for path in sorted(site.rglob("*")):
    if path.is_file() and path != manifest:
        data = path.read_bytes()
        files[path.relative_to(site).as_posix()] = {
            "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()
        }
document["files"] = dict(sorted(files.items()))
manifest.write_bytes((json.dumps(document, indent=2) + "\n").encode())
'@ | python -
if ($LASTEXITCODE) { throw "Asset update failed" }
```

Review `git status --short` and `git diff`. Include the newly named files, removed old names, updated index and manifest in the same commit. Stage only intended paths. Keep local credentials, login state and test output out of the release.

## Test before publishing

1. From the Worker directory run `node --check` against the single app JavaScript file, then run both Worker tests using the dependency installation and commands in the [console README](README.md#connect-the-production-node). The integration test checks a real WebSocket upgrade against a temporary local origin.
2. To test the UI locally, run `python -m http.server 8000 --bind 127.0.0.1 --directory site`. Open `http://127.0.0.1:8000/`, select **Replay the pilot capture**, and verify the changed behavior. For table changes, check Spots, History, and the narrow-screen layout. Stop the local server with Ctrl+C. Live connections require the production origin or a suitable test bridge.
3. Run the [repository validation lane](../../docs/dev-runbook.md) for any backend or configuration changes. Commit the intended release, push under the repository's normal approval process, and identify the Git SHA used for the release. Check CI on that SHA. The Worker tests and browser checks above provide evidence beyond repository CI.

## Publish the tested revision

Run these commands in `cloudflare/gocluster-pilot` in the release checkout. Use an authorized Cloudflare account. For an interactive deployment, `login` may open a browser for consent. Wrangler performs the upload. Browser automation is optional.

```powershell
git status --short -- .
git rev-parse HEAD
npx wrangler@4.167.0 whoami
# If authentication is needed, log in, then repeat whoami.
# npx wrangler@4.167.0 login
npx wrangler@4.167.0 deployments list
npx wrangler@4.167.0 deploy --dry-run
```

Resolve unexpected local differences or account and binding mismatches before deploying. Record the currently serving **Worker version ID** for rollback. Check the target Worker name and both bindings against `wrangler.toml`. Once the production deployment is authorized, run:

```powershell
npx wrangler@4.167.0 deploy
npx wrangler@4.167.0 deployments list
```

Record the new version ID reported by Wrangler. A dashboard edit or deployment from another checkout can change the serving version later.

## Verify what users actually received

1. Follow the [live acceptance check](README.md#verify-the-live-session). A private window should encounter Access before it can load the app. After sign-in, verify a live spot and a History response.
2. Hard-refresh the page with the browser Network panel open. Confirm the loaded app JS and CSS filenames match `site/index.html` at the release commit. In the same authenticated browser, open `/asset-manifest.json` on the production hostname and compare its hashes with the committed manifest. Exercise the actual changed control or view. A successful upload alone cannot establish this.
3. Keep a release receipt with the UTC time, hostname, Git SHA, CI result, previous and new Worker version IDs, observed asset filenames, and live result. An agent handing off the release should report which checks it observed and which are still pending. Keep authentication tokens, cookies and secret values out of receipts. If using a temporary interactive login, finish with `npx wrangler@4.167.0 logout`.

## Rollback

For a faulty site release, use the previous serving **version ID** recorded before deployment. From the Worker directory, run `npx wrangler@4.167.0 rollback <previous-version-id>`, substituting the actual ID. Check `deployments list`, refresh and repeat live acceptance and the asset comparison against the restored revision. Record the rollback and reconcile the Git change before the next deployment. A Worker rollback changes the serving version. Access policies, tunnel setup, cluster data and the Go binary have separate recovery procedures. [Cloudflare rollback behavior](https://developers.cloudflare.com/workers/versions-and-deployments/rollbacks/)

## Recover from machine loss

| What was lost | Recovery |
|---|---|
| Development PC | The deployed Worker and cluster operate independently of that PC. Sign in from another browser. For future edits, clone GitHub, install the tools above and authenticate to the correct Cloudflare account. Browser-local table preferences may need setting again. |
| Cluster host | Restore your private runtime configuration, user profiles, spot history, required data and service startup settings from an off-machine backup. Restore a compatible binary or build the tagged or committed source. Verify the chosen release actually includes the web listener. Restore the service environment and reconnect the tunnel on that host. Follow [origin setup and live verification](README.md#connect-the-production-node). |
| Worker deployment or account configuration | Recover access to the owning Cloudflare account. Verify the hostname-wide Access policy, tunnel identity, connector health, VPC service and binding. Provision or restore the matching shared secret on both ends, deploy the known-good Git revision and repeat live verification. |

Keep off-machine copies of the release identity, production data backup, service configuration and account recovery information. Store secrets separately in the operator's credential store. A clone supplies the code and public defaults. Recoverability of private station data depends on the backups the operator has actually made and tested.

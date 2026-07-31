# AGENTS.md — Hinweise für Coding-Agents

Kurzreferenz für die Arbeit an diesem Repo. Ausführliche Nutzer-Doku: README.md.

## Was das hier ist

RWX-Volumes für Hetzner Cloud CSI: pro RWX-Volume exportiert ein
Share-Manager-Pod (hostNetwork, privileged) ein hcloud-Block-Volume via
NFSv4 (NFS-Ganesha V12.0, aus Upstream-Source gebaut, mit gepatchtem
`hcloud`-Recovery-Backend). Vier Rust-Binaries in einem Container-Image —
siehe README → Components.

## Bauen & Testen

```bash
cargo test --locked                         # 21 Unit-Tests
cargo clippy --all-targets --locked -- -D warnings   # CI erzwingt warning-frei
kubectl kustomize k8s/base >/dev/null       # Manifeste müssen rendern
```

- `protoc` wird von `build.rs` benötigt (CSI-Codegen).
- Container-Image: `podman build -f Containerfile .` — Rust-Stage wird
  cross-kompiliert (TARGETARCH), ganesha-Stage läuft unter QEMU und dauert
  ~20 min.
- Patch-Validierung: `git apply --check ganesha-patch/hcloud-ganesha.patch`
  gegen ein Checkout von nfs-ganesha **V13.0** (Commit-Pin steht in
  `ganesha-patch/build-ganesha.sh`).
- Dependency-Pinning: Wir klonen nur den ganesha-Tag und nehmen dessen
  **eigene Submodul-Pins** für ntirpc, libkmip und prometheus-cpp-lite; das
  Skript asserted die erwarteten Commits. Niemals eigene Kombinationen
  zusammenstellen, die Upstream nie getestet hat.
- Upstream-Kadenz beachten: nfs-ganesha veröffentlicht alle 1–2 Wochen einen
  neuen Major und pflegt **keine** Stable-Branches ab V7. Es gibt also keinen
  Backport-Kanal — Fixes kommen nur über den nächsten Major. Bewusst auf
  einem getesteten Major bleiben und kontrolliert springen.

## Architektur-Invarianten (nicht verletzen)

1. **Provisionierung nur in `src/bin/csi.rs`** (CreateVolume/DeleteVolume).
   `src/bin/controller.rs` ist ein reiner **Failover-Controller** — er
   erstellt niemals PVs oder Erst-Ressourcen. Historie: eine frühere
   Doppel-Implementierung mit abweichendem Naming wurde entfernt.
2. **Gemeinsame Specs in `src/provision.rs`**, per `#[path]` in beide
   Binaries eingebunden. Pod-/Service-/PVC-Spezifikationen und Env-Lookups
   NUR dort ändern. Ressourcen-Naming ist volume_id-basiert
   (`pvc-<uid>` → `share-manager-<vn>`, `<vn>-backing`, `state-<vn>`).
3. **PV-Sources sind immutable.** NFS-Endpoint-Änderungen laufen NICHT über
   PV-Updates: `NodePublishVolume` fragt die Share-Manager-Service-API
   (`http://<svc>:9500/endpoint`) nach dem aktuellen Endpoint;
   `volume_context.nfsEndpoint` ist nur Fallback.
4. **`ganesha-patch/recovery_hcloud.c` ist LGPL-3.0** (Derivat von
   Longhorns `recovery_longhorn.c` aus rancher/nfs-ganesha). Lizenz-Header
   und Modifikationsliste im Dateikopf sowie NOTICE aktuell halten. Niemals
   als Eigenentwicklung ausgeben.
5. Der Patch enthält `recovery_hcloud.c` NICHT — `build-ganesha.sh` kopiert
   die Datei separat in den ganesha-Tree. Patch (7 Dateien) und C-Datei
   nicht redundant pflegen.
6. **Ganesha sendet historisch HTTP-Bodies mit trailing NUL**
   (`strlen(payload)+1` im Longhorn-Original). Das Recovery-Backend
   (`parse_input` in `recovery_backend.rs`) muss das tolerieren; der eigene
   C-Client sendet seit dem Fix ohne NUL.
7. Share-Manager-Exit: unhealthy ⇒ Exit-Code 1 (Pod-Phase `Failed`). Der
   Failover-Controller behandelt `Failed` | `Unknown` | `Succeeded` sowie
   NotReady-Nodes.
8. RBAC: drei ServiceAccounts (`-controller`, `-node`, `-recovery`) mit
   Least Privilege. Der Controller braucht cluster-weites pods delete
   (Workload-Eviction beim Failover) — das ist beabsichtigt. Keine
   `namespaces`-Rechte nötig (Namespace kommt aus kustomize).

## Deployment-Fallen

- Der Release-Tag (`vX.Y.Z`) ist auf ghcr **mutable** und die Manifeste
  nutzen `imagePullPolicy: IfNotPresent` → nach einem Re-Tag das alte Image
  auf den Nodes löschen (`podman rmi …`), sonst läuft der alte Digest weiter.
- ghcr-Packages überleben das Löschen des GitHub-Repos. Ein Package, das an
  ein gelöschtes Repo gebunden war, verweigert dem neuen Repo den Push
  (403) → Package löschen oder Actions-Access im Web-UI vergeben. Neue
  Packages sind default privat → Visibility manuell auf public stellen.
- Test-Cluster des Maintainers: kubectl-Kontext `rhein`, 3× arm64
  (k3s + **CRI-O** — Image-Storage ist mit podman geteilt; es gibt kein
  `k3s ctr`).
- `NFS_ALLOWED_CLIENTS` default RFC1918: Cluster, die über öffentliche
  Node-IPs mounten, brauchen eine angepasste Liste, sonst schlagen Mounts
  fehl.

## Git

Commits, Pushes, Tags und Releases macht der Maintainer selbst — Agents
bereiten Änderungen nur im Working Tree vor.

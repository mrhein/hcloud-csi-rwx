# hcloud-csi-rwx

RWX (ReadWriteMany) volume support for Hetzner Cloud CSI — built in Rust, powered by NFS-Ganesha V12.0.

This project extends the Hetzner Cloud block storage (RWO only) with RWX capability using the same architecture as [Longhorn's share-manager](https://longhorn.io/docs/advanced-resources/rwx-workloads/): per RWX volume one share-manager pod mounts the block device and exports it via NFSv4. A dedicated recovery backend preserves NFSv4 client state across failovers.

> **Project status**: v0.1.x — young project, in production use on the author's
> 3-node arm64 k3s cluster, but interfaces and defaults may still change.
> Review the [Security](#security) section before exposing it to untrusted
> networks. Not affiliated with or endorsed by Hetzner.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Kubernetes Cluster                          │
│                                                                     │
│  ┌───────────────────────┐    ┌──────────────────┐                  │
│  │  CSI Controller        │    │  CSI Node Plugin  │  (DaemonSet)   │
│  │  (csi-provisioner      │    │  (csi-node-       │                │
│  │   csi-attacher         │    │   driver-registrar│                │
│  │   + our CSI gRPC       │    │   + our CSI gRPC) │                │
│  │   + failover controller)│   └────────┬──────────┘                │
│  └────────┬───────────────┘             │ NodePublishVolume         │
│           │ CreateVolume                │ (mount -t nfs4)           │
│           ▼                             │                           │
│  ┌─────────────────────────────────────────────────────────┐       │
│  │  hcloud-csi-rwx CSI Driver (gRPC)                        │       │
│  │  Creates: backing PVC + share-manager pod + service      │       │
│  └─────────────────────────────────────────────────────────┘       │
│           │                                                         │
│           ▼                                                         │
│  ┌─────────────────────┐    ┌──────────────────────────────┐        │
│  │  Share-Manager Pod  │    │  Recovery Backend (2x)       │        │
│  │  (hostNetwork,       │    │  HTTP API → ConfigMap store  │        │
│  │   privileged)       │    │  ganesha talks to this       │        │
│  │                      │    │  via `hcloud` recovery       │        │
│  │  hcloud block volume │    │  backend for NFSv4 lock     │        │
│  │  mounted at /export  │───▶│  state preservation         │        │
│  │  ganesha.nfsd V12.0  │    └──────────────────────────────┘        │
│  │  exports via NFSv4   │                                          │
│  └──────────┬──────────┘                                          │
│             │ NFSv4 :2049 (node IP)                                │
│             ▼                                                      │
│  ┌──────────────────────────────────────────┐                      │
│  │  Workload Pods (1..N on any node)        │                      │
│  │  mount -t nfs4 <node-ip>:/<volume>       │                      │
│  └──────────────────────────────────────────┘                      │
└─────────────────────────────────────────────────────────────────────┘
```

### Per-Volume Lifecycle

1. User creates a PVC with `accessModes: [ReadWriteMany]` and `storageClassName: hcloud-csi-rwx`
2. CSI driver's `CreateVolume` provisions:
   - A backing RWO PVC (`hcloud-volumes` storage class = hcloud block volume,
     configurable via the `backingStorageClass` StorageClass parameter)
   - A share-manager pod (hostNetwork, privileged) on a selected node
   - A ClusterIP service for NFS and health endpoints
3. The share-manager waits for the block volume to attach, opens up the export
   root (mode `EXPORT_MODE`, default 0777), and starts ganesha.nfsd
4. CSI driver returns the NFS endpoint in `volume_context`
5. CSI node plugin's `NodePublishVolume` asks the share-manager service for
   the **current** NFS endpoint (PV specs are immutable, so this is how mounts
   stay correct across failovers) and mounts `nfs4://<node-ip>/<volume-id>`
   into the workload pod

### Failover

The `failover-controller` container (part of the CSI controller deployment)
watches every share-manager pod. When the pod fails, vanishes, or its node
goes NotReady:

1. Controller force-deletes `VolumeAttachment` objects for the backing PVC
2. Controller evicts workload pods using the RWX claim (breaks stale NFS mounts)
3. Controller waits for the hcloud volume to detach
4. Controller selects a **different** node (tracked via `prior_nodes` in the
   volume's state ConfigMap; resets automatically once all nodes were tried)
5. New share-manager pod starts on the new node
6. Recovery backend provides NFSv4 client state so clients can reclaim locks
   during the grace period
7. Evicted workload pods restart; on re-mount the CSI node plugin resolves the
   new endpoint via the share-manager service

### Tuning

These environment variables are set on the **csi-controller deployment**
(both the `csi-driver` and `failover-controller` containers) and are forwarded
into every share-manager pod it creates:

| Env Var | Default | Description |
|---------|---------|-------------|
| `LEASE_LIFETIME` | 60 | NFSv4 lease lifetime in seconds |
| `GRACE_PERIOD` | 90 | NFSv4 grace period in seconds (clients reclaim locks during this window) |
| `NFS_ALLOWED_CLIENTS` | RFC1918 CIDRs | Comma-separated CIDRs allowed to mount exports (`*` = everyone) |
| `EXPORT_MODE` | 0777 | Permissions applied to the export root |
| `SHARE_MANAGER_IMAGE` | released ghcr.io image | Image for share-manager pods |
| `SHARE_MANAGER_PULL_POLICY` | IfNotPresent | imagePullPolicy for share-manager pods |
| `BACKING_STORAGE_CLASS` | hcloud-volumes | Default backing StorageClass (per-SC override: `backingStorageClass` parameter) |

Shorter lease/grace values = faster failover but less time for clients to
reclaim locks. Follows the [Longhorn tuning guide](https://www.suse.com/support/kb/doc/?id=000019374).

## Installation

### Prerequisites

- Kubernetes cluster (>= 1.28) with hcloud-csi driver installed
- The `hcloud-volumes` StorageClass must exist (default from hcloud-csi Helm chart)
- Nodes must have `nfs` and `nfs4` kernel modules (`modprobe nfs4`)
- `kubectl` and `kustomize` (or `kubectl kustomize`)
- **Firewall**: if your nodes have public IPs, block inbound TCP :2049 (NFS),
  :9500 (share-manager API), and :9503 (recovery backend) from outside the
  cluster — see [Security](#security)

### Option A: Install a release (recommended)

The container image is built automatically via GitHub Actions for
`linux/amd64` and `linux/arm64`:

```bash
# Pinned to the v0.1.2 release (image ghcr.io/mrhein/hcloud-csi-rwx:v0.1.2)
kubectl apply -k "https://github.com/mrhein/hcloud-csi-rwx.git/k8s/base?ref=v0.1.2"
```

### Option B: Install from a local checkout

```bash
git clone --branch v0.1.2 https://github.com/mrhein/hcloud-csi-rwx.git
cd hcloud-csi-rwx
kubectl apply -k k8s/base
```

### Verify installation

```bash
# CSI driver registered
kubectl get csidriver hcloud-csi-rwx

# StorageClass created
kubectl get sc hcloud-csi-rwx

# Controller + node plugin + recovery backend running
kubectl -n hcloud-csi-rwx get pods
```

Expected output:
```
NAME                                             READY   STATUS    RESTARTS   AGE
hcloud-csi-rwx-csi-controller-xxxx               4/4     Running   0          1m
hcloud-csi-rwx-csi-node-xxxx                     2/2     Running   0          1m
hcloud-csi-rwx-csi-node-yyyy                     2/2     Running   0          1m
hcloud-csi-rwx-csi-node-zzzz                     2/2     Running   0          1m
hcloud-csi-rwx-recovery-backend-xxxx             1/1     Running   0          1m
hcloud-csi-rwx-recovery-backend-yyyy             1/1     Running   0          1m
```

### Create an RWX Volume

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: my-rwx-volume
spec:
  accessModes:
    - ReadWriteMany
  storageClassName: hcloud-csi-rwx
  resources:
    requests:
      storage: 10Gi
```

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: my-app
spec:
  replicas: 3
  selector:
    matchLabels:
      app: my-app
  template:
    metadata:
      labels:
        app: my-app
    spec:
      containers:
        - name: app
          image: nginx
          volumeMounts:
            - mountPath: /data
              name: data
      volumes:
        - name: data
          persistentVolumeClaim:
            claimName: my-rwx-volume
```

### Uninstall

> **Warning — data loss**: the StorageClass uses `reclaimPolicy: Delete`.
> Deleting RWX PVCs (or uninstalling while PVCs exist) deletes the backing
> hcloud block volumes **including all data**. Migrate or back up data first,
> and delete workload pods + PVCs before removing the driver.

```bash
kubectl delete -k k8s/base
# Or from remote:
kubectl delete -k "https://github.com/mrhein/hcloud-csi-rwx.git/k8s/base?ref=v0.1.2"
```

## Security

Read this before running the driver on nodes with public IPs.

- **NFS runs unencrypted with AUTH_SYS** (no Kerberos, no TLS). Anyone who
  can reach TCP :2049 on a node and is inside the allowed CIDRs can mount the
  export and read/write as any UID (`Squash = None`).
- **Client restriction**: exports are limited to RFC1918 networks by default
  (`NFS_ALLOWED_CLIENTS`, ganesha `CLIENT` block). If your cluster mounts via
  public node IPs you must widen this list — and then a firewall is
  mandatory. `NFS_ALLOWED_CLIENTS="*"` disables the restriction entirely.
- **Firewall**: block TCP :2049 (NFS), :9500 (share-manager status API, read
  only but unauthenticated), and :9503 (recovery backend) from outside the
  cluster, e.g. with Hetzner Cloud Firewalls. Share-manager pods use
  `hostNetwork`, so these ports are open on the node itself.
- **Recovery backend auth** (recommended): create a token secret — the API
  then requires `Authorization: Bearer <token>`, and share-manager pods send
  it automatically:

  ```bash
  kubectl -n hcloud-csi-rwx create secret generic hcloud-csi-rwx-recovery-token \
    --from-literal=token="$(openssl rand -hex 32)"
  kubectl -n hcloud-csi-rwx rollout restart deploy/hcloud-csi-rwx-recovery-backend
  ```

- **RBAC**: each component has its own ServiceAccount with least privilege
  (see `k8s/base/rbac.yaml`). The controller can delete pods cluster-wide —
  that is required for evicting workload pods during failover.

Report vulnerabilities via [SECURITY.md](SECURITY.md).

## Components

Four Rust binaries, all in a single container image built on SUSE BCI base with ganesha V12.0 compiled from upstream source:

| Binary | Role |
|--------|------|
| `hcloud-csi-rwx` | **Share-manager**: mounts block volume, starts ganesha, health checks, HTTP API on :9500 |
| `hcloud-csi-rwx-controller` | **Failover controller**: watches share-manager pods, recreates them on healthy nodes, evicts stale workloads |
| `hcloud-csi-rwx-recovery-backend` | **Recovery backend**: HTTP API on :9503, stores NFSv4 client state in ConfigMaps |
| `hcloud-csi-rwx-csi` | **CSI gRPC driver**: Identity + Controller + Node services, speaks CSI spec v1 |

### ganesha Configuration

The ganesha config is generated at runtime (see `src/nfs.rs`, based on
Longhorn's `nfs_server.go` template):

```ganesha
NFS_Core_Param {
    Enable_UDP = false;
    fsid_device = false;
    Bind_addr = 0.0.0.0;
    Protocols = 4;
}

LOG {
    Default_Log_Level = INFO;
    Facility { name = FILE; destination = "/proc/1/fd/1"; enable = active; }
}

NFSV4 {
    Lease_Lifetime = 60;
    Grace_Period = 90;
    Minor_Versions = 0, 1, 2;
    RecoveryBackend = hcloud;
    Only_Numeric_Owners = true;
}

Export_Defaults {
    Protocols = 4;
    Transports = TCP;
    Access_Type = None;
    SecType = sys;
    Squash = None;
}

EXPORT {
    Export_Id = 1;
    Path = /export;
    Pseudo = /<volume>;
    Protocols = 4;
    Transports = TCP;
    Access_Type = None;
    Squash = None;
    SecType = sys;
    Filesystem_id = 1.0;
    CLIENT {
        Clients = 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16;
        Access_Type = RW;
        Squash = None;
        SecType = sys;
    }
    FSAL { Name = VFS; }
}
```

The `RecoveryBackend = hcloud` is a patched-in recovery backend for
nfs-ganesha V12.0, derived from Longhorn's `recovery_longhorn.c`
(rancher/nfs-ganesha, LGPL-3.0) and adapted for this project — see the header
of `ganesha-patch/recovery_hcloud.c` for the list of modifications. It stores
client state via HTTP in a service named `hcloud-csi-rwx-recovery-backend` on
port 9503 (URL overridable via `HCLOUD_RECOVERY_BACKEND_URL`).

### ganesha Build

ganesha is built from upstream `nfs-ganesha/nfs-ganesha` V12.0 (not the rancher fork), commit-pinned:

- `ganesha-patch/hcloud-ganesha.patch` — unified diff wiring the `hcloud` recovery backend into 7 upstream files (enum, CMakeLists, config parser, init dispatch)
- `ganesha-patch/recovery_hcloud.c` — the recovery backend implementation (LGPL-3.0-or-later, derived from Longhorn's `recovery_longhorn.c`)
- `ganesha-patch/build-ganesha.sh` — clones upstream V12.0 + ntirpc v10.0 + libkmip (all commit-pinned), applies the patch, builds VFS-only

## Benchmark Results

fio benchmarks on a 3-node arm64 cluster (Hetzner Cloud, openSUSE MicroOS,
k3s v1.36), v0.1.0 image (NFS-Ganesha V12.0 built from upstream).
Measured 2026-07-24.

### Test Setup

- **RWO baseline**: direct hcloud block volume (ext4, no NFS)
- **RWX N pods**: N concurrent NFS clients on different nodes (aggregate
  throughput), mounted directly against the share-manager node IP — one of
  the clients runs on the share-manager's own node
- Each client runs:
  `fio --name=bench --rw=<mode> --bs=<1M|4k> --size=256M --time_based --runtime=10 --filename=/data/bench-$POD.bin`
  (buffered I/O, psync engine — page-cache effects are part of the measurement,
  as they are in real workloads)

### Results (aggregate MiB/s)

| Setup | Seq Write (1M) | Seq Read (1M) | Rand Write (4k) | Rand Read (4k) |
|-------|:--:|:--:|:--:|:--:|
| RWO baseline (direct block) | 794 | 2077 | 221 | 18 |
| RWX 1 pod (NFS) | 297 | 1606 | 223 | 30 |
| RWX 2 pods total | 345 | 2016 | 295 | 35 |
| RWX 3 pods total | 381 | 2306 | 300 | 41 |

### Analysis

- **Reads scale with client count** and approach or exceed the direct-block
  baseline — the NFS client page cache and ganesha's MDCache do a lot of the
  work for a 256M working set.
- **Sequential writes cost roughly 2–2.5x** vs. direct block access; the NFS
  round trips and write ordering dominate. Aggregate write throughput still
  grows with more clients.
- **Random 4k writes** over NFS match the direct block baseline thanks to
  client-side write coalescing; random 4k reads benefit from read-ahead.
- Numbers are aggregate throughput on this specific setup — treat them as a
  ballpark, not as universal.

## Limitations

- **One share-manager per node**: with `hostNetwork: true`, ganesha binds port 2049 on the node. Only one share-manager can run per node (same limitation as Longhorn). The node picker refuses nodes that already host one.
- **Failover requires volume detach**: hcloud volumes must detach from the old node before re-attaching to a new one. This takes 10-30s depending on hcloud API response time.
- **Grace period disruption**: during the NFSv4 grace period after failover, I/O operations will block until clients reclaim their locks or the grace period expires (default 90s).
- **No encryption**: neither the NFS traffic (AUTH_SYS only, no krb5/TLS) nor the volumes (no LUKS, unlike Longhorn).
- **No volume expansion or snapshots** yet — `allowVolumeExpansion` is not supported despite hcloud volumes supporting resize.

## Development

Local build requirements: Rust (see `Cargo.toml` for edition), `protoc`
(protobuf compiler — needed by `build.rs` for the CSI spec codegen).

```bash
cargo test          # unit tests
cargo clippy --all-targets
cargo build --release
```

Container image (multi-arch capable, ganesha stage takes a while):

```bash
podman build -t localhost/hcloud-csi-rwx:latest -f Containerfile .
```

For deploying a locally-built image there is a development overlay
(local image + example PVC) — pre-load the image onto every node, then:

```bash
kubectl apply -k k8s/overlays/test
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## Project Structure

```
hcloud-csi-rwx/
├── Cargo.toml              # One package, four [[bin]] targets
├── Containerfile           # 3-stage build: Rust (cross) + ganesha (QEMU) + runtime
├── build.rs                # Protobuf codegen for CSI spec (needs protoc)
├── proto/csi.proto         # Container Storage Interface spec v1 (Apache-2.0)
├── LICENSE                 # Apache-2.0 (this project)
├── LICENSES/               # LGPL-3.0 + GPL-3.0 texts (patched ganesha)
├── NOTICE                  # Third-party attributions
├── k8s/
│   ├── base/               # Kustomize base (namespace, RBAC, deployments, storageclass)
│   └── overlays/
│       └── test/           # Development overlay (local image + example PVC)
├── .github/workflows/
│   └── build.yml           # CI: tests + clippy, multi-arch (amd64+arm64) container build
├── ganesha-patch/
│   ├── build-ganesha.sh    # Clone upstream V12.0 (pinned) + apply patch + build
│   ├── hcloud-ganesha.patch  # Unified diff: 7 files wiring in the hcloud recovery backend
│   └── recovery_hcloud.c  # Recovery backend (LGPL-3.0, derived from Longhorn's)
└── src/
    ├── main.rs             # share-manager binary (block mount + ganesha + health)
    ├── api.rs              # HTTP API (health + endpoint discovery)
    ├── block.rs            # Block device detection, mkfs, mount/unmount
    ├── nfs.rs              # Ganesha config generation + process management
    ├── provision.rs        # Shared pod/service/PVC specs + config (csi + controller)
    └── bin/
        ├── controller.rs         # Failover controller (pod watch + recreate + evict)
        ├── recovery_backend.rs   # NFSv4 recovery backend HTTP API
        └── csi.rs               # CSI gRPC driver (Identity/Controller/Node)
```

## License

Apache-2.0 — see [LICENSE](LICENSE) for details.

The patched NFS-Ganesha components under `ganesha-patch/` are
LGPL-3.0-or-later; the full LGPL-3.0 and GPL-3.0 texts are in
[LICENSES/](LICENSES/). Third-party software licenses and attributions are
listed in [NOTICE](NOTICE).

This project includes:
- **NFS-Ganesha V12.0** (upstream, LGPL-3.0-or-later) — built from source with our patch
- **Longhorn recovery backend** (rancher/nfs-ganesha, LGPL-3.0-or-later) — basis of `recovery_hcloud.c`
- **ntirpc v10.0** (BSD-3-Clause) — built from source
- **libkmip** (ceph fork, Apache-2.0 OR BSD) — built from source
- **CSI spec** (Apache-2.0) — protobuf definitions
- **Longhorn** design patterns (Apache-2.0) — architecture reference
- **Rust crates** (MIT / Apache-2.0 / permissive) — see NOTICE

"Hetzner" is a trademark of Hetzner Online GmbH. This is an independent
community project, not affiliated with or endorsed by Hetzner.

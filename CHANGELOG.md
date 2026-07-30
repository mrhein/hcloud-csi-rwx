# Changelog

All notable changes to this project will be documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

## [0.1.2] - 2026-07-30

Restores fixes that were lost when v0.1.1 was built on a reset working tree.
Everything in 0.1.1 is included; upgrade directly from 0.1.0 or 0.1.1.

### Fixed

- Share-manager reported the **service name** instead of the node IP as its
  NFS endpoint (`--svc-ip <service>`), so workload pods scheduled onto the
  share-manager's own node could never mount. The endpoint now comes from the
  downward API (`status.podIP`, which equals the node IP under `hostNetwork`).
- Share-manager exited with code 0 after ganesha died, leaving the pod in
  phase `Succeeded`; it now exits non-zero so the pod ends up `Failed`.
- Failover controller ignored pods in phase `Succeeded`, so a dead
  share-manager was never replaced (`restartPolicy: Never`).
- `mount`/`umount` in NodePublishVolume ran synchronously without a timeout —
  a hard NFS mount against a dead server pinned tokio workers until the node
  plugin's gRPC server stopped answering. Both now run async with hard
  timeouts and `kill_on_drop`.
- The ganesha recovery client sent request bodies with a trailing NUL byte
  (`strlen(payload) + 1`); it now sends the exact length. The backend
  additionally tolerates a trailing NUL for older ganesha builds.

### Changed

- `AGENTS.md` (agent/contributor quick reference) restored.
- README benchmarks replaced with numbers measured on v0.1.0 (2026-07-30
  methodology section, no stale comparison table).

## [0.1.1] - 2026-07-30

### Fixed

- Recovery backend returned HTTP 400 for `add_clid`, `end_grace`, and
  `add_revoke_fh`: ganesha sends only `{"version": …}` in the body and passes
  the hostname as a URL path parameter, but `hostname` was a required body
  field. It is now optional (still required for `POST /v1/recoverybackend`,
  which has no path parameter).
- NodePublishVolume no longer falls back to the `volume_context` endpoint
  captured at provisioning time — during a failover that points at the dead
  node. It fails fast with `Unavailable` so kubelet retries.

## [0.1.0] - 2026-07-23

Initial release.

- RWX (ReadWriteMany) volumes for Hetzner Cloud: per volume one share-manager
  pod exports a backing hcloud block volume via NFSv4 (NFS-Ganesha V12.0,
  built from upstream with a custom `hcloud` recovery backend derived from
  Longhorn's).
- CSI driver (Identity/Controller/Node), csi-provisioner + csi-attacher
  sidecars, kustomize manifests.
- Failover controller: recreates share-managers on healthy nodes, force-
  detaches hcloud volumes, evicts workload pods for clean re-mounts; the node
  plugin resolves the current NFS endpoint at mount time.
- NFSv4 recovery backend storing client state in ConfigMaps (grace-period
  lock reclaim across failovers), optional bearer-token auth.
- NFS exports restricted to configurable client CIDRs (default RFC1918).
- Multi-arch container image (amd64 + arm64) on SUSE BCI, published to
  ghcr.io via GitHub Actions.

[Unreleased]: https://github.com/mrhein/hcloud-csi-rwx/compare/v0.1.2...HEAD
[0.1.2]: https://github.com/mrhein/hcloud-csi-rwx/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/mrhein/hcloud-csi-rwx/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/mrhein/hcloud-csi-rwx/releases/tag/v0.1.0

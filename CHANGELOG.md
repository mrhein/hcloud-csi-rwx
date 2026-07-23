# Changelog

All notable changes to this project will be documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

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

[Unreleased]: https://github.com/mrhein/hcloud-csi-rwx/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/mrhein/hcloud-csi-rwx/releases/tag/v0.1.0

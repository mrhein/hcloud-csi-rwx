# Contributing

Thanks for your interest in hcloud-csi-rwx!

## Development setup

- Rust toolchain (stable; the crate uses edition 2024)
- `protoc` (protobuf compiler) — required by `build.rs` for CSI codegen
- `kubectl` with kustomize for manifest work
- podman or docker if you want to build the container image

```bash
cargo test                        # unit tests
cargo clippy --all-targets        # must be warning-free (CI enforces -D warnings)
kubectl kustomize k8s/base        # manifests must render
```

## Changes to the ganesha patch

`ganesha-patch/recovery_hcloud.c` is LGPL-3.0-or-later (derived from
Longhorn's `recovery_longhorn.c`) — keep the license header and the
modification notes up to date. `hcloud-ganesha.patch` must apply cleanly to
upstream nfs-ganesha V12.0 (`git apply --check`); `build-ganesha.sh` pins the
exact commits.

## Pull requests

- Keep PRs focused; describe the behavior change and how you tested it.
- New behavior in `src/` should come with a unit test where practical.
- The share-manager pod spec lives in `src/provision.rs` and is shared by the
  CSI driver and the failover controller — change it there only.
- Testing on a real Hetzner Cloud cluster is gold; note in the PR if you did.

## License of contributions

Contributions to the Rust code are accepted under Apache-2.0. Contributions
to `ganesha-patch/` are accepted under LGPL-3.0-or-later.

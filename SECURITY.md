# Security Policy

## Supported Versions

Only the latest release receives security fixes.

## Reporting a Vulnerability

Please report vulnerabilities privately via
[GitHub Security Advisories](https://github.com/mrhein/hcloud-csi-rwx/security/advisories/new)
(preferred) or by email to <mathias@rhein.io>. Do not open public issues for
security problems.

You can expect an initial response within a week. Please include reproduction
steps and affected versions.

## Security Model (read before deploying)

- NFS traffic is **unencrypted** and authenticated only by AUTH_SYS
  (client-asserted UIDs, `Squash = None`). Anyone who can reach TCP :2049 on
  a node from within `NFS_ALLOWED_CLIENTS` (default: RFC1918 ranges) has full
  read/write access to the export.
- Share-manager pods run privileged with `hostNetwork` — ports :2049 (NFS)
  and :9500 (status API) are bound on the node. The recovery backend listens
  on :9503 behind a ClusterIP service.
- On nodes with public IPs, an external firewall (e.g. Hetzner Cloud
  Firewall) blocking :2049/:9500/:9503 from outside the cluster is mandatory.
- The recovery backend supports optional bearer-token authentication: create
  the `hcloud-csi-rwx-recovery-token` Secret (key `token`) in the
  `hcloud-csi-rwx` namespace (see README → Security).

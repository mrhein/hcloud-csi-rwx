# WordPress on RWX — the classic shared-webroot setup

Three WordPress replicas, one per node, all serving the **same**
`/var/www/html` from a `hcloud-csi-rwx` volume, with MariaDB on a normal
`hcloud-volumes` block device. Uploading an image or installing a plugin on
one replica makes it appear on the other two — that is the RWX property this
driver exists for.

## Deploy

Secrets are generated locally and never committed:

```bash
kubectl apply -f 00-namespace.yaml

kubectl -n wordpress create secret generic wordpress-db \
  --from-literal=password="$(openssl rand -base64 24)" \
  --from-literal=root-password="$(openssl rand -base64 24)"

kubectl -n wordpress create secret generic wordpress-salts \
  $(for k in auth-key secure-auth-key logged-in-key nonce-key \
             auth-salt secure-auth-salt logged-in-salt nonce-salt; do
      printf -- "--from-literal=%s=%s " "$k" "$(openssl rand -base64 48)"
    done)

kubectl apply -f 10-storage.yaml -f 20-mariadb.yaml -f 30-wordpress.yaml -f 40-networkpolicy.yaml
```

Adjust the host in `30-wordpress.yaml` (`WP_HOME`, `WP_SITEURL`, the Ingress
rule and the TLS entry) and the `ingressClassName` / cluster-issuer to match
your cluster.

## What is hardened

- **Non-root everywhere.** Apache listens on 8080 as `www-data` instead of
  binding 80 as root; the namespace enforces the `restricted` Pod Security
  Standard. Both containers run with `readOnlyRootFilesystem`, all
  capabilities dropped, no privilege escalation and `RuntimeDefault` seccomp.
- **Secrets are generated, never checked in** — DB passwords plus all eight
  WordPress salts. Without shared salts, replicas would invalidate each
  other's sessions.
- **NetworkPolicy default-deny.** The web tier accepts traffic only from the
  ingress-controller namespace; the database only from the web tier.
  Outbound is limited to DNS, the database, and public HTTP/S for updates —
  RFC1918 egress is explicitly excluded, so a compromised WordPress cannot
  reach the rest of the cluster.
- **TLS** via cert-manager, `FORCE_SSL_ADMIN`, and `X-Forwarded-Proto`
  handling so WordPress knows it is behind a terminating proxy.
- **`DISALLOW_FILE_EDIT`** removes the admin code editor. Plugin and theme
  *installs* stay enabled on purpose — that is the RWX demonstration.
- Apache refuses to serve `wp-config.php`, dotfiles, `*.sql` and `*.bak`
  whatever ends up in the shared volume, and directory indexes are off.
- Resource requests/limits on every container; `Recreate` strategy on the
  database so two writers can never touch the RWO volume.

## Notes

- The shared webroot is seeded by an init container guarded by an atomic
  `mkdir` lock, so three replicas starting simultaneously cannot corrupt it.
- WordPress writes uploads to the shared volume, which the driver exposes
  with `Squash = None`; keep the NFS ports firewalled (see the main
  [SECURITY.md](../../SECURITY.md)).
- This is a demo workload. For production add backups of both volumes, an
  object-storage offload for uploads, and a WAF in front.

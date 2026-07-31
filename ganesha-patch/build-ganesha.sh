#!/bin/bash
set -euo pipefail

# Build nfs-ganesha from upstream with our hcloud-csi-rwx recovery backend.
#
# We apply a clean .patch file (hcloud-ganesha.patch) that wires the hcloud
# recovery backend into ganesha's enum, CMakeLists, config parser, and init
# dispatch (7 files). The recovery backend implementation itself
# (recovery_hcloud.c, derived from Longhorn's recovery_longhorn.c, LGPL-3.0)
# is copied in as a new file.
#
# Dependency pinning: we clone the ganesha tag and let ganesha's own submodule
# pins decide the ntirpc / libkmip / prometheus-cpp-lite revisions. That way a
# single upstream tag determines the whole tree, and we cannot drift from a
# combination upstream never tested. The expected submodule commits are
# asserted below so an unnoticed upstream re-pin fails the build loudly.

GANESHA_TAG="V13.0"
GANESHA_COMMIT="429463bc77a4654a4f00e0109b8c1496c272abb4"
# Submodule commits as pinned by $GANESHA_TAG (verified, not chosen by us):
NTIRPC_COMMIT="96e980def1c2d4538ff4708b0908670dd6a8946d"      # == ntirpc v10.0
LIBKMIP_COMMIT="4f553ecaf8e57cc3019222b8551d17888f0a1e66"
PROMETHEUS_CPP_LITE_COMMIT="48d09c45ee6deb90e02579b03037740e1c01fd27"

PATCH_DIR="$(cd "$(dirname "$0")" && pwd)"
SRC_DIR="/tmp/nfs-ganesha"

verify_commit() {
	local dir="$1" expected="$2" name="$3"
	local actual
	actual="$(git -C "$dir" rev-parse HEAD)"
	if [ "$actual" != "$expected" ]; then
		echo "ERROR: $name is at $actual, expected $expected" >&2
		exit 1
	fi
	echo "  ok: $name @ $expected"
}

echo "=== Cloning upstream nfs-ganesha $GANESHA_TAG ==="
git clone --depth 1 --branch "$GANESHA_TAG" https://github.com/nfs-ganesha/nfs-ganesha.git "$SRC_DIR"
cd "$SRC_DIR"

echo "=== Fetching submodules at the revisions $GANESHA_TAG pins ==="
git submodule update --init --recursive

echo "=== Verifying pinned revisions ==="
verify_commit "$SRC_DIR" "$GANESHA_COMMIT" "nfs-ganesha $GANESHA_TAG"
verify_commit "$SRC_DIR/src/libntirpc" "$NTIRPC_COMMIT" "ntirpc"
verify_commit "$SRC_DIR/src/libkmip" "$LIBKMIP_COMMIT" "libkmip"
verify_commit "$SRC_DIR/src/libntirpc/src/monitoring/prometheus-cpp-lite" \
	"$PROMETHEUS_CPP_LITE_COMMIT" "prometheus-cpp-lite"

echo "=== Applying hcloud-csi-rwx recovery backend patch ==="
git apply "$PATCH_DIR/hcloud-ganesha.patch"
cp "$PATCH_DIR/recovery_hcloud.c" src/SAL/recovery/recovery_hcloud.c

echo "=== Verifying patch applied ==="
echo "  gsh_recovery.h: $(grep -c 'RECOVERY_BACKEND_HCLOUD' src/include/gsh_recovery.h) (expect 1)"
echo "  sal_functions.h: $(grep -c 'hcloud_backend_init' src/include/sal_functions.h) (expect 1)"
echo "  nfs4_recovery.c: $(grep -c 'RECOVERY_BACKEND_HCLOUD' src/SAL/nfs4_recovery.c) (expect 3)"
echo "  nfs_read_conf.c: $(grep -c 'hcloud' src/support/nfs_read_conf.c) (expect 1)"
echo "  SAL/CMakeLists.txt: $(grep -c 'recovery_hcloud' src/SAL/CMakeLists.txt) (expect 1)"
echo "  CMakeLists.txt: $(grep -c 'LIBCURL_LIB' src/CMakeLists.txt) (expect 2)"

# USE_MONITORING builds the Prometheus exposer (src/monitoring). Its only
# dependency, prometheus-cpp-lite, ships as a submodule of ntirpc, so this
# costs no extra packages. Metrics stay off at runtime unless the generated
# ganesha.conf sets Enable_Metrics (see src/nfs.rs).
echo "=== Building nfs-ganesha $GANESHA_TAG (VFS-only, hcloud recovery, monitoring) ==="
export CC="${CC:-/usr/bin/gcc-14}" CXX="${CXX:-/usr/bin/g++-14}"
mkdir -p /usr/local
cmake -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
      -DCMAKE_BUILD_TYPE=Release -DBUILD_CONFIG=vfs_only \
      -DUSE_DBUS=OFF -DUSE_NLM=OFF -DUSE_RQUOTA=OFF -DUSE_9P=OFF -D_MSPAC_SUPPORT=OFF -DRPCBIND=OFF \
      -DUSE_RADOS_RECOV=OFF -DRADOS_URLS=OFF -DUSE_FSAL_VFS=ON -DUSE_FSAL_XFS=OFF \
      -DUSE_FSAL_PROXY_V4=OFF -DUSE_FSAL_PROXY_V3=OFF -DUSE_FSAL_LUSTRE=OFF -DUSE_FSAL_LIZARDFS=OFF \
      -DUSE_FSAL_KVSFS=OFF -DUSE_FSAL_CEPH=OFF -DUSE_FSAL_GPFS=OFF -DUSE_FSAL_PANFS=OFF -DUSE_FSAL_GLUSTER=OFF \
      -DUSE_GSS=NO -DHAVE_ACL_GET_FD_NP=ON -DHAVE_ACL_SET_FD_NP=ON \
      -DUSE_MONITORING=ON \
      -DCMAKE_INSTALL_PREFIX=/usr/local src/

make -j$(nproc)
make install

mkdir -p /ganesha-extra/etc/dbus-1/system.d
cp src/scripts/ganeshactl/org.ganesha.nfsd.conf /ganesha-extra/etc/dbus-1/system.d/ 2>/dev/null || true

echo "=== ganesha $GANESHA_TAG with hcloud recovery backend built ==="

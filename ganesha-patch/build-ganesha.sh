#!/bin/bash
set -euo pipefail

# Build nfs-ganesha V12.0 from upstream with our hcloud-csi-rwx recovery backend.
#
# We apply a clean .patch file (hcloud-ganesha.patch) that wires the hcloud
# recovery backend into ganesha's enum, CMakeLists, config parser, and init
# dispatch (7 files). The recovery backend implementation itself
# (recovery_hcloud.c, derived from Longhorn's recovery_longhorn.c, LGPL-3.0)
# is copied in as a new file.
#
# No sed hacks — just git apply + one new file.

GANESHA_TAG="V12.0"
GANESHA_COMMIT="8e157ac8db7aa7c69f7e1d9c6b4446cc84d62699"
NTIRPC_TAG="v10.0"
NTIRPC_COMMIT="96e980def1c2d4538ff4708b0908670dd6a8946d"
LIBKMIP_COMMIT="4f553ecaf8e57cc3019222b8551d17888f0a1e66"
PATCH_DIR="$(cd "$(dirname "$0")" && pwd)"
SRC_DIR="/tmp/nfs-ganesha"

verify_commit() {
    local dir="$1" expected="$2" name="$3"
    local actual
    actual="$(git -C "$dir" rev-parse HEAD)"
    if [ "$actual" != "$expected" ]; then
        echo "ERROR: $name HEAD is $actual, expected $expected" >&2
        exit 1
    fi
}

echo "=== Cloning upstream nfs-ganesha $GANESHA_TAG ==="
git clone --depth 1 --branch "$GANESHA_TAG" https://github.com/nfs-ganesha/nfs-ganesha.git "$SRC_DIR"
verify_commit "$SRC_DIR" "$GANESHA_COMMIT" "nfs-ganesha $GANESHA_TAG"
cd "$SRC_DIR"

echo "=== Cloning ntirpc $NTIRPC_TAG ==="
rm -rf src/libntirpc
git clone --depth 1 --branch "$NTIRPC_TAG" https://github.com/nfs-ganesha/ntirpc.git src/libntirpc
verify_commit src/libntirpc "$NTIRPC_COMMIT" "ntirpc $NTIRPC_TAG"
cd src/libntirpc
git submodule update --init --recursive
cd "$SRC_DIR"

echo "=== Cloning libkmip (new in V12.0, pinned commit) ==="
git clone https://github.com/ceph/libkmip.git src/libkmip
git -C src/libkmip checkout --quiet "$LIBKMIP_COMMIT"

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

echo "=== Building nfs-ganesha V12.0 (VFS-only, hcloud recovery) ==="
export CC="${CC:-/usr/bin/gcc-14}" CXX="${CXX:-/usr/bin/g++-14}"
mkdir -p /usr/local
cmake -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
      -DCMAKE_BUILD_TYPE=Release -DBUILD_CONFIG=vfs_only \
      -DUSE_DBUS=OFF -DUSE_NLM=OFF -DUSE_RQUOTA=OFF -DUSE_9P=OFF -D_MSPAC_SUPPORT=OFF -DRPCBIND=OFF \
      -DUSE_RADOS_RECOV=OFF -DRADOS_URLS=OFF -DUSE_FSAL_VFS=ON -DUSE_FSAL_XFS=OFF \
      -DUSE_FSAL_PROXY_V4=OFF -DUSE_FSAL_PROXY_V3=OFF -DUSE_FSAL_LUSTRE=OFF -DUSE_FSAL_LIZARDFS=OFF \
      -DUSE_FSAL_KVSFS=OFF -DUSE_FSAL_CEPH=OFF -DUSE_FSAL_GPFS=OFF -DUSE_FSAL_PANFS=OFF -DUSE_FSAL_GLUSTER=OFF \
      -DUSE_GSS=NO -DHAVE_ACL_GET_FD_NP=ON -DHAVE_ACL_SET_FD_NP=ON \
      -DUSE_MONITORING=OFF \
      -DCMAKE_INSTALL_PREFIX=/usr/local src/

make -j$(nproc)
make install

mkdir -p /ganesha-extra/etc/dbus-1/system.d
cp src/scripts/ganeshactl/org.ganesha.nfsd.conf /ganesha-extra/etc/dbus-1/system.d/ 2>/dev/null || true

echo "=== ganesha V12.0 with hcloud recovery backend built ==="

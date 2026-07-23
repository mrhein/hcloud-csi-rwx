# syntax=docker/dockerfile:1.25.0

# ── Stage 1: Build Rust binaries (cross-compiled per target arch) ──
# Runs natively on the build host and cross-compiles for $TARGETARCH — the
# ganesha stage below runs under QEMU instead (C toolchain cross-setup is not
# worth it there).
FROM --platform=$BUILDPLATFORM rust:1.97-bookworm AS rust-builder
ARG TARGETARCH
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler libprotobuf-dev && \
    if [ "$TARGETARCH" = "arm64" ] && [ "$(uname -m)" != "aarch64" ]; then \
        apt-get install -y --no-install-recommends gcc-aarch64-linux-gnu libc6-dev-arm64-cross; \
    fi && \
    if [ "$TARGETARCH" = "amd64" ] && [ "$(uname -m)" != "x86_64" ]; then \
        apt-get install -y --no-install-recommends gcc-x86-64-linux-gnu libc6-dev-amd64-cross; \
    fi && \
    rm -rf /var/lib/apt/lists/*
# On Debian these linker names exist natively too (gcc multiarch symlinks),
# so they are safe to set unconditionally.
ENV CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc \
    CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=x86_64-linux-gnu-gcc
COPY Cargo.toml Cargo.lock build.rs ./
COPY proto ./proto
COPY src ./src
RUN case "$TARGETARCH" in \
        amd64) RUST_TARGET=x86_64-unknown-linux-gnu ;; \
        arm64) RUST_TARGET=aarch64-unknown-linux-gnu ;; \
        *) echo "unsupported TARGETARCH=$TARGETARCH" >&2; exit 1 ;; \
    esac && \
    rustup target add "$RUST_TARGET" && \
    cargo build --release --locked --bins --target "$RUST_TARGET" && \
    mkdir -p /out && \
    cp "target/$RUST_TARGET/release/hcloud-csi-rwx" /out/ && \
    cp "target/$RUST_TARGET/release/hcloud-csi-rwx-controller" /out/ && \
    cp "target/$RUST_TARGET/release/hcloud-csi-rwx-recovery-backend" /out/ && \
    cp "target/$RUST_TARGET/release/hcloud-csi-rwx-csi" /out/

# ── Stage 2: Build ganesha from upstream nfs-ganesha/nfs-ganesha + hcloud recovery backend patch ──
FROM registry.suse.com/bci/bci-base:15.7 AS ganesha-builder
ARG TARGETPLATFORM

RUN zypper -n ref && \
    zypper -n install autoconf bison curl git tar gzip jq libcurl-devel libjson-c-devel \
                      libacl-devel libdbus-1-3 liburcu-devel libblkid-devel \
                      e2fsprogs e2fsprogs-devel xfsprogs xfsprogs-devel \
                      dbus-1-devel flex Mesa-libGL-devel nfsidmap-devel \
                      graphviz-devel doxygen libnsl-devel && \
    zypper clean -a

# Add build repos (cmake/make + gcc14 are not in the SLE BCI repos)
RUN for i in {1..10}; do \
        zypper -n addrepo --refresh "https://download.opensuse.org/repositories/devel:libraries:c_c++/openSUSE_Factory/?AVOID_COUNTRY=ru,by" devel:libraries:c_c++.repo; \
        if [ "$TARGETPLATFORM" = "linux/amd64" ]; then \
            zypper -n addrepo --refresh "https://download.opensuse.org/repositories/devel:/tools:/building//openSUSE_Factory/?AVOID_COUNTRY=ru,by" devel:tools:building.repo; \
        elif [ "$TARGETPLATFORM" = "linux/arm64" ]; then \
            zypper -n addrepo --refresh "https://download.opensuse.org/repositories/devel:/tools:/building//openSUSE_Factory_ARM/?AVOID_COUNTRY=ru,by" devel:tools:building.repo; \
        fi && \
        zypper --gpg-auto-import-keys ref && break || sleep 1; \
    done

RUN zypper -n --no-refresh install cmake make

RUN for i in {1..10}; do \
        if [ "$TARGETPLATFORM" = "linux/amd64" ]; then \
            zypper -n addrepo --refresh "https://download.opensuse.org/repositories/devel:gcc/openSUSE_Factory/?AVOID_COUNTRY=ru,by" devel:gcc.repo; \
        elif [ "$TARGETPLATFORM" = "linux/arm64" ]; then \
            zypper -n addrepo --refresh "https://download.opensuse.org/repositories/devel:gcc/openSUSE_Factory_ARM/?AVOID_COUNTRY=ru,by" devel:gcc.repo; \
        fi && \
        zypper --gpg-auto-import-keys ref && break || sleep 1; \
    done

RUN zypper -n --no-refresh install gcc14 gcc14-c++ && zypper clean -a

# Copy our hcloud recovery backend patch
COPY ganesha-patch/ /ganesha-patch/

# Build ganesha from upstream with our patch applied
RUN export CC="/usr/bin/gcc-14" CXX="/usr/bin/g++-14" && \
    bash /ganesha-patch/build-ganesha.sh

# ── Stage 3: Runtime image ──
FROM registry.suse.com/bci/bci-base:15.7 AS runtime
ARG TARGETPLATFORM
ENV ARCH=${TARGETPLATFORM#linux/}

LABEL org.opencontainers.image.title="hcloud-csi-rwx" \
      org.opencontainers.image.description="RWX (ReadWriteMany) volumes for Hetzner Cloud CSI via NFS-Ganesha" \
      org.opencontainers.image.source="https://github.com/mrhein/hcloud-csi-rwx" \
      org.opencontainers.image.licenses="Apache-2.0 AND LGPL-3.0-or-later"

# nfs-client / nfs4-acl-tools are not in the SLE BCI repos; use the versioned
# openSUSE Leap 15.6 repo (same SLE 15 base) instead of a rolling Factory repo.
RUN zypper -n ref && \
    zypper -n install rpcbind hostname libblkid1 libjson-c* dbus-1-x11 dbus-1 \
                       nfsidmap-devel libnsl-devel nfs-kernel-server xfsprogs e2fsprogs && \
    for i in {1..10}; do \
        zypper -n addrepo --refresh "https://download.opensuse.org/distribution/leap/15.6/repo/oss/" leap-oss.repo && \
        zypper --gpg-auto-import-keys ref && break || sleep 1; \
    done && \
    zypper -n install nfs-client nfs4-acl-tools && \
    zypper clean -a

RUN mkdir -p /var/run/dbus /export && \
    echo /usr/local/lib64 > /etc/ld.so.conf.d/local_libs.conf && \
    ([ -f /etc/nsswitch.conf ] && sed -i 's/systemd//g' /etc/nsswitch.conf || true) && \
    ln -sf /proc/self/mounts /etc/mtab

# ganesha from our build
COPY --from=ganesha-builder /usr/local /usr/local/
COPY --from=ganesha-builder /ganesha-extra /

# Rust binaries
COPY --from=rust-builder /out/hcloud-csi-rwx /usr/local/bin/hcloud-csi-rwx
COPY --from=rust-builder /out/hcloud-csi-rwx-controller /usr/local/bin/hcloud-csi-rwx-controller
COPY --from=rust-builder /out/hcloud-csi-rwx-recovery-backend /usr/local/bin/hcloud-csi-rwx-recovery-backend
COPY --from=rust-builder /out/hcloud-csi-rwx-csi /usr/local/bin/hcloud-csi-rwx-csi

# License texts and attributions for everything shipped in this image
# (Apache-2.0 for our code, LGPL-3.0 for the patched ganesha, BSD/MIT deps —
# see NOTICE). Corresponding source: the repository in
# org.opencontainers.image.source (upstream tag + patch + build script).
COPY LICENSE NOTICE /usr/share/licenses/hcloud-csi-rwx/
COPY LICENSES/ /usr/share/licenses/hcloud-csi-rwx/LICENSES/

RUN ldconfig

EXPOSE 2049/tcp 9500/tcp 9503/tcp 9600/tcp

ENTRYPOINT ["hcloud-csi-rwx"]

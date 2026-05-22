# osodevops/kafka-backup, compiled from source onto a Red Hat UBI base.
#
# The upstream image is Debian-bookworm based (glibc 2.36); UBI9 is glibc 2.34 and
# glibc is forward-compatible only, so a bookworm-built binary will not run on UBI9.
# We therefore compile the Rust workspace on a UBI9 builder stage so the binary links
# against UBI9's own glibc. Project policy: every image we build uses a UBI base.
#
# kafka-backup is MIT-licensed (https://github.com/osodevops/kafka-backup).
ARG KAFKA_BACKUP_VERSION=0.15.7

# ---- builder: compile the Rust workspace against UBI9's glibc ----------------
FROM registry.access.redhat.com/ubi9/ubi:latest AS build
ARG KAFKA_BACKUP_VERSION
RUN dnf install -y gcc gcc-c++ make perl pkgconf-pkg-config openssl-devel git \
    && dnf clean all
# Rust via rustup — UBI repos do not reliably carry a recent enough toolchain.
# The `stable` channel is required: recent kafka-backup dependencies use the
# Rust 2024 edition (stabilised in 1.85). A rust-toolchain.toml in the repo, if
# present, is honoured automatically.
ENV RUSTUP_HOME=/opt/rustup CARGO_HOME=/opt/cargo PATH=/opt/cargo/bin:$PATH
RUN curl -sSf https://sh.rustup.rs \
    | sh -s -- -y --profile minimal --default-toolchain stable
WORKDIR /src
RUN git clone --depth 1 --branch "v${KAFKA_BACKUP_VERSION}" \
        https://github.com/osodevops/kafka-backup.git .
# Default features only — the optional `gssapi` (Kerberos) feature is left off, so
# no krb5 system dependency is pulled in.
RUN cargo build --release \
    && test -x target/release/kafka-backup

# ---- runtime: UBI9-minimal + the binary + schema-backup helper tooling ------
FROM registry.access.redhat.com/ubi9/ubi-minimal:latest
LABEL org.opencontainers.image.title="kafka-backup" \
      org.opencontainers.image.description="osodevops/kafka-backup compiled on Red Hat UBI" \
      org.opencontainers.image.source="https://github.com/osodevops/kafka-backup" \
      org.opencontainers.image.licenses="MIT"
RUN microdnf install -y ca-certificates openssl jq gawk findutils shadow-utils \
    && microdnf clean all
# MinIO client — used by the schema-backup scripts to push/pull the Apicurio
# export zip to S3/GCS/Azure object storage.
RUN curl -sSfL "https://dl.min.io/client/mc/release/linux-amd64/mc" \
        -o /usr/local/bin/mc \
    && chmod 0755 /usr/local/bin/mc
COPY --from=build /src/target/release/kafka-backup /usr/local/bin/kafka-backup
COPY apicurio-export.sh apicurio-import.sh /usr/local/bin/
RUN chmod 0755 /usr/local/bin/kafka-backup \
        /usr/local/bin/apicurio-export.sh /usr/local/bin/apicurio-import.sh \
    && useradd -u 1000 -r -g 0 -s /sbin/nologin kafka-backup \
    && mkdir -p /data && chown -R 1000:0 /data
USER 1000
WORKDIR /data
ENTRYPOINT ["kafka-backup"]
CMD ["--help"]

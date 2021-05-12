{ pkgs ? import <nixos-unstable> { } }:
with pkgs;

buildEnv {
  name = "polixy-env";
  paths = [
    binutils
    cacert
    cargo-deny
    cargo-fuzz
    cargo-udeps
    cargo-watch
    clang
    cmake
    curl
    docker
    jq
    kubectl
    kube3d
    loc
    git
    (glibcLocales.override { locales = [ "en_US.UTF-8" ]; })
    gnupg
    openssl
    pkg-config
    protobuf
    rustup
    shellcheck
    stdenv
  ];

  passthru = with pkgs; {
    CARGO_TERM_COLOR = "always";
    CURL_CA_BUNDLE = "${cacert}/etc/ssl/certs/ca-bundle.crt";
    GIT_SSL_CAINFO = "${cacert}/etc/ssl/certs/ca-bundle.crt";
    LOCALE_ARCHIVE = "${glibcLocales}/lib/locale/locale-archive";
    LC_ALL = "en_US.UTF-8";
    OPENSSL_DIR = "${openssl.dev}";
    OPENSSL_LIB_DIR = "${openssl.out}/lib";
    PROTOC = "${protobuf}/bin/protoc";
    PROTOC_INCLUDE = "${protobuf}/include";
    RUST_BACKTRACE = "full";
    SSL_CERT_FILE = "${cacert}/etc/ssl/certs/ca-bundle.crt";
  };
}

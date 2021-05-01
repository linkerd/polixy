{ pkgs ? import <nixpkgs> { } }:
with pkgs;

let env = (import ./default.nix scope);

in mkShell { 
  CARGO_TERM_COLOR = "always";
  CURL_CA_BUNDLE = "${cacert}/etc/ssl/certs/ca-bundle.crt";
  GIT_SSL_CAINFO = "${cacert}/etc/ssl/certs/ca-bundle.crt";
  LC_ALL = "en_US.UTF-8";
  LOCALE_ARCHIVE = "${glibcLocales}/lib/locale/locale-archive";
  OPENSSL_DIR = "${openssl.dev}";
  OPENSSL_LIB_DIR = "${openssl.out}/lib";
  PROTOC = "${protobuf}/bin/protoc";
  PROTOC_INCLUDE = "${protobuf}/include";
  RUST_BACKTRACE = "1";
  SSL_CERT_FILE = "${cacert}/etc/ssl/certs/ca-bundle.crt";

  buildInputs = [ (import ./default.nix { inherit pkgs; }) ];
}

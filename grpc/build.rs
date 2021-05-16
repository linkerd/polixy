fn main() -> Result<(), Box<dyn std::error::Error>> {
    let files = &["proto/polixy.proto"];
    let dirs = &["proto"];

    let build_client = std::env::var_os("CARGO_FEATURE_CLIENT").is_some();
    let build_server = std::env::var_os("CARGO_FEATURE_SERVER").is_some();
    if !build_client && !build_server {
        return Err("either the `client` or `server` features must be enabled".into());
    }

    tonic_build::configure()
        .build_client(build_client)
        .build_server(build_server)
        .compile(files, dirs)?;

    // recompile protobufs only if any of the proto files changes.
    for file in files {
        println!("cargo:rerun-if-changed={}", file);
    }

    Ok(())
}

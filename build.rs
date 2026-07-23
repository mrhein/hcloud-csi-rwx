use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(std::env::var("OUT_DIR")?);
    tonic_build::configure()
        .out_dir(&out_dir)
        .build_server(true)
        .build_client(false)
        .compile_protos(&["proto/csi.proto"], &["proto"])?;
    println!("cargo:rerun-if-changed=proto/csi.proto");
    Ok(())
}

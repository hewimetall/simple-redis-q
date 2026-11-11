fn main() {
    // The installed ELF sits in <venv>/bin. $ORIGIN/../lib is <venv>/lib.
    // A uv-managed interpreter lives outside that tree. The wheel launcher
    // reads pyvenv.cfg and adds that interpreter's lib to LD_LIBRARY_PATH.
    if std::env::var("CARGO_CFG_TARGET_OS").ok().as_deref() == Some("linux") {
        println!("cargo:rustc-link-arg=-Wl,-rpath,$ORIGIN/../lib");
    }
}

fn main() {
    // lmdb-sys v0.8 doesn't link advapi32 on Windows (needed for
    // InitializeSecurityDescriptor / SetSecurityDescriptorDacl).
    #[cfg(target_os = "windows")]
    println!("cargo:rustc-link-lib=advapi32");
}

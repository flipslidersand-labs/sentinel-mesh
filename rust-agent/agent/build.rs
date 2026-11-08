fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Server stubs are generated only so unit tests can stand up an
    // in-process fake collector (#236); the agent binary never uses them.
    tonic_build::configure()
        .build_server(true)
        .compile(&["../../proto/sentinel.proto"], &["../../proto"])?;
    Ok(())
}

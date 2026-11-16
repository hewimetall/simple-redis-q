use super::*;

#[test]
fn factory_follows_the_flag_or_a_truthy_env_value() {
    // Arrange

    let absent = Cli::try_parse_from(["agent-worker", "worker", "--app", "app:create"])
        .expect("worker parses");
    match absent.command {
        Command::Worker { factory, .. } => assert!(!factory),
        _ => panic!("worker"),
    }

    let flagged = Cli::try_parse_from(["agent-worker", "info", "--app", "app:create", "--factory"])
        .expect("info parses");
    match flagged.command {
        Command::Info { factory, .. } => assert!(factory),
        _ => panic!("info"),
    }
    // Act

    let from_env = env_flag_enabled(std::env::var("SIMPLE_Q_APP_FACTORY").ok());
    // Assert

    assert_eq!(factory_enabled(false), from_env);
    assert!(factory_enabled(true));
    for value in ["1", "true", "TRUE", "yes", "on"] {
        assert!(env_flag_enabled(Some(value.into())), "{value}");
    }
    for value in ["false", "0", "no", "off", "", " false ", "banana"] {
        assert!(!env_flag_enabled(Some(value.into())), "{value}");
    }
    assert!(!env_flag_enabled(None));
}

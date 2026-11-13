//! Validator display names from on-chain `ValidatorInfo` config accounts, read whole off RPC.
//! Without the validator's index of the config program the dashboard shows pubkeys.

use {
    crate::collect::json_or_null,
    serde::{Deserialize, Serialize},
    solana_account_decoder::{
        parse_config::{ConfigAccountType, parse_config},
        validator_info,
    },
    solana_pubkey::Pubkey,
    std::{
        collections::HashMap,
        sync::{Arc, OnceLock},
    },
};

const MAX_VALIDATOR_INFO_LEN: usize = 576 + 1 + (32 + 1) * 2;

/// The account also carries a website, description and keybase name, which nothing displays.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct ValidatorInfo {
    pub name: Option<String>,
    #[serde(rename = "iconUrl")]
    pub icon_url: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct Displays {
    pub keys: Vec<String>,
    pub names: Vec<Option<String>>,
    pub icons: Vec<Option<String>>,
}

#[derive(Debug, Default)]
pub struct ValidatorInfoCache {
    by_identity: HashMap<Pubkey, ValidatorInfo>,
    /// `displays` as JSON, made on the first request after the table changes.
    displays_json: OnceLock<Arc<str>>,
}

impl ValidatorInfoCache {
    pub fn get(&self, identity: &Pubkey) -> Option<&ValidatorInfo> {
        self.by_identity.get(identity)
    }

    pub fn displays(&self) -> Displays {
        let (keys, (names, icons)) = self
            .by_identity
            .iter()
            .filter(|(_, info)| info.name.is_some() || info.icon_url.is_some())
            .map(|(key, info)| (key.to_string(), (info.name.clone(), info.icon_url.clone())))
            .unzip();
        Displays { keys, names, icons }
    }

    /// The `summary.displays` value, encoded once and shared until the table changes.
    pub fn displays_json(&self) -> Arc<str> {
        Arc::clone(
            self.displays_json
                .get_or_init(|| json_or_null(&self.displays())),
        )
    }

    /// The table as one read of the config program's accounts gives it.
    pub fn replace(&mut self, by_identity: HashMap<Pubkey, ValidatorInfo>) {
        if self.by_identity != by_identity {
            self.by_identity = by_identity;
            self.displays_json = OnceLock::new();
        }
    }
}

/// Keyed on the identity that signed for the account, its second key.
pub fn parse(data: &[u8]) -> Option<(Pubkey, ValidatorInfo)> {
    if data.len() > MAX_VALIDATOR_INFO_LEN {
        return None;
    }
    // The account's own key goes unread.
    let ConfigAccountType::ValidatorInfo(config) =
        parse_config(data, &validator_info::id()).ok()?;
    let identity = config.keys.get(1)?.pubkey.parse().ok()?;
    Some((identity, serde_json::from_value(config.config_data).ok()?))
}

#[cfg(test)]
mod tests {
    use {super::*, solana_config_interface::state::ConfigKeys};

    fn encode(keys: Vec<(Pubkey, bool)>, json: &str) -> Vec<u8> {
        let mut data = bincode::serialize(&ConfigKeys { keys }).unwrap();
        data.extend(bincode::serialize(&json.to_string()).unwrap());
        data
    }

    fn named(name: &str) -> ValidatorInfo {
        ValidatorInfo {
            name: Some(name.into()),
            icon_url: None,
        }
    }

    #[test]
    fn test_each_read_replaces_the_table() {
        let mut cache = ValidatorInfoCache::default();
        let (identity, other) = (Pubkey::new_unique(), Pubkey::new_unique());
        cache.replace(HashMap::from([(identity, named("Lantern"))]));
        assert_eq!(
            cache.get(&identity).and_then(|info| info.name.as_deref()),
            Some("Lantern")
        );
        cache.replace(HashMap::from([(other, named("Beacon"))]));
        assert_eq!(cache.get(&identity), None, "an account gone takes its name");
        assert_eq!(
            cache.get(&other).and_then(|info| info.name.as_deref()),
            Some("Beacon")
        );
    }

    #[test]
    fn test_parses_a_validator_info_account() {
        let identity = Pubkey::new_unique();
        let data = encode(
            vec![(validator_info::id(), false), (identity, true)],
            r#"{"name":"Lantern","iconUrl":"https://example.com/i.png"}"#,
        );
        let (parsed_identity, info) = parse(&data).unwrap();
        assert_eq!(parsed_identity, identity);
        assert_eq!(info.name.as_deref(), Some("Lantern"));
        assert_eq!(info.icon_url.as_deref(), Some("https://example.com/i.png"));
    }

    #[test]
    fn test_fields_the_dashboard_does_not_render_are_ignored() {
        // An unknown field must be skipped, or a validator publishing a website would
        // lose its name too.
        let identity = Pubkey::new_unique();
        let data = encode(
            vec![(validator_info::id(), false), (identity, true)],
            r#"{"name":"Lantern","website":"https://example.com","details":"hi",
                "keybaseUsername":"lantern"}"#,
        );
        let (_, info) = parse(&data).unwrap();
        assert_eq!(info.name.as_deref(), Some("Lantern"));
    }

    #[test]
    fn test_ignores_other_config_accounts() {
        let data = encode(
            vec![(Pubkey::new_unique(), false), (Pubkey::new_unique(), true)],
            r#"{"name":"nope"}"#,
        );
        assert!(parse(&data).is_none());
    }

    #[test]
    fn test_ignores_malformed_data() {
        assert!(parse(&[]).is_none());
        assert!(parse(&[0xff; 32]).is_none());
        assert!(parse(&vec![0u8; MAX_VALIDATOR_INFO_LEN.saturating_add(1)]).is_none());
    }

    #[test]
    fn test_the_encoded_table_is_kept_until_the_table_changes() {
        let mut cache = ValidatorInfoCache::default();
        let identity = Pubkey::new_unique();
        let fresh = |cache: &ValidatorInfoCache| serde_json::to_string(&cache.displays()).unwrap();

        assert_eq!(&*cache.displays_json(), fresh(&cache).as_str());
        cache.replace(HashMap::from([(identity, named("Lantern"))]));
        let held = cache.displays_json();
        assert_eq!(&*held, fresh(&cache).as_str());

        // A read that changes nothing keeps the encoding.
        cache.replace(HashMap::from([(identity, named("Lantern"))]));
        assert!(Arc::ptr_eq(&held, &cache.displays_json()));

        cache.replace(HashMap::from([(identity, named("Beacon"))]));
        assert_eq!(&*cache.displays_json(), fresh(&cache).as_str());
        assert!(cache.displays_json().contains("Beacon"));

        cache.replace(HashMap::from([
            (identity, named("Beacon")),
            (Pubkey::new_unique(), named("Harbour")),
        ]));
        assert_eq!(&*cache.displays_json(), fresh(&cache).as_str());
        assert!(cache.displays_json().contains("Harbour"));
    }
}

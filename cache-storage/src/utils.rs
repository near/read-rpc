// Help functions to serialize and deserialize the data to be stored in the cache.
// This is needed to handle the backward incompatible changes in the TransactionDetails

pub fn to_vec<T: serde::ser::Serialize>(value: &T) -> anyhow::Result<Vec<u8>> {
    let value_json = serde_json::to_value(value)?.to_string();
    Ok(value_json.into_bytes())
}

pub fn from_slice<'a, T: serde::de::Deserialize<'a>>(data: &'a [u8]) -> anyhow::Result<T> {
    Ok(serde_json::from_slice(data)?)
}

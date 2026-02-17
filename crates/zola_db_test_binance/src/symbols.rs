use serde::Deserialize;

#[derive(Deserialize)]
struct ExchangeInfo {
    symbols: Vec<SymbolInfo>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SymbolInfo {
    symbol: String,
    contract_type: String,
    quote_asset: String,
    status: String,
}

/// Fetch Binance USDT perpetual symbols sorted alphabetically.
pub async fn fetch_symbols(client: &reqwest::Client) -> Vec<String> {
    let info: ExchangeInfo = client
        .get("https://fapi.binance.com/fapi/v1/exchangeInfo")
        .send()
        .await
        .expect("failed to fetch exchangeInfo")
        .json()
        .await
        .expect("failed to parse exchangeInfo");

    let mut names: Vec<String> = info
        .symbols
        .into_iter()
        .filter(|s| {
            s.contract_type == "PERPETUAL"
                && s.quote_asset == "USDT"
                && s.status == "TRADING"
        })
        .map(|s| s.symbol)
        .collect();

    names.sort();
    names
}

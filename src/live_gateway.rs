use std::{str::FromStr, sync::Arc, time::Duration};

use ethers::types::H160;
use hyperliquid_rust_sdk::{
    BaseUrl, ClientLimit, ClientOrder, ClientOrderRequest, ClientTrigger, ExchangeClient,
    ExchangeDataStatus, ExchangeResponseStatus, InfoClient, MarketOrderParams, OrderStatusResponse,
};
use rust_decimal::{Decimal, RoundingStrategy, prelude::ToPrimitive};
use uuid::Uuid;

use crate::{
    domain::{Side, TradeIntent},
    live_execution::{
        ConfirmedFill, GatewayFailure, LiveExecutionGateway, OrderRole, deterministic_cloid,
    },
};

pub struct HyperliquidGateway {
    client: Arc<ExchangeClient>,
    info: Arc<InfoClient>,
    account: H160,
}

impl HyperliquidGateway {
    pub async fn new(
        wallet: ethers::signers::LocalWallet,
        base_url: BaseUrl,
        account_address: &str,
    ) -> Result<Self, String> {
        let account = H160::from_str(account_address)
            .map_err(|_| "account address is not a valid EVM address".to_string())?;
        let client = ExchangeClient::new(None, wallet, Some(base_url), None, None)
            .await
            .map_err(|error| error.to_string())?;
        let info = InfoClient::new(None, Some(base_url))
            .await
            .map_err(|error| error.to_string())?;
        Ok(Self {
            client: Arc::new(client),
            info: Arc::new(info),
            account,
        })
    }
}

impl LiveExecutionGateway for HyperliquidGateway {
    async fn submit_entry(
        &self,
        intent: &TradeIntent,
        approved_notional: Decimal,
        cloid: Uuid,
    ) -> Result<ConfirmedFill, GatewayFailure> {
        let user_state = self
            .info
            .user_state(self.account)
            .await
            .map_err(|error| GatewayFailure::OutcomeUnknown(error.to_string()))?;
        let already_open = user_state.asset_positions.iter().any(|position| {
            position.position.coin == intent.symbol
                && position
                    .position
                    .szi
                    .parse::<Decimal>()
                    .is_ok_and(|size| !size.is_zero())
        });
        if already_open {
            return Err(GatewayFailure::Definite(
                "pre-submit risk rejected an existing symbol position".into(),
            ));
        }
        let reference_price = decimal_f64(intent.reference_price)?;
        let size = decimal_f64(approved_notional)? / reference_price;
        let response = self
            .client
            .market_open(MarketOrderParams {
                asset: &intent.symbol,
                is_buy: intent.side == Side::Long,
                sz: size,
                px: Some(reference_price),
                slippage: Some(f64::from(intent.entry.max_slippage_bps) / 10_000.0),
                cloid: Some(cloid),
                wallet: None,
            })
            .await
            .map_err(|error| GatewayFailure::OutcomeUnknown(error.to_string()))?;
        submission_accepted(response)?;
        self.query_confirmed_fill(cloid).await
    }

    async fn establish_protection(
        &self,
        intent: &TradeIntent,
        fill: &ConfirmedFill,
    ) -> Result<(), GatewayFailure> {
        let close_is_buy = intent.side == Side::Short;
        let total_size = self.round_size(&intent.symbol, fill.size)?;
        let mut orders = vec![trigger_order(
            intent,
            close_is_buy,
            total_size,
            self.round_price(&intent.symbol, decimal_f64(intent.exit.stop_loss_price)?)?,
            "sl",
            deterministic_cloid(intent.intent_id, OrderRole::StopLoss),
        )];
        for (index, target) in intent.exit.take_profit.iter().enumerate() {
            orders.push(trigger_order(
                intent,
                close_is_buy,
                self.round_size(
                    &intent.symbol,
                    fill.size * Decimal::from(target.position_pct) / Decimal::ONE_HUNDRED,
                )?,
                self.round_price(&intent.symbol, decimal_f64(target.price)?)?,
                "tp",
                deterministic_cloid(intent.intent_id, OrderRole::TakeProfit(index as u8)),
            ));
        }
        let response = self
            .client
            .bulk_order(orders, None)
            .await
            .map_err(|error| GatewayFailure::OutcomeUnknown(error.to_string()))?;
        require_all_accepted(response)?;
        self.verify_protection(intent, fill).await
    }

    async fn emergency_close(
        &self,
        intent: &TradeIntent,
        fill: &ConfirmedFill,
        cloid: Uuid,
    ) -> Result<(), GatewayFailure> {
        let price = decimal_f64(fill.average_price)?;
        let slippage = f64::from(intent.entry.max_slippage_bps) / 10_000.0;
        let close_is_buy = intent.side == Side::Short;
        let aggressive_price = if close_is_buy {
            price * (1.0 + slippage)
        } else {
            price * (1.0 - slippage)
        };
        let aggressive_price = self.round_price(&intent.symbol, aggressive_price)?;
        let response = self
            .client
            .order(
                ClientOrderRequest {
                    asset: intent.symbol.clone(),
                    is_buy: close_is_buy,
                    reduce_only: true,
                    limit_px: aggressive_price,
                    sz: self.round_size(&intent.symbol, fill.size)?,
                    cloid: Some(cloid),
                    order_type: ClientOrder::Limit(ClientLimit { tif: "Ioc".into() }),
                },
                None,
            )
            .await
            .map_err(|error| GatewayFailure::OutcomeUnknown(error.to_string()))?;
        confirmed_fill(response).map(|_| ())
    }
}

impl HyperliquidGateway {
    fn size_decimals(&self, symbol: &str) -> Result<u32, GatewayFailure> {
        self.client
            .meta
            .universe
            .iter()
            .find(|asset| asset.name == symbol)
            .map(|asset| asset.sz_decimals)
            .ok_or_else(|| {
                GatewayFailure::Definite("asset is missing from exchange metadata".into())
            })
    }

    fn round_size(&self, symbol: &str, size: Decimal) -> Result<f64, GatewayFailure> {
        let rounded =
            size.round_dp_with_strategy(self.size_decimals(symbol)?, RoundingStrategy::ToZero);
        if rounded <= Decimal::ZERO {
            return Err(GatewayFailure::Definite(
                "order size rounds to zero at exchange precision".into(),
            ));
        }
        decimal_f64(rounded)
    }

    fn round_price(&self, symbol: &str, price: f64) -> Result<f64, GatewayFailure> {
        let decimals = 6u32.saturating_sub(self.size_decimals(symbol)?);
        let magnitude = price.abs().log10().floor() as i32;
        let scale = 10f64.powi(5 - magnitude - 1);
        let significant = (price.abs() * scale).round() / scale;
        let decimal_scale = 10f64.powi(decimals as i32);
        Ok((significant.copysign(price) * decimal_scale).round() / decimal_scale)
    }

    async fn query_confirmed_fill(&self, cloid: Uuid) -> Result<ConfirmedFill, GatewayFailure> {
        let order = self.query_order_by_cloid_with_retry(cloid).await?;
        let oid = order
            .order
            .as_ref()
            .map(|value| value.order.oid)
            .ok_or_else(|| {
                GatewayFailure::OutcomeUnknown("orderStatus did not return the IOC order".into())
            })?;
        for _ in 0..5 {
            let fills = self
                .info
                .user_fills(self.account)
                .await
                .map_err(|error| GatewayFailure::OutcomeUnknown(error.to_string()))?;
            let matching = fills
                .iter()
                .filter(|fill| fill.oid == oid)
                .collect::<Vec<_>>();
            if !matching.is_empty() {
                let mut size = Decimal::ZERO;
                let mut quote = Decimal::ZERO;
                for fill in matching {
                    let fill_size = fill.sz.parse::<Decimal>().map_err(|_| {
                        GatewayFailure::OutcomeUnknown("invalid authoritative fill size".into())
                    })?;
                    let price = fill.px.parse::<Decimal>().map_err(|_| {
                        GatewayFailure::OutcomeUnknown("invalid authoritative fill price".into())
                    })?;
                    size += fill_size;
                    quote += fill_size * price;
                }
                if !size.is_zero() {
                    return Ok(ConfirmedFill {
                        exchange_oid: oid,
                        size,
                        average_price: quote / size,
                    });
                }
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        if matches!(
            order.status.as_str(),
            "canceled" | "rejected" | "marginCanceled"
        ) {
            Err(GatewayFailure::Definite(format!(
                "IOC ended with no fill: {}",
                order.status
            )))
        } else {
            Err(GatewayFailure::OutcomeUnknown(
                "authoritative fills did not expose the IOC result".into(),
            ))
        }
    }

    async fn verify_protection(
        &self,
        intent: &TradeIntent,
        fill: &ConfirmedFill,
    ) -> Result<(), GatewayFailure> {
        let stop_cloid = cloid_hex(deterministic_cloid(intent.intent_id, OrderRole::StopLoss));
        for _ in 0..5 {
            let open = self
                .info
                .open_orders(self.account)
                .await
                .map_err(|error| GatewayFailure::OutcomeUnknown(error.to_string()))?;
            for summary in open {
                let detail = self
                    .info
                    .query_order_by_oid(self.account, summary.oid)
                    .await
                    .map_err(|error| GatewayFailure::OutcomeUnknown(error.to_string()))?;
                let Some(order) = detail.order.map(|value| value.order) else {
                    continue;
                };
                let size = order.sz.parse::<Decimal>().map_err(|_| {
                    GatewayFailure::OutcomeUnknown("invalid protection size".into())
                })?;
                let correct_side = match intent.side {
                    Side::Long => order.side == "A",
                    Side::Short => order.side == "B",
                };
                if order.cloid.as_deref() == Some(stop_cloid.as_str())
                    && order.coin == intent.symbol
                    && order.reduce_only
                    && order.is_trigger
                    && correct_side
                    && size >= fill.size
                {
                    return Ok(());
                }
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        Err(GatewayFailure::Definite(
            "authoritative open orders do not provide full stop-loss coverage".into(),
        ))
    }

    async fn query_order_by_cloid_with_retry(
        &self,
        cloid: Uuid,
    ) -> Result<OrderStatusResponse, GatewayFailure> {
        let payload = serde_json::json!({
            "type": "orderStatus",
            "user": format!("{:#x}", self.account),
            "oid": cloid_hex(cloid),
        })
        .to_string();
        for _ in 0..5 {
            let raw = self
                .info
                .http_client
                .post("/info", payload.clone())
                .await
                .map_err(|error| GatewayFailure::OutcomeUnknown(error.to_string()))?;
            let response: OrderStatusResponse = serde_json::from_str(&raw).map_err(|_| {
                GatewayFailure::OutcomeUnknown("invalid orderStatus response".into())
            })?;
            if response.order.is_some() {
                return Ok(response);
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        Err(GatewayFailure::OutcomeUnknown(
            "order was not found by cloid".into(),
        ))
    }
}

fn trigger_order(
    intent: &TradeIntent,
    is_buy: bool,
    size: f64,
    trigger_price: f64,
    tpsl: &str,
    cloid: Uuid,
) -> ClientOrderRequest {
    ClientOrderRequest {
        asset: intent.symbol.clone(),
        is_buy,
        reduce_only: true,
        limit_px: trigger_price,
        sz: size,
        cloid: Some(cloid),
        order_type: ClientOrder::Trigger(ClientTrigger {
            is_market: true,
            trigger_px: trigger_price,
            tpsl: tpsl.into(),
        }),
    }
}

fn decimal_f64(value: Decimal) -> Result<f64, GatewayFailure> {
    value
        .to_f64()
        .ok_or_else(|| GatewayFailure::Definite("decimal is outside exchange range".into()))
}

fn confirmed_fill(response: ExchangeResponseStatus) -> Result<ConfirmedFill, GatewayFailure> {
    let statuses = response_statuses(response)?;
    match statuses.as_slice() {
        [ExchangeDataStatus::Filled(fill)] => Ok(ConfirmedFill {
            exchange_oid: fill.oid,
            size: fill.total_sz.parse().map_err(|_| {
                GatewayFailure::OutcomeUnknown("exchange returned invalid fill size".into())
            })?,
            average_price: fill.avg_px.parse().map_err(|_| {
                GatewayFailure::OutcomeUnknown("exchange returned invalid average price".into())
            })?,
        }),
        [ExchangeDataStatus::Error(error)] => Err(GatewayFailure::Definite(error.clone())),
        _ => Err(GatewayFailure::OutcomeUnknown(
            "IOC response did not contain one confirmed fill".into(),
        )),
    }
}

fn submission_accepted(response: ExchangeResponseStatus) -> Result<(), GatewayFailure> {
    let statuses = response_statuses(response)?;
    match statuses.as_slice() {
        [ExchangeDataStatus::Filled(_) | ExchangeDataStatus::WaitingForFill] => Ok(()),
        [ExchangeDataStatus::Error(error)] => Err(GatewayFailure::Definite(error.clone())),
        _ => Err(GatewayFailure::OutcomeUnknown(
            "IOC submission response was ambiguous".into(),
        )),
    }
}

fn cloid_hex(cloid: Uuid) -> String {
    format!("0x{}", cloid.simple())
}

fn require_all_accepted(response: ExchangeResponseStatus) -> Result<(), GatewayFailure> {
    let statuses = response_statuses(response)?;
    if statuses.iter().all(|status| {
        matches!(
            status,
            ExchangeDataStatus::WaitingForTrigger | ExchangeDataStatus::Resting(_)
        )
    }) {
        Ok(())
    } else if let Some(ExchangeDataStatus::Error(error)) = statuses
        .iter()
        .find(|status| matches!(status, ExchangeDataStatus::Error(_)))
    {
        Err(GatewayFailure::Definite(error.clone()))
    } else {
        Err(GatewayFailure::OutcomeUnknown(
            "protection response was not fully accepted".into(),
        ))
    }
}

fn response_statuses(
    response: ExchangeResponseStatus,
) -> Result<Vec<ExchangeDataStatus>, GatewayFailure> {
    match response {
        ExchangeResponseStatus::Ok(response) => {
            response.data.map(|data| data.statuses).ok_or_else(|| {
                GatewayFailure::OutcomeUnknown("exchange response omitted order statuses".into())
            })
        }
        ExchangeResponseStatus::Err(error) => Err(GatewayFailure::Definite(error)),
    }
}

# SPDX-License-Identifier: LicenseRef-Blockscout
defmodule Explorer.Market.Source.SvmOracle do
  @moduledoc """
  Prices the native coin (STEEM) and the SBD ERC-20 straight from the Steem
  Virtual Machine `oracledata` precompile.

  The precompile lives at a fixed address on every SVM chain and exposes
  `getPrice(string pair) -> (uint256 rate, uint256 updateEpoch, uint256 updateTime)`.
  `rate` is 18-decimal fixed point USD; an unknown or not-yet-finalized pair
  returns zeros instead of reverting, so `rate == 0` is treated as "no price".
  Rates are refreshed on-chain roughly once per vote period (~1h).

  Only spot prices are supported: there is no price-history source.
  """

  alias ABI.TypeDecoder
  alias EthereumJSONRPC.Contract
  alias Explorer.Chain.Hash
  alias Explorer.Market.{Source, Token}

  import EthereumJSONRPC, only: [json_rpc: 2]

  @behaviour Source

  @oracle_precompile "0x0000000000000000000000000000000000000902"
  # SBD ERC-20 wrapper of the `asbd` bank denom
  @sbd_token "0x0000000000000000000000000000000000000901"

  @native_pair "STEEM/USD_External"
  @sbd_pair "SBD/USD_External"

  @impl Source
  def native_coin_fetching_enabled?, do: enabled?()

  @impl Source
  def fetch_native_coin do
    case oracle_price(@native_pair) do
      {:ok, price} ->
        {:ok,
         %Token{
           available_supply: nil,
           total_supply: nil,
           btc_value: nil,
           last_updated: DateTime.utc_now(),
           market_cap: nil,
           tvl: nil,
           name: nil,
           symbol: nil,
           fiat_value: price,
           volume_24h: nil,
           image_url: nil,
           circulating_supply: nil
         }}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl Source
  def secondary_coin_fetching_enabled?, do: :ignore
  @impl Source
  def fetch_secondary_coin, do: :ignore

  @impl Source
  def tokens_fetching_enabled?, do: enabled?()

  @impl Source
  def fetch_tokens(state, _batch_size) do
    with {:ok, price} <- oracle_price(@sbd_pair),
         {:ok, hash} <- Hash.Address.cast(@sbd_token) do
      {:ok, state, true, [%{contract_address_hash: hash, fiat_value: price, type: "ERC-20"}]}
    else
      {:error, reason} -> {:error, reason}
      _ -> {:error, "invalid SBD token address"}
    end
  end

  @impl Source
  def native_coin_price_history_fetching_enabled?, do: :ignore
  @impl Source
  def fetch_native_coin_price_history(_previous_days), do: :ignore

  @impl Source
  def secondary_coin_price_history_fetching_enabled?, do: :ignore
  @impl Source
  def fetch_secondary_coin_price_history(_previous_days), do: :ignore

  @impl Source
  def market_cap_history_fetching_enabled?, do: :ignore
  @impl Source
  def fetch_market_cap_history(_previous_days), do: :ignore

  @impl Source
  def tvl_history_fetching_enabled?, do: :ignore
  @impl Source
  def fetch_tvl_history(_previous_days), do: :ignore

  defp oracle_price(pair) do
    data = "0x" <> Base.encode16(ABI.encode("getPrice(string)", [pair]), case: :lower)

    case data
         |> Contract.eth_call_request(@oracle_precompile, 1, nil, nil)
         |> json_rpc(json_rpc_named_arguments()) do
      {:ok, "0x" <> encoded} ->
        case encoded
             |> Base.decode16!(case: :mixed)
             |> TypeDecoder.decode_raw([{:uint, 256}, {:uint, 256}, {:uint, 256}]) do
          [0, _epoch, _time] ->
            {:error, "oracle has no finalized price for #{pair}"}

          [rate, _epoch, _time] ->
            {:ok, Decimal.div(Decimal.new(rate), Decimal.new(1_000_000_000_000_000_000))}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp enabled?, do: not is_nil(json_rpc_named_arguments())

  defp json_rpc_named_arguments, do: Application.get_env(:explorer, :json_rpc_named_arguments)
end

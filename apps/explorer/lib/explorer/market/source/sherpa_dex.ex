# SPDX-License-Identifier: LicenseRef-Blockscout
defmodule Explorer.Market.Source.SherpaDex do
  @moduledoc """
  Prices STEEM (native coin), wSTEEM, sUSD, SBD, and SHERPA directly from the
  on-chain SherpaOracle contract (`getPrice(bytes32) -> (uint256, bool)`,
  1e18-scaled USD), and derives a fiat price for any other token traded on
  the SherpaV3Factory DEX (a PancakeSwap V3 / Uniswap V3 fork) by anchoring
  a pool's `slot0().sqrtPriceX96` ratio to whichever side of the pool is
  already priced.

  Every poll re-scans the factory for new `PoolCreated` events since the
  last-seen block, so a brand new pool gets priced automatically on the next
  cycle as long as one side of it is already priced (directly via the
  oracle, the sUSD/wSTEEM anchors below, or transitively via another pool).

  `fetch_tokens/2`'s `state` argument carries the running set of discovered
  pools and resolved prices forward across polls (see
  `Explorer.Market.Fetcher.Token`, which only resets it after repeated
  errors) — so a full historical rescan only happens once, on first start or
  after failures.
  """

  require Logger

  import EthereumJSONRPC, only: [json_rpc: 2]

  alias ABI.TypeDecoder
  alias EthereumJSONRPC.{Contract, Logs}
  alias Explorer.Chain.Hash
  alias Explorer.Market.{Source, Token}

  @behaviour Source

  # keccak256("getPrice(bytes32)")
  @get_price_signature "0x31d98b3f"
  # keccak256("slot0()")
  @slot0_signature "0x3850c7bd"
  # keccak256("decimals()")
  @decimals_signature "0x313ce567"
  # keccak256("PoolCreated(address,address,uint24,int24,address)")
  @pool_created_topic "0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118"

  # keccak256("STEEM/USD")
  @native_oracle_key "0x1efd5096f31769b85fb782c12551769b98e7a9d3946bf3ac774d3e714959fd7d"
  # keccak256("SBD/USD")
  @sbd_oracle_key "0xa05f39c45fff8bfd6f07385642b09efefd59508d78b752ca1124cd5af81e7086"
  # keccak256("SHERPA/USD")
  @sherpa_oracle_key "0x05ae2d15e99b4e4a1932103920d41d46dfe4dd3640485a98c2bd9ae1e3f1eed5"

  # eth_getLogs block range accepted by the node in one call
  @max_log_block_range 9_999
  # cap on price-propagation passes over the discovered pool graph per poll
  @max_propagation_passes 10

  @impl Source
  def native_coin_fetching_enabled?, do: enabled?()

  @impl Source
  def fetch_native_coin do
    case oracle_price(@native_oracle_key) do
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
    %{last_block: last_block, pools: known_pools, prices: known_prices} = state || initial_state()

    native_price = ok_or(oracle_price(@native_oracle_key), Map.get(known_prices, wsteem_address()))
    sbd_price = ok_or(oracle_price(@sbd_oracle_key), Map.get(known_prices, sbd_address()))
    sherpa_price = ok_or(oracle_price(@sherpa_oracle_key), Map.get(known_prices, sherpa_address()))

    with {:ok, head_block} <- latest_block_number(),
         {:ok, new_pools} <- discover_pools(last_block, head_block) do
      pools = Map.merge(known_pools, new_pools)

      seed_prices =
        known_prices
        |> Map.put(susd_address(), Decimal.new(1))
        |> maybe_put(wsteem_address(), native_price)
        |> maybe_put(sbd_address(), sbd_price)
        |> maybe_put(sherpa_address(), sherpa_price)

      prices = propagate_prices(seed_prices, pools)

      token_params =
        prices
        |> Enum.map(fn {address_hash, price} -> build_token_param(address_hash, price) end)
        |> Enum.reject(&is_nil/1)

      new_state = %{last_block: head_block, pools: pools, prices: prices}

      {:ok, new_state, true, token_params}
    else
      {:error, reason} -> {:error, reason}
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

  defp initial_state, do: %{last_block: 0, pools: %{}, prices: %{}}

  # Discovers PoolCreated events on the factory since `last_block`, chunked
  # to respect the node's eth_getLogs block-range limit.
  defp discover_pools(last_block, head_block) when head_block < last_block, do: {:ok, %{}}

  defp discover_pools(last_block, head_block) do
    last_block
    |> Stream.iterate(&(&1 + @max_log_block_range + 1))
    |> Enum.take_while(&(&1 <= head_block))
    |> Enum.reduce_while({:ok, %{}}, fn chunk_start, {:ok, acc} ->
      chunk_end = min(chunk_start + @max_log_block_range, head_block)

      request =
        Logs.request(0, %{
          from_block: chunk_start,
          to_block: chunk_end,
          address: factory_address(),
          topics: [@pool_created_topic]
        })

      case json_rpc(request, json_rpc_named_arguments()) do
        {:ok, raw_logs} when is_list(raw_logs) ->
          new_pools =
            raw_logs
            |> Enum.map(&parse_pool_created_log/1)
            |> Enum.reject(&is_nil/1)
            |> Map.new(&{&1.pool_address, &1})

          {:cont, {:ok, Map.merge(acc, new_pools)}}

        {:error, reason} ->
          {:halt, {:error, reason}}
      end
    end)
  end

  defp parse_pool_created_log(%{"topics" => [_, token0_topic, token1_topic, fee_topic], "data" => data}) do
    [token0] = token0_topic |> decode_hex() |> TypeDecoder.decode_raw([:address])
    [token1] = token1_topic |> decode_hex() |> TypeDecoder.decode_raw([:address])
    [fee] = fee_topic |> decode_hex() |> TypeDecoder.decode_raw([{:uint, 24}])
    [_tick_spacing, pool_address] = data |> decode_hex() |> TypeDecoder.decode_raw([{:int, 24}, :address])

    %{
      pool_address: encode_address(pool_address),
      token0: encode_address(token0),
      token1: encode_address(token1),
      fee: fee
    }
  rescue
    _ -> nil
  end

  defp parse_pool_created_log(_), do: nil

  # Repeatedly walks the discovered pool graph, pricing one more token per
  # pass from whichever side is already priced, until a pass makes no
  # progress (handles pools chained N hops away from an anchor).
  defp propagate_prices(prices, pools) do
    Enum.reduce_while(1..@max_propagation_passes, prices, fn _pass, acc ->
      {new_acc, changed?} =
        Enum.reduce(Map.values(pools), {acc, false}, fn pool, {acc2, changed2} ->
          if Map.has_key?(acc2, pool.token0) and Map.has_key?(acc2, pool.token1) do
            {acc2, changed2}
          else
            case derive_price(acc2, pool) do
              {:ok, address_hash, price} -> {Map.put(acc2, address_hash, price), true}
              :unresolved -> {acc2, changed2}
            end
          end
        end)

      if changed?, do: {:cont, new_acc}, else: {:halt, new_acc}
    end)
  end

  defp derive_price(prices, pool) do
    token0_price = Map.get(prices, pool.token0)
    token1_price = Map.get(prices, pool.token1)

    with true <- is_nil(token0_price) != is_nil(token1_price),
         {:ok, sqrt_price_x96} <- pool_sqrt_price(pool.pool_address),
         {:ok, decimals0} <- token_decimals(pool.token0),
         {:ok, decimals1} <- token_decimals(pool.token1) do
      ratio = token1_per_token0_ratio(sqrt_price_x96, decimals0, decimals1)

      if token0_price do
        {:ok, pool.token1, Decimal.div(token0_price, ratio)}
      else
        {:ok, pool.token0, Decimal.mult(token1_price, ratio)}
      end
    else
      _ -> :unresolved
    end
  end

  # amount of token1 (human units) per 1 token0, from Uniswap-V3-style
  # sqrtPriceX96: (sqrtPriceX96 / 2^96)^2 * 10^(decimals0 - decimals1)
  defp token1_per_token0_ratio(sqrt_price_x96, decimals0, decimals1) do
    price = :math.pow(sqrt_price_x96 / :math.pow(2, 96), 2) * :math.pow(10, decimals0 - decimals1)
    Decimal.from_float(price)
  end

  defp pool_sqrt_price(pool_address) do
    case @slot0_signature
         |> Contract.eth_call_request(pool_address, 1, nil, nil)
         |> json_rpc(json_rpc_named_arguments()) do
      {:ok, "0x" <> encoded} ->
        [sqrt_price_x96 | _rest] =
          encoded
          |> Base.decode16!(case: :mixed)
          |> TypeDecoder.decode_raw([{:uint, 160}, {:int, 24}, {:uint, 16}, {:uint, 16}, {:uint, 16}, {:uint, 8}, :bool])

        {:ok, sqrt_price_x96}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp token_decimals(address) do
    case @decimals_signature
         |> Contract.eth_call_request(address, 1, nil, nil)
         |> json_rpc(json_rpc_named_arguments()) do
      {:ok, "0x" <> encoded} ->
        [decimals] = encoded |> Base.decode16!(case: :mixed) |> TypeDecoder.decode_raw([{:uint, 256}])
        {:ok, decimals}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp oracle_price(key) do
    data = @get_price_signature <> String.trim_leading(key, "0x")

    case data
         |> Contract.eth_call_request(oracle_address(), 1, nil, nil)
         |> json_rpc(json_rpc_named_arguments()) do
      {:ok, "0x" <> encoded} ->
        case encoded |> Base.decode16!(case: :mixed) |> TypeDecoder.decode_raw([{:uint, 256}, :bool]) do
          [price, true] -> {:ok, Decimal.div(Decimal.new(price), one_e18())}
          [_price, false] -> {:error, "SherpaOracle marked price invalid for key #{key}"}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp latest_block_number do
    case json_rpc(EthereumJSONRPC.request(%{id: 0, method: "eth_blockNumber", params: []}), json_rpc_named_arguments()) do
      {:ok, "0x" <> hex} -> {:ok, String.to_integer(hex, 16)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp build_token_param(address_hash_string, price) do
    case Hash.Address.cast(address_hash_string) do
      {:ok, hash} -> %{contract_address_hash: hash, fiat_value: price, type: "ERC-20"}
      _ -> nil
    end
  end

  defp decode_hex("0x" <> hex), do: Base.decode16!(hex, case: :mixed)

  defp encode_address(<<_::binary-size(20)>> = bytes), do: "0x" <> Base.encode16(bytes, case: :lower)

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  defp ok_or({:ok, value}, _default), do: value
  defp ok_or({:error, _reason}, default), do: default

  defp one_e18, do: Decimal.new(1_000_000_000_000_000_000)

  defp enabled?, do: not is_nil(oracle_address()) and not is_nil(json_rpc_named_arguments())

  defp oracle_address, do: normalize_address(config(:oracle_address))
  defp factory_address, do: normalize_address(config(:factory_address))
  defp wsteem_address, do: normalize_address(config(:wsteem_address))
  defp susd_address, do: normalize_address(config(:susd_address))
  defp sbd_address, do: normalize_address(config(:sbd_address))
  defp sherpa_address, do: normalize_address(config(:sherpa_address))

  defp normalize_address(nil), do: nil
  defp normalize_address(address), do: String.downcase(address)

  defp json_rpc_named_arguments, do: Application.get_env(:explorer, :json_rpc_named_arguments)

  defp config(key), do: Application.get_env(:explorer, __MODULE__, [])[key]
end

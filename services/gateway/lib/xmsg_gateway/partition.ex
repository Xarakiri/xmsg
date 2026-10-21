# SPDX-License-Identifier: AGPL-3.0-only
defmodule XmsgGateway.Partition do
  @moduledoc """
  Kafka-compatible partitioning of channel IDs (spec/partitioning).

  The gateway uses it to publish typing indicators to `typing.{partition}` and
  to scope resync hints (ADR-0009, ADR-0021).
  """

  import Bitwise

  @seed 0x9747B28C
  @m 0x5BD1E995
  @r 24
  @mask32 0xFFFFFFFF

  @doc "Kafka's `Utils.murmur2`: the same signed 32-bit result as the Java code."
  @spec murmur2(binary()) :: integer()
  def murmur2(data) when is_binary(data) do
    length = byte_size(data)
    {h, rest} = mix_blocks(data, band(bxor(@seed, length), @mask32))

    h =
      case rest do
        <<a, b, c>> -> h |> bxor(c <<< 16) |> bxor(b <<< 8) |> bxor(a) |> mul()
        <<a, b>> -> h |> bxor(b <<< 8) |> bxor(a) |> mul()
        <<a>> -> h |> bxor(a) |> mul()
        <<>> -> h
      end

    h = bxor(h, h >>> 13) |> mul()
    h = bxor(h, h >>> 15)
    to_signed32(h)
  end

  @doc "The Kafka key for a channel: its ID as 8-byte big-endian two's complement."
  @spec channel_key(integer()) :: <<_::64>>
  def channel_key(channel_id) when is_integer(channel_id), do: <<channel_id::signed-big-64>>

  @doc "Partition of `channel_id` in a topic with `partition_count` partitions."
  @spec partition(integer(), pos_integer()) :: non_neg_integer()
  def partition(channel_id, partition_count)
      when is_integer(partition_count) and partition_count > 0 do
    channel_id |> channel_key() |> murmur2() |> band(0x7FFFFFFF) |> rem(partition_count)
  end

  defp mix_blocks(<<k::little-unsigned-32, rest::binary>>, h) do
    k = k |> mul() |> then(&bxor(&1, &1 >>> @r)) |> mul()
    mix_blocks(rest, bxor(mul(h), k))
  end

  defp mix_blocks(rest, h), do: {h, rest}

  defp mul(x), do: band(x * @m, @mask32)

  defp to_signed32(x) when x >= 0x80000000, do: x - 0x100000000
  defp to_signed32(x), do: x
end

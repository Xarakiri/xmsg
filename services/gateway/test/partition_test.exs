# SPDX-License-Identifier: AGPL-3.0-only
defmodule XmsgGateway.PartitionTest do
  use ExUnit.Case, async: true

  alias XmsgGateway.Partition

  @vectors Path.expand("../../../spec/partitioning/vectors.csv", __DIR__)

  test "matches spec/partitioning/vectors.csv" do
    [header | rows] = @vectors |> File.read!() |> String.trim() |> String.split("\n")

    counts =
      header
      |> String.split(",")
      |> Enum.drop(3)
      |> Enum.map(&(&1 |> String.trim_leading("p") |> String.to_integer()))

    for row <- rows do
      [id, key_hex, hash | parts] = String.split(row, ",")
      channel_id = String.to_integer(id)
      key = Partition.channel_key(channel_id)

      assert Base.encode16(key, case: :lower) == key_hex, "key bytes for #{channel_id}"
      assert Partition.murmur2(key) == String.to_integer(hash), "murmur2 for #{channel_id}"

      for {n, expected} <- Enum.zip(counts, parts) do
        assert Partition.partition(channel_id, n) == String.to_integer(expected),
               "partition(#{channel_id}, #{n})"
      end
    end

    assert length(rows) > 200
  end

  test "rejects a non-positive partition count" do
    assert_raise FunctionClauseError, fn -> Partition.partition(1, 0) end
  end
end

# SPDX-License-Identifier: AGPL-3.0-only
defmodule XmsgGateway.MixProject do
  use Mix.Project

  def project do
    [
      app: :xmsg_gateway,
      version: "0.0.0",
      elixir: "~> 1.17",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps()
    ]
  end

  def application do
    [extra_applications: [:logger, :inets, :ssl], mod: {XmsgGateway.Application, []}]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:bandit, "~> 1.6"},
      {:plug, "~> 1.16"},
      {:websock_adapter, "~> 0.5"},
      {:protobuf, "~> 0.14"},
      {:gnat, "~> 1.8"},
      {:brod, "~> 4.0"},
      {:mint_web_socket, "~> 1.0", only: :test}
    ]
  end
end

# SPDX-License-Identifier: AGPL-3.0-only
defmodule XmsgGateway.MixProject do
  use Mix.Project

  def project do
    [
      app: :xmsg_gateway,
      version: "0.0.0",
      elixir: "~> 1.17",
      start_permanent: Mix.env() == :prod,
      deps: []
    ]
  end

  def application do
    [extra_applications: [:logger]]
  end
end

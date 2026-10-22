# SPDX-License-Identifier: AGPL-3.0-only
# :kafka tests need a real broker: run them with `mix test --only kafka`.
ExUnit.start(exclude: [:kafka])

# Contributing to xmsg

1. **CLA required.** Before a pull request can be merged, you must sign the xmsg Contributor License Agreement. (The CLA text and the CLA bot are pending legal review; see `docs/adr/0022-open-core-licensing.md`.)
2. **SPDX headers.** Every new source file starts with an `SPDX-License-Identifier` line:
   - `AGPL-3.0-only` for server code,
   - `Apache-2.0` for `proto/`, `spec/`, `clients/`, SDKs,
   - `LicenseRef-xmsg-EE` for anything under an `ee/` directory.
3. **Core never imports enterprise code.** Code outside `ee/` must not reference anything inside `ee/`; CI checks this.
4. **Run `make test` and `make lint`** before opening a pull request.
5. **Architecture changes** need an ADR in `docs/adr/`.

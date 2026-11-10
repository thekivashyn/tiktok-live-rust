# Protocol reference

This experimental Rust implementation uses protocol observations and the direct guest
transport approach from [talkaboutdesign/tiklive](https://github.com/talkaboutdesign/tiklive),
commit `a290dd420c71f36e0af9ac2394cb893436ec9daa` (2026-10-30).
References: `packages/core/src/LiveConnection.ts`, `TikTokWeb.ts`,
`proto/frames.ts`, and `proto/messages.ts`. The implementation is new Rust code;
the MIT notice is retained for the adapted transport and wire-model material.

No full TikTok-Live-Connector or tiktok-live-proto AGPL schema files are vendored here.
The exploratory review also covered nglmercer/tiktok-signer; none of its signing
bundle, runtime or schema code is included in this experiment.

The isolated `benchmarks/connector` npm project pins `tiktok-live-connector` 2.5.0
for comparative testing. That dependency declares AGPL-3.0 and retains its own
upstream notices inside the installed package. It is not linked into or required
by the Rust engine; its source and schema files are not vendored in the Rust crate.

The 0.8.0 identity/like extension uses an explicit, selected interoperability
field table authored in `scripts/generate_metadata_models.py`. Field numbers and
wire types were checked against the pinned `tiktok-live-proto` 0.2.4 v3 decoder
used by connector 2.5.0. The test-only oracle independently decodes the same
synthetic and LIVE bytes with that installed package. Its generated upstream
implementation and full User schema are not copied into or linked by the Rust
engine. Generated Rust metadata models, validation and normalization are local
implementation code; the installed test dependencies retain their own notices.

## tiklive MIT license

Copyright (c) 2026 Talkabout Design LLC

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

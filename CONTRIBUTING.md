# Contributing to HyParView

Thanks for your interest. This project is small and focused — it implements
exactly one thing: the HyParView membership protocol. Contributions that fit
that scope are very welcome.

## Scope

In scope:
- Correctness fixes against the [DSN 2007 paper](https://www.dpss.inesc-id.pt/~ler/reports/dsn07-leitao.pdf)
- Performance improvements to the protocol implementation
- Test coverage (especially property-based tests)
- Documentation, examples, diagrams
- New transport implementations (must implement `HyParView.Transport`)

Out of scope (we will politely decline):
- Broadcast layers (Plumtree etc.) — that belongs in a separate library
- Distributed registry / supervisor primitives — see Horde
- TLS / authentication for the default transport — wrap the transport behaviour
- Application-level RPC primitives

## Developer setup

```sh
asdf install                  # uses .tool-versions (Elixir 1.19, OTP 28)
mix deps.get
mix check                     # format + credo + dialyzer + test
```

`mix check` is the gate that CI runs. If it passes locally it should pass in CI.

## Pull request workflow

1. Fork and create a topic branch off `main`.
2. Keep changes focused. One logical change per PR.
3. Add tests. New behaviour without a property or example test will be asked for.
4. Run `mix check` before pushing.
5. Open a PR with a description that explains *why*, not just *what*.

## Developer Certificate of Origin (DCO)

All commits must carry a `Signed-off-by` line certifying the [Developer
Certificate of Origin](https://developercertificate.org/). This is a lightweight
alternative to a CLA — by signing off, you assert that you have the right to
contribute the change under the project's license.

The easiest way is to use `git commit -s`, which appends:

```
Signed-off-by: Your Name <you@example.com>
```

Make sure your `user.name` and `user.email` are set in your local git config.

## Commit messages

Short imperative subject (under 72 chars), blank line, then a body that explains
the *why*. Reference issues with `Fixes #N` where applicable.

## Code style

- `mix format` is authoritative.
- `credo --strict` should pass.
- Public functions need `@doc` and `@spec`.
- Modules need `@moduledoc`.
- Prefer pattern matching and pipelines over nested case.

## Releases

Maintainers cut releases by tagging `vX.Y.Z`, updating `CHANGELOG.md`, and
running `mix hex.publish`. Pre-1.0, the public API may break across minor
versions but every break will be called out in the changelog.

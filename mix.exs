defmodule HyParView.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/b-erdem/hyparview"
  @description """
  HyParView membership protocol — a clean, BEAM-native implementation of the
  hybrid partial-view membership protocol from Leitão, Pereira, and Rodrigues
  (DSN 2007). Provides bounded partial views and reactive failure handling
  for large-scale peer-to-peer overlays.
  """

  def project do
    [
      app: :hyparview,
      version: @version,
      elixir: "~> 1.19",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      consolidate_protocols: Mix.env() != :test,
      deps: deps(),
      description: @description,
      package: package(),
      docs: docs(),
      source_url: @source_url,
      homepage_url: @source_url,
      dialyzer: dialyzer(),
      aliases: aliases()
    ]
  end

  def cli do
    [
      preferred_envs: [
        check: :test,
        "ci.lint": :test,
        "ci.test": :test
      ]
    ]
  end

  def application do
    [
      extra_applications: [:logger, :crypto],
      mod: {HyParView.Application, []}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:telemetry, "~> 1.3"},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:stream_data, "~> 1.1", only: [:dev, :test]}
    ]
  end

  defp package do
    [
      maintainers: ["Baris Erdem"],
      licenses: ["Apache-2.0"],
      links: %{
        "GitHub" => @source_url,
        "Paper" => "https://www.dpss.inesc-id.pt/~ler/reports/dsn07-leitao.pdf"
      },
      files: ~w(lib mix.exs README.md LICENSE CHANGELOG.md)
    ]
  end

  defp docs do
    [
      main: "readme",
      source_ref: "v#{@version}",
      extras: ["README.md", "CHANGELOG.md", "CONTRIBUTING.md"],
      groups_for_modules: [
        Core: [HyParView, HyParView.Peer, HyParView.State],
        Server: [HyParView.Server, HyParView.Supervisor, HyParView.Application],
        Transport: [HyParView.Transport, HyParView.Transport.TCP, HyParView.Transport.Test]
      ]
    ]
  end

  defp dialyzer do
    [
      plt_add_apps: [:mix, :ex_unit],
      flags: [:error_handling, :missing_return, :extra_return, :underspecs],
      ignore_warnings: ".dialyzer_ignore.exs"
    ]
  end

  defp aliases do
    [
      check: ["format --check-formatted", "credo --strict", "dialyzer", "test"],
      "ci.lint": ["format --check-formatted", "credo --strict"],
      "ci.test": ["test --warnings-as-errors --cover"]
    ]
  end
end

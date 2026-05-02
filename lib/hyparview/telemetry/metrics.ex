defmodule HyParView.Telemetry.Metrics do
  @moduledoc """
  Pre-built `Telemetry.Metrics` definitions for the events emitted by
  `HyParView.Server`. Drop them into a metrics reporter
  (`telemetry_metrics_prometheus`, `telemetry_metrics_statsd`, etc.) and
  you get observability out of the box.

  ## Setup

  Add `:telemetry_metrics` and a reporter to your deps:

      def deps do
        [
          {:hyparview, "~> 0.2"},
          {:telemetry_metrics, "~> 1.0"},
          {:telemetry_metrics_prometheus, "~> 1.1"}
        ]
      end

  Then add the reporter to your supervision tree:

      def start(_type, _args) do
        children = [
          {TelemetryMetricsPrometheus, metrics: HyParView.Telemetry.Metrics.metrics()},
          # ... rest of your tree
        ]

        Supervisor.start_link(children, strategy: :one_for_one)
      end

  ## What you get

    * `hyparview.peer.added.count` — counter of peers added to the active
      view, tagged by `:view` (`:active`).
    * `hyparview.peer.removed.count` — counter of peers removed from the
      active view, tagged by `:view`.
    * `hyparview.send.count` — counter of outbound messages, tagged by
      the message struct module (`:message`).
    * `hyparview.send_error.count` — counter of outbound send failures,
      tagged by `:message` and `:reason`.

  ## Custom prefix

  If you started `HyParView.Server` with a non-default `:telemetry_prefix`,
  pass the same list here:

      HyParView.Telemetry.Metrics.metrics([:my_app, :hyparview])
  """

  @doc """
  Return all metric definitions for HyParView's events under `prefix`.

  Default prefix is `[:hyparview]`.

  Requires the `:telemetry_metrics` package to be present at runtime.
  """
  if Code.ensure_loaded?(Telemetry.Metrics) do
    @spec metrics([atom()]) :: [Telemetry.Metrics.t()]
    def metrics(prefix \\ [:hyparview]) when is_list(prefix) do
      [
        Telemetry.Metrics.counter(
          prefix ++ [:peer, :added, :count],
          event_name: prefix ++ [:peer, :added],
          measurement: :count,
          description: "Peers added to the active view.",
          tags: [:view]
        ),
        Telemetry.Metrics.counter(
          prefix ++ [:peer, :removed, :count],
          event_name: prefix ++ [:peer, :removed],
          measurement: :count,
          description: "Peers removed from the active view.",
          tags: [:view]
        ),
        Telemetry.Metrics.counter(
          prefix ++ [:send, :count],
          event_name: prefix ++ [:send],
          measurement: fn _measurements -> 1 end,
          description: "Outbound HyParView protocol messages sent.",
          tags: [:message]
        ),
        Telemetry.Metrics.counter(
          prefix ++ [:send_error, :count],
          event_name: prefix ++ [:send_error],
          measurement: fn _measurements -> 1 end,
          description: "Outbound HyParView protocol message send failures.",
          tags: [:message, :reason]
        )
      ]
    end
  else
    @spec metrics([atom()]) :: no_return()
    def metrics(_prefix \\ [:hyparview]) do
      raise """
      HyParView.Telemetry.Metrics requires :telemetry_metrics. Add to your deps:

          {:telemetry_metrics, "~> 1.0"}
      """
    end
  end
end

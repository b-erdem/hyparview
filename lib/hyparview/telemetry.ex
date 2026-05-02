defmodule HyParView.Telemetry do
  @moduledoc """
  Telemetry events emitted by `HyParView.Server`.

  All events are emitted under the configured prefix (default
  `[:hyparview]`); set `config :hyparview, :telemetry_prefix, [:my_app, :hyparview]`
  to override globally, or pass `:telemetry_prefix` to `HyParView.start_link/1`
  for per-server overrides.

  ## Events

    | Event | Measurements | Metadata |
    | ----- | ------------ | -------- |
    | `[:peer, :added]` | `%{count: 1}` | `%{peer: Peer.t(), view: :active}` |
    | `[:peer, :removed]` | `%{count: 1}` | `%{peer: Peer.t(), view: :active}` |
    | `[:send]` | `%{}` | `%{target: Peer.t(), message: module()}` |
    | `[:send_error]` | `%{}` | `%{target: Peer.t(), message: module(), reason: term()}` |

  ## Attaching handlers

      :telemetry.attach_many(
        "my-handler",
        [
          [:hyparview, :peer, :added],
          [:hyparview, :peer, :removed]
        ],
        &MyApp.Telemetry.handle_event/4,
        nil
      )

      def handle_event([:hyparview, :peer, :added], _measurements, metadata, _config) do
        Logger.info("HyParView added \#{metadata.peer.id}")
      end
  """

  @typedoc "Standard telemetry handler measurements map."
  @type measurements :: map()

  @typedoc "Standard telemetry handler metadata map."
  @type metadata :: map()

  @doc """
  Build the full event name from a relative path and the configured prefix.

  Used internally by `HyParView.Server`; exposed here for testing handlers
  that observe events.

  ## Examples

      iex> HyParView.Telemetry.event([:hyparview], [:peer, :added])
      [:hyparview, :peer, :added]

      iex> HyParView.Telemetry.event([:my_app, :hp], [:send])
      [:my_app, :hp, :send]
  """
  @spec event([atom()], [atom()]) :: [atom()]
  def event(prefix, suffix) when is_list(prefix) and is_list(suffix) do
    prefix ++ suffix
  end

  @doc """
  All event paths emitted by the library, *relative to the prefix*.

  Useful for setting up observability dashboards that subscribe to all
  events:

      events = Enum.map(
        HyParView.Telemetry.event_paths(),
        &HyParView.Telemetry.event([:hyparview], &1)
      )

      :telemetry.attach_many("hyparview-all", events, handler, nil)
  """
  @typedoc "An event-path atom emitted by HyParView."
  @type event_atom :: :peer | :added | :removed | :send | :send_error

  @spec event_paths() :: nonempty_list(nonempty_list(event_atom()))
  def event_paths do
    [
      [:peer, :added],
      [:peer, :removed],
      [:send],
      [:send_error]
    ]
  end
end

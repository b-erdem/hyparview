defmodule HyParView.Transport.Test do
  @moduledoc """
  In-process transport: no sockets, no serialization. Messages are passed
  directly between BEAM processes registered in a shared `Registry`.

  Used by the library's own integration tests. Useful for application
  tests too — start your `HyParView.Server` with `transport:
  HyParView.Transport.Test` to exercise membership without spinning up
  TCP listeners.

  ## Wiring

  A single named `Registry` (`HyParView.Transport.Test.Registry`) tracks
  every running test node by its `Peer.address`. When `send_message/3` is
  called, we look up the recipient in the registry and call its delivery
  callback directly.

  Concurrent test runs share the registry — each test should use unique
  peer addresses (typically by tagging with `make_ref/0`).
  """

  @behaviour HyParView.Transport

  alias HyParView.{Messages, Peer}

  @registry __MODULE__.Registry

  @doc """
  Start the shared registry. Called from `test_helper.exs`.
  """
  @spec start_link() :: GenServer.on_start()
  def start_link do
    Registry.start_link(keys: :unique, name: @registry)
  end

  @doc "Child spec for placing in a supervision tree."
  @spec child_spec(term()) :: Supervisor.child_spec()
  def child_spec(_), do: Registry.child_spec(keys: :unique, name: @registry)

  @impl HyParView.Transport
  def listen(%Peer{address: address}, deliver) when is_function(deliver, 2) do
    case Registry.register(@registry, address, deliver) do
      {:ok, _owner} -> {:ok, %{address: address}}
      {:error, _} = err -> err
    end
  end

  @impl HyParView.Transport
  def send_message(_state, %Peer{address: address} = from, %_{} = message) do
    case Registry.lookup(@registry, address) do
      [{_pid, deliver}] ->
        deliver.(from, message)
        :ok

      [] ->
        {:error, :not_listening}
    end
  end

  @impl HyParView.Transport
  def close(%{address: address}) do
    try do
      Registry.unregister(@registry, address)
    catch
      :exit, _ -> :ok
    end

    :ok
  end

  @doc false
  @spec deliver_to(Peer.address(), Peer.t(), Messages.t()) :: :ok | {:error, :not_listening}
  def deliver_to(address, %Peer{} = from, message) do
    case Registry.lookup(@registry, address) do
      [{_pid, deliver}] ->
        deliver.(from, message)
        :ok

      [] ->
        {:error, :not_listening}
    end
  end
end

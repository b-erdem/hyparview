defmodule HyParView.Telemetry.MetricsTest do
  use ExUnit.Case, async: true

  alias HyParView.Telemetry.Metrics

  describe "metrics/1 (default prefix)" do
    setup do
      [definitions: Metrics.metrics()]
    end

    test "returns four metric definitions", %{definitions: definitions} do
      assert length(definitions) == 4
      assert Enum.all?(definitions, &is_struct(&1, Telemetry.Metrics.Counter))
    end

    test "every definition uses the [:hyparview] event prefix", %{definitions: definitions} do
      for d <- definitions do
        assert hd(d.event_name) == :hyparview
      end
    end

    test "expected metric names are present", %{definitions: definitions} do
      names = MapSet.new(definitions, & &1.name)

      assert MapSet.subset?(
               MapSet.new([
                 [:hyparview, :peer, :added, :count],
                 [:hyparview, :peer, :removed, :count],
                 [:hyparview, :send, :count],
                 [:hyparview, :send_error, :count]
               ]),
               names
             )
    end

    test ":peer counters are tagged by :view", %{definitions: definitions} do
      [added] = Enum.filter(definitions, &(&1.name == [:hyparview, :peer, :added, :count]))
      assert :view in added.tags
    end

    test ":send counter is tagged by :message", %{definitions: definitions} do
      [send] = Enum.filter(definitions, &(&1.name == [:hyparview, :send, :count]))
      assert :message in send.tags
    end

    test ":send_error counter is tagged by :message and :reason", %{definitions: definitions} do
      [err] = Enum.filter(definitions, &(&1.name == [:hyparview, :send_error, :count]))
      assert :message in err.tags
      assert :reason in err.tags
    end
  end

  describe "metrics/1 (custom prefix)" do
    test "applies the supplied prefix to every metric name and event_name" do
      definitions = Metrics.metrics([:my_app, :hp])

      for d <- definitions do
        assert Enum.take(d.name, 2) == [:my_app, :hp]
        assert Enum.take(d.event_name, 2) == [:my_app, :hp]
      end
    end
  end
end

defmodule HyParView.ConfigTest do
  use ExUnit.Case, async: true

  alias HyParView.Config

  doctest Config

  describe "new/1" do
    test "applies all paper defaults" do
      config = Config.new()

      assert config.active_view_size == 5
      assert config.passive_view_size == 30
      assert config.arwl == 6
      assert config.prwl == 3
      assert config.shuffle_active_count == 3
      assert config.shuffle_passive_count == 4
      assert config.shuffle_interval == 30_000
      assert config.shuffle_ttl == 6
    end

    test "overrides selected fields" do
      config = Config.new(active_view_size: 4, prwl: 2)
      assert config.active_view_size == 4
      assert config.prwl == 2
      # untouched defaults
      assert config.passive_view_size == 30
      assert config.arwl == 6
    end
  end

  describe "validation" do
    test "rejects active_view_size < 1" do
      assert_raise ArgumentError, ~r/:active_view_size/, fn ->
        Config.new(active_view_size: 0)
      end
    end

    test "rejects negative passive_view_size" do
      assert_raise ArgumentError, ~r/:passive_view_size/, fn ->
        Config.new(passive_view_size: -1)
      end
    end

    test "rejects PRWL > ARWL" do
      assert_raise ArgumentError, ~r/:prwl must not exceed :arwl/, fn ->
        Config.new(arwl: 3, prwl: 5)
      end
    end

    test "accepts PRWL == ARWL (boundary)" do
      assert %Config{arwl: 4, prwl: 4} = Config.new(arwl: 4, prwl: 4)
    end

    test "rejects shuffle_interval < 1" do
      assert_raise ArgumentError, ~r/:shuffle_interval/, fn ->
        Config.new(shuffle_interval: 0)
      end
    end
  end
end

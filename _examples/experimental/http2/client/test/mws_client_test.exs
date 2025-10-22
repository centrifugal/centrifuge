defmodule MwsClientTest do
  use ExUnit.Case
  doctest MwsClient

  test "greets the world" do
    assert MwsClient.hello() == :world
  end
end

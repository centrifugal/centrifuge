defmodule MwsClient.MixProject do
  use Mix.Project

  def project do
    [
      app: :mws_client,
      version: "0.1.0",
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
    defp deps do
      [
        {:mint, "~> 1.7"},
        {:mint_web_socket, "~> 1.0"},
        {:jason, "~> 1.4"}
      ]
    end
end

defmodule MwsClient do
  require Logger
  alias Mint.WebSocket
  alias Mint.HTTP

  @default_num 100
  @default_port 8080
  @default_path "/connection/websocket"

  def main do
    host = System.get_env("TARGET_HOST") || "localhost"
    port = (System.get_env("TARGET_PORT") || "#{@default_port}") |> String.to_integer()
    path = System.get_env("TARGET_PATH") || @default_path
    num = (System.get_env("NUM") || "#{@default_num}") |> String.to_integer()
    mode = System.get_env("HTTP_MODE") || "h2"

    Logger.info("Mode: #{mode} → connecting to wss://#{host}:#{port}#{path} (#{num} sockets)")

    case mode do
      "h1" -> run_http1(host, port, path, num)
      "h2" -> run_http2(host, port, path, num)
      other -> Logger.error("Unknown MODE=#{other}, use h1 or h2")
    end
  end

  # ---------------------
  # HTTP/1.1 Upgrade
  # ---------------------
  defp run_http1(host, port, path, num) do
    parent = self()

    # Create each WebSocket connection in a separate process
    Enum.each(1..num, fn i ->
      spawn(fn ->
        {:ok, conn} =
          HTTP.connect(:https, host, port,
            protocols: [:http1],
            transport_opts: [verify: :verify_none]
          )

        {:ok, conn, ws_ref} = WebSocket.upgrade(:wss, conn, path, [])

        # Wait for the upgrade response
        http_reply_message = receive do message -> message end

        {:ok, conn, responses} = WebSocket.stream(conn, http_reply_message)

        # Extract status and headers from responses
        {status, headers} =
          Enum.reduce(responses, {nil, []}, fn
            {:status, ^ws_ref, status}, {_, headers} -> {status, headers}
            {:headers, ^ws_ref, new_headers}, {status, _} -> {status, new_headers}
            {:done, ^ws_ref}, acc -> acc
            _, acc -> acc
          end)

        {:ok, conn, ws} = WebSocket.new(conn, ws_ref, status, headers)
        payload = Jason.encode!(%{"id" => i, "connect" => %{}})
        {:ok, _ws, data} = WebSocket.encode(ws, {:text, payload})
        {:ok, conn} = WebSocket.stream_request_body(conn, ws_ref, data)

        Logger.info("[h1] started websocket id=#{i}")

        # Message loop for this connection
        loop_http1(conn, ws_ref, ws)
      end)
    end)

    :timer.sleep(:infinity)
  end

  # ---------------------
  # HTTP/1.1 message loop
  # ---------------------
  defp loop_http1(conn, ws_ref, ws) do
    receive do
      message ->
        case WebSocket.stream(conn, message) do
          {:ok, conn, [{:data, ^ws_ref, data}]} ->
            {:ok, ws, frames} = WebSocket.decode(ws, data)

            Enum.each(frames, fn
              {:text, "{}"} ->
                {:ok, _ws, data} = WebSocket.encode(ws, {:text, "{}"})
                {:ok, _conn} = WebSocket.stream_request_body(conn, ws_ref, data)
                Logger.info("[h1] pong {}")

              frame ->
                Logger.debug("[h1] received frame: #{inspect(frame)}")
            end)

            loop_http1(conn, ws_ref, ws)

          {:ok, conn, _events} ->
            loop_http1(conn, ws_ref, ws)

          {:error, conn, reason} ->
            Logger.error("[h1] Stream error: #{inspect(reason)}")
            loop_http1(conn, ws_ref, ws)
        end
    end
  end

  # ---------------------
  # HTTP/2 RFC8441
  # ---------------------
  defp run_http2(host, port, path, num) do
    # Limit streams per connection to avoid too_many_concurrent_requests
    # Most servers default to 100-250, we'll use 100 to be safe
    max_streams_per_conn = 100

    # Calculate how many connections we need
    num_connections = div(num + max_streams_per_conn - 1, max_streams_per_conn)

    Logger.info("[h2] creating #{num_connections} connections for #{num} websockets")

    # Create multiple HTTP/2 connections and spawn a process for each
    Enum.each(1..num_connections, fn conn_idx ->
      start_id = (conn_idx - 1) * max_streams_per_conn + 1
      end_id = min(conn_idx * max_streams_per_conn, num)
      count = end_id - start_id + 1

      spawn(fn ->
        {:ok, conn} =
          HTTP.connect(:https, host, port,
            protocols: [:http2],
            transport_opts: [verify: :verify_none]
          )

        conn = %{conn | server_settings: Map.put(conn.server_settings, :enable_connect_protocol, true)}
        initial_state = %{conn: conn, ws_map: %{}, pending_upgrades: %{}}

        Logger.info("[h2] connection #{conn_idx}: sending #{count} upgrade requests (id #{start_id}-#{end_id})")

        # Send all upgrade requests for this connection
        final_state = Enum.reduce(start_id..end_id, initial_state, fn i, state ->
          case WebSocket.upgrade(:ws, state.conn, path, []) do
            {:ok, conn, ws_ref} ->
              Logger.debug("[h2] conn #{conn_idx}: sent upgrade request id=#{i}, ref=#{inspect(ws_ref)}")
              %{state | conn: conn, pending_upgrades: Map.put(state.pending_upgrades, ws_ref, i)}

            {:error, conn, reason} ->
              Logger.error("[h2] conn #{conn_idx}: upgrade failed id=#{i}: #{inspect(reason)}")
              %{state | conn: conn}

            {:error, reason} ->
              Logger.error("[h2] conn #{conn_idx}: upgrade failed (2-tuple) id=#{i}: #{inspect(reason)}")
              state
          end
        end)

        loop_http2(final_state)
      end)
    end)

    :timer.sleep(:infinity)
  end

  # ---------------------
  # HTTP/2 message loop
  # ---------------------
  defp loop_http2(state) do
    receive do
      message ->
        case WebSocket.stream(state.conn, message) do
          {:ok, conn, events} ->
            state = %{state | conn: conn}
            state = handle_events(events, state)
            loop_http2(state)

          {:error, conn, reason} ->
            Logger.error("Stream error: #{inspect(reason)}")
            loop_http2(%{state | conn: conn})
        end
    end
  end

  # ---------------------
  # Handle incoming WS frames
  # ---------------------
  defp handle_events(events, state) do
    Enum.reduce(events, state, fn
      {:status, ref, status}, st ->
        Logger.debug("[h2] received status #{status} for ref=#{inspect(ref)}")
        Map.put(st, {:status, ref}, status)

      {:headers, ref, headers}, st ->
        Logger.debug("[h2] received headers for ref=#{inspect(ref)}")
        st = Map.put(st, {:headers, ref}, headers)

        # For HTTP/2 WebSockets, create the WebSocket immediately after headers
        # Don't wait for {:done, ref} as the stream needs to stay open
        if Map.has_key?(st.pending_upgrades, ref) do
          id = st.pending_upgrades[ref]
          status = Map.get(st, {:status, ref})

          Logger.info("[h2] upgrade complete for id=#{id}, status=#{status}")

          case WebSocket.new(st.conn, ref, status, headers) do
            {:ok, conn, ws} ->
              payload = Jason.encode!(%{"id" => id, "connect" => %{}})
              Logger.debug("[h2] encoded payload: #{payload}")
              {:ok, ws, data} = WebSocket.encode(ws, {:text, payload})
              Logger.debug("[h2] encoded frame data (#{byte_size(data)} bytes): #{inspect(data, limit: 100)}")

              conn = case WebSocket.stream_request_body(conn, ref, data) do
                {:ok, conn} ->
                  Logger.info("[h2] opened websocket id=#{id}, sent connect frame")
                  conn

                {:error, conn, reason} ->
                  Logger.error("[h2] failed to send connect frame for id=#{id}: #{inspect(reason)}")
                  conn
              end

              st
              |> Map.put(:conn, conn)
              |> Map.put(:ws_map, Map.put(st.ws_map, ref, ws))
              |> Map.put(:pending_upgrades, Map.delete(st.pending_upgrades, ref))
              |> Map.delete({:status, ref})
              |> Map.delete({:headers, ref})

            {:error, conn, reason} ->
              Logger.error("[h2] WebSocket.new failed for id=#{id}: #{inspect(reason)}")
              %{st | conn: conn, pending_upgrades: Map.delete(st.pending_upgrades, ref)}
          end
        else
          st
        end

      {:done, ref}, st ->
        # Just log it, WebSocket setup already happened in headers event
        Logger.debug("[h2] received done for ref=#{inspect(ref)}")
        st

      {:data, ref, data}, st ->
        case Map.get(st.ws_map, ref) do
          nil ->
            Logger.debug("[h2] received data for unknown ref=#{inspect(ref)}")
            st

          ws ->
            {:ok, ws, frames} = WebSocket.decode(ws, data)

            Enum.each(frames, fn
              {:text, "{}"} ->
                {:ok, _ws, data} = WebSocket.encode(ws, {:text, "{}"})
                {:ok, _conn} = WebSocket.stream_request_body(st.conn, ref, data)
                Logger.info("[h2] pong {} ref=#{inspect(ref)}")

              frame ->
                Logger.debug("[h2] received frame: #{inspect(frame)}")
            end)

            %{st | ws_map: Map.put(st.ws_map, ref, ws)}
        end

      event, st ->
        Logger.debug("[h2] unhandled event: #{inspect(event)}")
        st
    end)
  end
end

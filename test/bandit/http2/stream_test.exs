defmodule Bandit.HTTP2.StreamTest do
  use ExUnit.Case, async: true

  alias Bandit.HTTP2.Stream
  alias Bandit.HTTPTransport

  defp rejected_stream(bytes_remaining) do
    %Stream{
      connection_pid: self(),
      stream_id: 1,
      state: :local_closed,
      bytes_remaining: bytes_remaining
    }
  end

  test "cleanup counts all queued DATA frames before END_STREAM" do
    Stream.deliver_data(self(), "abc", false)
    Stream.deliver_data(self(), "def", true)

    assert %Stream{state: :closed, bytes_remaining: 0} =
             HTTPTransport.ensure_completed(rejected_stream(6))

    refute_receive {:bandit, _}
    refute_receive {{:send_recv_window_update, _}, _}
    refute_receive {{:send_rst_stream, _}, _}
  end

  test "cleanup counts queued DATA before trailers" do
    Stream.deliver_data(self(), "abcdef", false)
    Stream.deliver_headers(self(), [], true)

    assert %Stream{state: :closed, bytes_remaining: 0} =
             HTTPTransport.ensure_completed(rejected_stream(6))

    refute_receive {:bandit, _}
    refute_receive {{:send_recv_window_update, _}, _}
  end

  test "cleanup cancels an incomplete upload without granting more stream credit" do
    Stream.deliver_data(self(), "abc", false)
    HTTPTransport.ensure_completed(rejected_stream(6))

    assert_received {{:send_rst_stream, 0}, 1}
    refute_received {:bandit, _}
    refute_received {{:send_recv_window_update, _}, _}
  end

  test "cleanup still rejects a mismatched content length" do
    for length <- [5, 7] do
      Stream.deliver_data(self(), "abc", false)
      Stream.deliver_data(self(), "def", true)

      assert_raise Bandit.HTTP2.Errors.StreamError,
                   "Received END_STREAM with byte still pending",
                   fn -> HTTPTransport.ensure_completed(rejected_stream(length)) end

      refute_received {:bandit, _}
    end
  end

  test "cleanup accepts a body without content length" do
    Stream.deliver_data(self(), "abc", false)
    Stream.deliver_data(self(), "def", true)

    assert %Stream{state: :closed, bytes_remaining: nil} =
             HTTPTransport.ensure_completed(rejected_stream(nil))

    refute_received {:bandit, _}
  end
end

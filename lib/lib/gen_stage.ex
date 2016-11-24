alias Experimental.GenStage

defmodule GenStageExample3 do

  @read_modes [:read, :binary, :raw, :read_ahead] # This modes showed best performance on benchmarks
  @buf_size 65_535 # Read files by 64 KB by-default

  defmodule Splitter do
    @moduledoc """
    This GenStage example is meant to illustrate the splitting of an output flow of
    events - a list of integers - to more than one stage. This is accomplished by
    using the PartitionDispatcher dispatcher and a "splitter" function that shunts
    even integers to one partition and odd integers to another partition. For a more
    detailed explanation, please see this blog [post](www.elixirfbp.com)
    """
    use GenStage

    def init(_) do
      {:producer_consumer, %{}, []}
    end

    @doc """
    Simply pass input events onto the partition dispatcher
    """
    def handle_events([events], _from, state) when is_binary(events) do

      IO.inspect "asd"
       loan = events
       |> _decode()
#       |> Keyword.get(:acc)

#    loan = BSON.Decoder.decode(events)

#       loan = %{"asd" => "asd"}
      IO.inspect {"decoded", loan}


      {:noreply, [loan], state}
    end

    def handle_info({process, {:producer, :halted} = status}, state) do
      IO.inspect {"halted", process, status, state}
      {:noreply, [], state}
    end
    def handle_info({process, {:producer, :done} = status}, state) do
      IO.inspect {"done", process, status, state}

      {:noreply, [], state}
    end

#    defp decode(val), do: Bson.decode(val)
    defp decode(<<size::32-little-signed, _::binary>> = doc) when byte_size(doc) == size do
      _decode(doc)
    end

    defp decode(<<size::32-little-signed, _::binary>> = acc) when byte_size(acc) > size do
      <<doc::binary-size(size), next::binary>> = acc

      _decode(doc)
    end
#    defp decode(<<_::binary>> = acc) do
#      case Enum.at(io, index, :none) do
#        data when is_binary(data) ->
#          iterate(io, acc <> data, func, index + 1)
#        ^index ->
#          io
#      end
#    end
    defp _decode(doc) do
      try do
        BSON.Decoder.decode(doc)
      rescue
        _ -> {:error, :corrupted_document}
      end
    end
  end

  defmodule Ticker do
    use GenStage
    def init(state) do
      {:consumer, state}
    end
    def handle_events([loan], _from, {sleeping_time, name} = state) when is_map(loan) do
      mem()
#      IO.inspect BSON.Decoder.decode(events)

      IO.puts "Ticker(#{name}) events: #{inspect loan}"

#      Process.sleep(sleeping_time)
      {:noreply, [], state}
    end

    def handle_events(err, _from, {sleeping_time, name} = state) do
      IO.puts "Ticker(#{name}) error: #{inspect err}"
      {:noreply, [], state}
    end

    def mem do
      IO.inspect(:erlang.memory(:total) / 1024/1024)
    end
  end

#  stream = BSONEach.File.stream("test/100000.bson")
#  stream = BSONEach.File.stream("test/5000.bson")
  stream = File.stream!("test/3.bson", @read_modes, @buf_size)
#  IO.inspect stream
  stream = BSONEach.stream("test/3.bson")
#  |> Stream.map(&(&1))
  |> Stream.map(&(IO.inspect &1))

  "test/3.bson"
  |> BSONEach.Stream.resource
  |> Enum.map(fn document ->
    IO.inspect document
  end)

  IO.inspect stream

  {:ok, inport} = a = GenStage.from_enumerable(stream)
  IO.inspect a

  size_s = 3
  size_z = 3
  for _ <- 1..size_s do
    {:ok, splitter}  = GenStage.start_link(Splitter, 0)
    GenStage.sync_subscribe(splitter, to: inport, max_demand: 1)

    for z <- 1..size_z do
      {:ok, w}     = GenStage.start_link(Ticker, {500, z})
      GenStage.sync_subscribe(w, to: splitter, partition: 0, max_demand: 1)
    end
  end

#IO.inspect stream


#alias Experimental.Flow
#res = stream
#|> Flow.from_enumerable()
##|> Flow.flat_map(&String.split(&1, " "))
#|> Flow.partition()
#|> Flow.reduce(fn -> %{} end, fn word, acc ->
#IO.inspect(Bson.decode(word))
##  Map.update(acc, word, 1, & &1 + 1)
#end)
#|> Enum.to_list()

#IO.inspect res
#  alias Experimental.Flow
#  stream
#  |> Flow.from_enumerable()
#  |> Flow.flat_map(&String.split(&1, " "))
#  |> Flow.reduce(fn -> %{} end, fn value, acc ->
#    IO.inspect value
#
#    Map.update(acc, value, 1, & &1)
#  end)

IO.puts("")
IO.puts("==============================")
IO.puts("end")

  Process.sleep(:infinity)


    def handle_events([events], _from, state) when is_binary(events) do
      {:noreply, [BSON.Decoder.decode(events)], state}
    end

end
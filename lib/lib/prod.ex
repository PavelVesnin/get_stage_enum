defmodule BatchProducer do
  @moduledoc """
  GenStage producer for Loan
  """
  alias Experimental.GenStage

  use GenStage

  def start_link do
    GenStage.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def init(:ok) do
    {:producer, :ok, buffer_size: 10_000}
  end

#  def handle_call({:notify, event}, _from, state) do
#    {:reply, :ok, [event], state}
#  end
#
#  def handle_demand(_demand, state) do
#    {:noreply, [], state}
#  end

end

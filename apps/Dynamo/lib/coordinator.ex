defmodule Dynamo.Coordinator do
  @moduledoc """
  An implementation of the Dynamo coordinator.
  """
  import Emulation, only: [send: 2]

  import Kernel,
         except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]
  alias __MODULE__

  defstruct(
    key_range: nil,
    node_status: nil,
  )
  require Fuzzers
  require Logger

  @spec coordinator() :: no_return()
  def coordinator() do

  end
end

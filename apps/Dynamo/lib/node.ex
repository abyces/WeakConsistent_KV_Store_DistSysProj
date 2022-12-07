defmodule Dynamo.Node do
  @moduledoc """
  An implementation of the Dynamo.
  """
  import Emulation,
    only: [send: 2, timer: 1, now: 0, whoami: 0, cancel_timer: 1]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  require Fuzzers
  require Logger

  # This structure contains all the process state
  # required by the Raft protocol.
  defstruct(
    node_list: nil,
    replica_count: nil,
    vector_clock: nil,
    gossip_table: nil,
    data: nil,
    heartbeat_timeout: nil,
    heartbeat_timer: nil,
    fail_timeout: nil,
    fail_timer: nil,
    gossip_cleanup_timeout: nil,
    gossip_cleanup_timer: nil,

  )

  @doc """
  Create state for an initial Raft cluster. Each
  process should get an appropriately updated version
  of this state.
  """
  @spec new_configuration(
          [atom()],
          atom(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer()
        ) :: %Raft{}
  def new_configuration(
        view,
        leader,
        min_election_timeout,
        max_election_timeout,
        heartbeat_timeout
      ) do
    %Raft{
      view: view,
      current_leader: leader,
      min_election_timeout: min_election_timeout,
      max_election_timeout: max_election_timeout,
      heartbeat_timeout: heartbeat_timeout,
      # Start from term 1
      current_term: 1,
      voted_for: nil,
      log: [],
      commit_index: 0,
      last_applied: 0,
      is_leader: false,
      next_index: nil,
      match_index: nil,
      queue: :queue.new()
    }
  end

  # Save a handle to the election timer.
  @spec save_election_timer(%Raft{}, reference()) :: %Raft{}
  defp save_election_timer(state, timer) do
    %{state | election_timer: timer}
  end

  # Save a handle to the hearbeat timer.
  @spec save_heartbeat_timer(%Raft{}, reference()) :: %Raft{}
  defp save_heartbeat_timer(state, timer) do
    %{state | heartbeat_timer: timer}
  end

  # Utility function to send a message to all
  # processes other than the caller. Should only be used by leader.
  @spec broadcast_to_others(%Raft{is_leader: true}, any()) :: [boolean()]
  defp broadcast_to_others(state, message) do
    me = whoami()

    state.view
    |> Enum.filter(fn pid -> pid != me end)
    |> Enum.map(fn pid -> send(pid, message) end)
  end

  # END OF UTILITY FUNCTIONS. You should not need to (but are allowed to)
  # change any of the code above this line, but will definitely need to
  # change the code that follows.

  # This function should cancel the current
  # election timer, and set  a new one. You can use
  # `get_election_time` defined above to get a
  # randomized election timeout. You might need
  # to call this function from within your code.
  @spec reset_election_timer(%Raft{}) :: %Raft{}
  defp reset_election_timer(state) do
    old_election_timer = state.election_timer

    state =
      case old_election_timer do
        nil ->
          save_election_timer(
            state,
            timer(get_election_time(state))
          )

        t ->
          cancel_timer(old_election_timer)

          save_election_timer(
            state,
            timer(get_election_time(state))
          )
      end

    state
  end

  # This function should cancel the current
  # hearbeat timer, and set  a new one. You can
  # get heartbeat timeout from `state.heartbeat_timeout`.
  # You might need to call this from your code.
  @spec reset_heartbeat_timer(%Raft{}) :: %Raft{}
  defp reset_heartbeat_timer(state) do
    old_heartbeat_timer = state.heartbeat_timer

    state =
      case old_heartbeat_timer do
        nil ->
          save_heartbeat_timer(state, timer(state.heartbeat_timeout))

        t ->
          cancel_timer(old_heartbeat_timer)
          save_heartbeat_timer(state, timer(state.heartbeat_timeout))
      end

    state
  end

  @doc """
  This function transitions a process so it is
  a follower.
  """
  @spec node_init() :: no_return()
  def node_init() do

  end

  @doc """
  This function implements the state machine for a process
  that is currently a follower.

  `extra_state` can be used to hod anything that you find convenient
  when building your implementation.
  """
  @spec node() :: no_return()
  def node() do

  end

end

defmodule Node do
  @moduledoc """
  An implementation of the Dynamo.
  """
  import Emulation,
    only: [send: 2, timer: 2, now: 0, whoami: 0, cancel_timer: 1]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  require Fuzzers
  require Logger
  require System

  # This structure contains all the process state
  # required by the Raft protocol.
  defstruct(
    node_list: nil,
    replica_count: nil,
    vector_clock: nil,
    gossip_table: nil,

    data: nil,
    hinted_data: nil,

    heartbeat_timeout: nil,
    heartbeat_timer: nil,
    gossip_check_timer: nil,
    gossip_check_timeout: nil,
    gossip_send_timer: nil,
    gossip_send_timeout: nil,
    fail_timeout: nil,
    cleanup_timeout: nil,

    R: nil,
    W: nil
  )

  @doc """
  Create state for an initial Raft cluster. Each
  process should get an appropriately updated version
  of this state.
  """
  @spec new_configuration(
          [atom()],
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer()
      ) :: %Node{}
  def new_configuration(
        node_list,
        replica_count,
        heartbeat_timeout,
        gossip_check_timeout,
        gossip_send_timeout,
        fail_timeout,
        cleanup_timeout
      ) do
    %Node{
      node_list: node_list,
      replica_count: replica_count,
      vector_clock: %{},
      gossip_table: nil,

      data: %{},
      hinted_data: %{},

      heartbeat_timeout: heartbeat_timeout,
      heartbeat_timer: nil,
      gossip_check_timer: nil,
      gossip_check_timeout: gossip_check_timeout,
      gossip_send_timer: nil,
      gossip_send_timeout: gossip_send_timeout,
      fail_timeout: fail_timeout,
      cleanup_timeout: cleanup_timeout
    }
  end

  # Utility function to send a message to all
  @spec broadcast_to_others(%Node{}, any()) :: [boolean()]
  defp broadcast_to_others(state, message) do
    me = whoami()

    state.view
    |> Enum.filter(fn pid -> pid != me end)
    |> Enum.map(fn pid -> send(pid, message) end)
  end

  # This function reset the heartbeat timer
  @spec reset_heartbeat_timer(%Node{}) :: %Node{}
  defp reset_heartbeat_timer(state) do
    if state.heartbeat_timer != nil do
      Emulation.cancel_timer(state.heartbeat_timer)
    end
    %{state | heartbeat_timer: timer(state.heartbeat_timeout, :heartbeat)}
  end

  # This function reset the gossip check timer
  @spec reset_gossip_check_timer(%Node{}) :: %Node{}
  defp reset_gossip_check_timer(state) do
    if state.gossip_check_timer != nil do
      Emulation.cancel_timer(state.gossip_check_timer)
    end
    %{state | gossip_check_timer: timer(state.gossip_check_timeout, :gossip_check)}
  end

  # This function reset the gossip send timer
  @spec reset_gossip_send_timer(%Node{}) :: %Node{}
  defp reset_gossip_send_timer(state) do
    if state.gossip_send_timer != nil do
      Emulation.cancel_timer(state.gossip_send_timer)
    end
    %{state | gossip_send_timer: timer(state.gossip_send_timeout, :gossip_send)}
  end

  # This function checks the node's gossip table periodically
  @spec gossip_table_periodical_check(%Node{}) :: %Node{}
  defp gossip_table_periodical_check(state) do
    cur_time = System.os_time(:millisecond)
    new_gossip_table = Map.new(state.gossip_table, fn {node_id, {cnt, time, stat}} ->
        if node_id != whoami() do
          case stat do
            :alive ->
              if cur_time - time > state.fail_timeout do
                {node_id, {cnt, cur_time, :failed}}
              else
                {node_id, {cnt, cur_time, :alive}}
              end

            :failed ->
              if cur_time - time > state.cleanup_timeout do
                {node_id, {cnt, cur_time, :deleted}}
              else
                {node_id, {cnt, cur_time, :alive}}
              end

            :deleted ->
              {node_id, {cnt, cur_time, :deleted}}
          end
        else
          {node_id, {cnt, time, stat}}
        end
    end)
    %{state | gossip_table: new_gossip_table}
  end

  # This function merges the node's gossip table with received gossip table
  @spec gossip_table_merge(%Node{}, map()) :: %Node{}
  defp gossip_table_merge(state, received_gossip_table) do
    cur_time = System.os_time(:millisecond)
    new_gossip_table = Map.new(state.gossip_table, fn {node_id, {cnt, time, stat}} ->
        if node_id != whoami() do
          case stat do
            :failed ->
              {node_id, {cnt, time, stat}}

            _ ->
              {ncnt, ntime, nstat} = received_gossip_table[node_id]
              if nstat != :deleted && ncnt > cnt do
                {node_id, {ncnt, cur_time, :alive}}
              else
                {node_id, {cnt, time, stat}}
              end
          end
        else
          {node_id, {cnt, time, stat}}
        end
    end)
    %{state | gossip_table: new_gossip_table}
  end

  # This function set the gossip_table with particular node_id to be :alive
  @spec gossip_table_set_alive_by_id(%Node{}, atom()) :: %Node{}
  defp gossip_table_set_alive_by_id(state, node_id) do
    {cnt, _, _} = state.gossip_table[node_id]
    %{state | gossip_table: Map.put(state.gossip_table, node_id, {cnt, System.cur_time(:millisecond), :alive})}
  end

  # This function updates the heartbeat cnt with particular node_id
  @spec gossip_table_update_heartbeat(%Node{}, atom()) :: %Node{}
  defp gossip_table_update_heartbeat(state, node_id) do
    {cnt, _, _} = state.gossip_table[node_id]
    %{state | gossip_table: Map.put(state.gossip_table, node_id, {cnt+1, System.cur_time(:millisecond), :alive})}
  end

  @spec put_data(%Node{}, non_neg_integer(), any()) :: %Node{}
  defp put_data(state, key, val) do
    %{state | data: Map.put(state.data, key, val)}
  end

  @spec get_data(%Node{}, non_neg_integer()) :: any()
  defp get_data(state, key) do
    Map.get(state.data, key)
  end

  @spec put_hinted_data(%Node{}, atom(), non_neg_integer(), any()) :: %Node{}
  defp put_hinted_data(state, original_target, key, val) do
    if Map.has_key?(state.hinted_data, original_target) do
      _updated_data = %{state.hinted_data[original_target] | key: val}
      %{state | hinted_data: Map.put(state.hinted_data, original_target, _updated_data)}
    else
      %{state | hinted_data: Map.put(state.hinted_data, original_target, %{key: val})}
    end
  end

  @spec get_hinted_data(%Node{}, atom(), non_neg_integer()) :: any()
  defp get_hinted_data(state, original_target, key) do
    case Map.has_key?(state.hinted_data, original_target) do
      true -> Map.get(state.hinted_data[original_target], key)
      false -> nil
    end
  end


  @doc """
  This function initialize a dynamo node
  """
  @spec node_init(%Node{}) :: no_return()
  def node_init(state) do
    cur_timestamp = System.os_time(:millisecond)
    gossip_table = Map.new(state.node_list, fn v -> {v, {0, cur_timestamp, :alive}} end)
    %{state |
      gossip_table: gossip_table,
      heartbeat_timer: timer(state.heartbeat_timeout, :heartbeat),
      gossip_check_timer: timer(state.gossip_check_timeout, :gossip_check),
      gossip_send_timer: timer(state.gossip_send_timeout, :gossip_send)
    }
    run_node(state)
  end

  @doc """
  This function implements the state machine for dynamo nodes
  """
  @spec run_node(%Node{}) :: no_return()
  defp run_node(state) do
    receive do
      :heartbeat ->
        broadcast_to_others(state, GossipMessage.new_gossip_message(nil))
        state = reset_heartbeat_timer(state)
        node(state)

      :gossip_check ->
        state = gossip_table_periodical_check(state)
        state = reset_gossip_check_timer(state)
        node(state)

      :gossip_send ->
        broadcast_to_others(state, GossipMessage.new_gossip_message(state.gossip_table))
        state = reset_gossip_send_timer(state)
        node(state)

      {sender,
        %GossipMessage{gossip_table: rev_gossip_table}} ->
        case rev_gossip_table do
          nil ->
            state = gossip_table_update_heartbeat(state, sender)
            run_node(state)
          _ ->
            state = gossip_table_set_alive_by_id(state, sender)
            state = gossip_table_merge(state, rev_gossip_table)
            run_node(state)
        end

      {sender,
        %CoordinateRequest{
          client: client,
          method: method,
          hint: hint,
          key: key,
          val: val
        }} ->
        case hint do
          nil ->
            case method do
              :put ->
                state = put_data(state, key, val)
                run_node(state)
              :get ->
                r_data = get_data(state, key)
                case r_data do
                  nil -> IO.puts("key not found")
                  _ -> IO.puts(r_data)
                end
                run_node(state)
            end
          origin_target ->
            case method do
              :put ->
                state = put_hinted_data(state, origin_target, key, val)
                run_node(state)
              :get ->
                r_data = get_hinted_data(state, origin_target, key)
                case r_data do
                  nil -> IO.puts("key not found")
                  _ -> IO.puts(r_data)
                end
                run_node(state)
            end
        end
    end
  end

end

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
    hash_ring: nil,
    hash_avl: nil,
    self_idx: nil,
    self_hash: nil,
    hash_to_node: nil,
    replica_count: nil,
    preference_list: nil,
    vector_clock: nil,
    gossip_table: nil,

    data: nil,
    hinted_data: nil,
    response_cnt: nil,

    heartbeat_timeout: nil,
    heartbeat_timer: nil,
    gossip_check_timer: nil,
    gossip_check_timeout: nil,
    gossip_send_timer: nil,
    gossip_send_timeout: nil,
    fail_timeout: nil,
    cleanup_timeout: nil,

    r_cnt: nil,
    w_cnt: nil
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
        cleanup_timeout,
        r_cnt,
        w_cnt
      ) do
    hash_to_node =
      node_list
      |> Enum.map(fn v -> {String.to_integer(:crypto.hash(:md5, Atom.to_string(v)) |> Base.encode16(), 16), v} end)
      |> Map.new()

    hash_avl =
      hash_to_node
      |> Enum.map(fn {k, v} -> k end)
      |> Enum.into(AVLTree.new())

    hash_ring = AVLTree.inorder_traverse(hash_avl)
    self_hash = String.to_integer(:crypto.hash(:md5, Atom.to_string(whoami())) |> Base.encode16(), 16)
    self_idx = Enum.find_index(hash_ring, fn v -> v == self_hash end)
    preference_list =
      Enum.slice(hash_ring ++ hash_ring, self_idx + 1 .. self_idx + min(replica_count + 1, length(node_list) - 1))
      |> Enum.map(fn v -> hash_to_node[v] end)

    %Node{
      node_list: node_list,
      hash_ring: hash_ring,
      hash_avl: hash_avl,
      hash_to_node: hash_to_node,
      self_idx: self_idx,
      self_hash: self_hash,
      replica_count: replica_count,
      preference_list: preference_list,
      vector_clock: %{},
      gossip_table: nil,

      data: %{},
      hinted_data: %{},
      response_cnt: %{},

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

  @spec broadcast_replica(%Node{}, any()) :: [boolean()]
  defp broadcast_replica(state, message) do
    Enum.each(state.preferenect_list, fn node -> send(node, message) end)
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
    me = whoami()
    cur_time = System.os_time(:millisecond)
    new_gossip_table = Map.new(state.gossip_table, fn {node_id, {cnt, time, stat}} ->
        if node_id != me do
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
    me = whoami()
    cur_time = System.os_time(:millisecond)
    new_gossip_table = Map.new(state.gossip_table, fn {node_id, {cnt, time, stat}} ->
        if node_id != me do
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


  # This function merges the processed context from client with local contexts
  @spec merge_context(list(), any(), list()) :: list()
  defp merge_context([head | rest], new_val, rev_vector_clock) do
    res = case rest do
      [] -> process_context_entry_with_vals(head, new_val, rev_vector_clock)
      _ -> process_context_entry_with_vals(head, new_val, rev_vector_clock) ++ merge_context(rest, new_val, rev_vector_clock)
    end
    Enum.dedup(Enum.sort(res))
  end

  @spec process_context_entry_with_vals(tuple(), any(), list()) :: list()
  defp process_context_entry_with_vals({val, vector_clk}, new_val, rev_vector_clock) do
    compare = Enum.map(rev_vector_clock, fn {v, vec_clk} -> {v, vec_clk, VectorClock.compare_vectors(vector_clk, vec_clk)} end)
    res = []
    res = res ++ case Enum.any?(compare, fn {v, vec_clk, cmp} -> cmp == :before end) do
      true -> []
      false -> [vector_clk]
    end
    res = res ++
          compare
          |> Enum.filter(fn {v, vec_clk, cmp} -> cmp != :before end)
          |> Enum.map(fn {v, vec_clk, cmp} -> {v, vec_clk} end)
  end

  # This function process each context entry([map, map, ..]) from client
  # if me in context entry -> +1
  # else add(me => 1)
  @spec update_context_from_client(list(), list()) :: list()
  defp update_context_from_client(context, local_data_list) do
    me = whoami()
    Enum.map(process_context_from_client(context, local_data_list), fn {context, cmp} ->
      case Map.has_key?(context, me) do
        true ->
          Map.put(context, me, Map.get(context, me) + 1)
        false ->
          Map.put(context, me, 1)
      end
    end)
  end

  @spec process_context_from_client(list(), list()) :: list()
  defp process_context_from_client([head | rest], local_data_list) do
    res = case rest do
      [] -> [process_context_entry_without_vals(head, local_data_list)]
      _ -> [process_context_entry_without_vals(head, local_data_list)] ++ process_context_from_client(rest, local_data_list)
    end
  end

  # This function compares each new context entry(vector clock) to each of original list [{v1, clk1}, ...]
  # and returns context without :before.
  @spec process_context_entry_without_vals(tuple(), list()) :: list()
  defp process_context_entry_without_vals(context_entry, local_data_list) do
    compare = Enum.map(local_data_list, fn {v, vec_clk} -> {context_entry, VectorClock.compare_vectors(context_entry, vec_clk)} end)
    Enum.filter(compare, fn {context, cmp} -> cmp != :before end)
  end


  @spec make_vector_clock_full_length(%Node{}, [tuple()]) :: [tuple()]
  defp make_vector_clock_full_length(state, context) do
    case context do
      [] -> []
      [{val, clk} | tl] -> [{val, state.node_list
                           |> Enum.map(fn node ->
                                  case Map.hasKey?(clk, node) do
                                    true -> {node, clk[node]}
                                    false -> {node, 0}
                                  end
                              end)
                           |> Map.new()}] ++ make_vector_clock_full_length(state, tl)

    end
  end

  @spec remove_zero_from_vector_clock([tuple()]) :: [tuple()]
  defp remove_zero_from_vector_clock(context) do
    case context do
      [] -> []
      [{val, clk} | tl] -> [{val, clk |> Enum.filter(fn {_, v} -> v != 0 end) |> Map.new()}] ++ remove_zero_from_vector_clock(tl)
    end
  end


  @spec put_key_from_client(%Node{}, non_neg_integer(), any(), map()) :: %Node{}
  defp put_key_from_client(state, key, val, context) do
    me = whoami()
    processed_context = case Map.has_key?(context, me) do
      true -> Map.put(context, me, Map.get(context, me) + 1)
      false -> Map.put(context, me, 1)
    end
    case Map.has_key?(state.data, key) do
      false ->
        %{state | data: Map.put(state.data, key, [{val, processed_context}])}
      true ->
        new_data_list = merge_context(Map.get(state.data, key), val, [processed_context])
        %{state | data: Map.put(state.data, key, new_data_list)}
    end
  end


  @spec put_hinted_data(%Node{}, atom(), non_neg_integer(), any(), list()) :: %Node{}
  defp put_hinted_data(state, original_target, key, val, context) do
    me = whoami()
    processed_context = case Map.has_key?(context, me) do
      true -> Map.put(context, me, Map.get(context, me) + 1)
      false -> Map.put(context, me, 1)
    end
    case Map.has_key?(state.hinted_data, original_target) do
      false ->
        _data = [{val, processed_context}]
        %{state | hinted_data: Map.put(state.hinted_data, original_target, %{key => _data})}
      true ->
        local_storage = Map.get(state.hinted_data, original_target)
        new_storage = case Map.has_key?(local_storage, key) do
          false ->
            Map.put(local_storage, key, [{val, processed_context}])
          true ->
            new_data_list = process_data_list(Map.get(local_storage, key), val, processed_context) # TODO: Undefined func
            Map.put(local_storage, key, new_data_list)
        end
        %{state | hinted_data: Map.put(state.hinted_data, original_target, new_storage)}
    end
  end

  @spec get_data(%Node{}, non_neg_integer()) :: {any(), map()}
  defp get_data(state, key) do
    Map.get(state.data, key)
  end

  @spec get_hinted_data(%Node{}, atom(), non_neg_integer()) :: any()
  defp get_hinted_data(state, original_target, key) do
    case Map.has_key?(state.hinted_data, original_target) do
      true -> Map.get(state.hinted_data[original_target], key)
      false -> nil
    end
  end

  #  # 这个好像没用了
  #  @spec merge_key_from_node(%Node{}, non_neg_integer(), any(), map()) :: %Node{}
  #  defp merge_key_from_node(state, key, val, recv_vector_clk) do
  #    me = whoami()
  #    case Map.has_key?(state.data, key) do
  #      false -> %{state | data: Map.put(state.data, key, {val, recv_vector_clk})}
  #      true ->
  #        case VectorClock.compare_vectors(recv_vector_clk, elem(Map.get(state.data, key), 1)) do
  #          :after -> %{state | data: Map.put(state.data, key, {val, recv_vector_clk})}
  #          _ -> state
  #        end
  #    end
  #  end


  @spec send_replica_to_nodes(%Node{}, non_neg_integer(), any()) :: atom()
  def send_replica_to_nodes(state, key, hint) do
    data = state.data[key] # 存疑，只发修改了的还是包含冲突的在内的全部
    message = ReplicaPutRequest.new(key, data, hint)
    broadcast_replica(state, message)
  end

  @spec get_replica_from_nodes(%Node{}, non_neg_integer(), any()) :: atom() | value()
  def get_replica_from_nodes(state, key, hint) do
    message = ReplicaGetRequest.new(key, hint)
    broadcast_replica(state, message)
  end

  @spec context_summary_helper(map(), list()) :: tuple()
  defp context_summary_helper(res_context, local_context) do
    case local_context do
      [] -> res_context
      [{val, context} | tl] ->
        context_summary_helper(
          Map.merge(
            res_context,
            context,
            fn _k, v1, v2 -> max(v1, v2)
            end
          ),
          tl
        )
    end
  end


  @spec get_context_summary(%Node{}, any()) :: tuple()
  defp get_context_summary(state, key) do
    data = state.data[key]
    val = Enum.map(data, fn {v, clk} -> v end)
    context = context_summary_helper(%{}, data)
    {val, context}
  end


  # This funtions check all target_node in hinted data with gossip table
  # and send hinted data to target_node if gossip_table[target] is :alive.
  # state is unchanged, hinted_data is updated when receiving responses
  @spec send_hinted_data_ref_gossip_table(map(), map()) :: nil
  defp send_hinted_data_ref_gossip_table(hinted_data, gossip_table) do
    Enum.each(hinted_data, fn {target_node, vals} ->
      case enum(Map.get(gossip_table, target_node), 2) do
        :alive ->
          Enum.each(vals, fn {k, {v, context}} ->
            send(target_node, HintedDataRequest.new(k, v, context))
          end)
        _ ->
          {target_node, vals}
      end
    end)
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

            send_hinted_data_ref_gossip_table(state.hinted_data, state.gossip_table)

            run_node(state)
        end

      {sender,
        %CoordinateRequest{
          client: client,
          method: method,
          hint: hint,
          key: key,
          val: val,
          context: context
        }} ->
        case hint do
          nil ->
            case method do
              :put ->
                case context do
                  nil -> send(sender, CoordinateResponse.new_put_response(client, :fail, key))
                          run_node(state)
                  _ -> state = put_key_from_client(state, key, val, context)
                        # TODO: replica
                        %{state | response_cnt: Map.put(state.response_cnt, client, 1)}
                        run_node(state)
                end

              :get ->
                r_data = get_data(state, key)
                case r_data do
                  nil -> send(sender, CoordinateResponse.new_get_response(client, :fail, key, nil, nil))
                          run_node(state)
                  context -> %{state | response_cnt: Map.put(state.response_cnt, client, 1)}
                              run_node(state)
                end
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

      {sender,
        %ReplicaPutRequest{
          client: client,
          key: k,
          context: context,
          hint: hint
        }} -> case hint do
                nil -> %{state | data: Map.put(state.data, k, merge_context(context, state.data[k]))}
                        send(sender, ReplicaPutResponse.new(client, k, :success))
                h -> # TODO
              end

      {sender,
        %ReplicaGetRequest{
          client: client,
          key: key,
          hint: hint
        }} -> case hint do
                nil -> case Map.has_key?(state.data, key) do
                        true -> send(sender, ReplicaGetResponse.new(client, key, Map.get(state.data, key), :success))
                        false -> send(sender, ReplicaGetResponse.new(client, key, nil, :fail))
                       end

                h -> # TODO
              end

      {sender,
        %ReplicaGetResponse{
          client: client,
          key: key,
          context: context,
          succ: succ
        }} -> case Map.has_key?(state.response_cnt, client) do
                true -> %{state | response_cnt: Map.put(state.response_cnt, client, Map.get(state.response_cnt, client) + 1)}
                        %{state | data: Map.put(state.data, key, merge_context(context, state.data[key]))}
                        if state.response_cnt[client] >= state.r_cnt do
                          {val, context} = get_context_summary(state, key)
                          send(client, CoordinateResponse.new_get_response(client, :success, key, val, context))
                        end
                        run_node(state)
                false -> run_node(state) # TODO
              end

      {sender,
        %ReplicaPutResponse{
          client: client,
          key: key,
          succ: succ
        }} -> case Map.has_key?(state.response_cnt, client) do
                true -> %{state | response_cnt: Map.put(state.response_cnt, client, Map.get(state.response_cnt, client) + 1)}
                        if state.response_cnt[client] >= state.w_cnt do
                          send(client, CoordinateResponse.new_put_response(client, :success, key))
                        end
                        run_node(state)
                false -> run_node(state)
              end

      {sender, %HintedDataRequest{
        key: key,
        val: val,
        context: context
      }} ->
        state = put_key_from_client(state, key, val, context) # node 从别的node收到hinted data时要更新这个context 么？
        send(sender, HintedDataResponse.new(key, :succ))
        run_node(state)

      {sender, %HintedDataResponse{
        key: key,
        succ: succ
      }} ->
        case succ do
          :succ ->
            new_data_list = Map.delete(Map.get(state.hinted_data, sender), key)  # deleted {key, {v, c}} from hinted_data[sender]
            %{state | hinted_data: Map.put(state.hinted_data, sender, new_data_list)}  # update hinted_data
          :fail ->
            state
        end

        run_node(state)
    end
  end

end

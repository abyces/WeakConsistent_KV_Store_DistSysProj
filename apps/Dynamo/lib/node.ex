defmodule DynamoNode do
  @moduledoc """
  An implementation of the Dynamo.
  """
  alias __MODULE__

  import Emulation,
    only: [send: 2, timer: 2, now: 0, whoami: 0, cancel_timer: 1, timer: 1]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  require Fuzzers
  require Logger
  require System

  # This structure contains all info for a dynamo node
  defstruct(
    node_list: nil,
    hash_ring: nil,
    hash_avl: nil,
    self_idx: nil,
    self_hash: nil,
    hash_to_node: nil,
    replica_count: nil,
    preference_list: nil,       # nodes to send replica
    vector_clock: nil,
    gossip_table: nil,

    data: nil,                        # #{key => [{v, context}, ...]}
    hinted_data: nil,                 # %{key => [{v, context}, ...]}
    response_cnt: nil,                # %{timer_msg => {cnt, client, method, key, timer}}
    client_request_identifier: nil,   # %{client => atom()}
    hinted_nodes: nil,                # %{node => MapSet<[keys]>}
    key_range: nil,                   # %{node_hash => MapSet<[keys]>, node_hash => [keys}

    heartbeat_timeout: nil,
    heartbeat_timer: nil,
    gossip_check_timer: nil,
    gossip_check_timeout: nil,
    gossip_send_timer: nil,
    gossip_send_timeout: nil,
    fail_timeout: nil,          # gossip_table fail timeout
    cleanup_timeout: nil,       # gossip_table cleanup timeout

    r_cnt: nil,
    w_cnt: nil,
    w_timeout: nil,
    r_timeout: nil,
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
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer()
        ) :: %DynamoNode{}
  def new_configuration(
        node_list,
        replica_count,
        heartbeat_timeout,
        gossip_check_timeout,
        gossip_send_timeout,
        fail_timeout,
        cleanup_timeout,
        r_cnt,
        w_cnt,
        w_timeout,
        r_timeout
      ) do

    %DynamoNode{
      node_list: node_list,
      hash_ring: nil,
      hash_avl: nil,
      hash_to_node: nil,
      self_idx: nil,
      self_hash: nil,
      replica_count: replica_count,
      preference_list: nil,
      vector_clock: %{},
      gossip_table: %{},
      data: %{},
      hinted_data: %{},
      response_cnt: %{},
      hinted_nodes: %{},
      key_range: %{},
      client_request_identifier: %{},
      heartbeat_timeout: heartbeat_timeout,
      heartbeat_timer: nil,
      gossip_check_timer: nil,
      gossip_check_timeout: gossip_check_timeout,
      gossip_send_timer: nil,
      gossip_send_timeout: gossip_send_timeout,
      fail_timeout: fail_timeout,
      cleanup_timeout: cleanup_timeout,
      r_cnt: r_cnt,
      w_cnt: w_cnt,
      w_timeout: w_timeout,
      r_timeout: r_timeout,
    }
  end

  # Utility function to send a message to all
  @spec broadcast_to_others(%DynamoNode{}, any()) :: [boolean()]
  defp broadcast_to_others(state, message) do
    me = whoami()

    (state.node_list ++ [:loadbalancer])
    |> Enum.filter(fn pid -> pid != me end)
    |> Enum.map(fn pid -> send(pid, message) end)
  end

  # This function broadcast the message to all its replica node(preference_list)
  @spec broadcast_replica(%DynamoNode{}, any()) :: [boolean()]
  defp broadcast_replica(state, message) do
    Enum.each(state.preference_list, fn node ->
      if elem(state.gossip_table[node], 2) == :alive do
        send(node, message)
      end
    end)
  end

  # This function reset the heartbeat timer
  @spec reset_heartbeat_timer(%DynamoNode{}) :: %DynamoNode{}
  defp reset_heartbeat_timer(state) do
    if state.heartbeat_timer != nil do
      Emulation.cancel_timer(state.heartbeat_timer )
    end

    %{state | heartbeat_timer: timer(state.heartbeat_timeout, :heartbeat)}
  end

  # This function reset the gossip check timer
  @spec reset_gossip_check_timer(%DynamoNode{}) :: %DynamoNode{}
  defp reset_gossip_check_timer(state) do
    if state.gossip_check_timer != nil do
      Emulation.cancel_timer(state.gossip_check_timer)
    end

    %{state | gossip_check_timer: timer(state.gossip_check_timeout, :gossip_check)
    }
  end

  # This function reset the gossip send timer
  @spec reset_gossip_send_timer(%DynamoNode{}) :: %DynamoNode{}
  defp reset_gossip_send_timer(state) do
    if state.gossip_send_timer != nil do
      Emulation.cancel_timer(state.gossip_send_timer)
    end

    %{state | gossip_send_timer: timer(state.gossip_send_timeout, :gossip_send)}
  end

  @spec put_key_from_client(%DynamoNode{}, non_neg_integer(), any(), map()) :: %DynamoNode{}
  defp put_key_from_client(state, key, val, context) do
    me = whoami()
    processed_context = Context.increment_vector_clock(context)
    case Map.has_key?(state.data, key) do
      false ->
        %{state | data: Map.put(state.data, key, [{val, processed_context}])}

      true ->
        new_data_list =
          Context.merge_context(Map.get(state.data, key), [{val, processed_context}])
        %{state | data: Map.put(state.data, key, new_data_list)}
    end
  end

  @spec put_hinted_data(%DynamoNode{}, atom(), non_neg_integer(), any(), list()) :: %DynamoNode{}
  defp put_hinted_data(state, hint, key, val, context) do
    me = whoami()
    processed_context = Context.increment_vector_clock(context)
    k_list = case Map.has_key?(state.hinted_nodes, hint) do
      true -> MapSet.put(state.hinted_nodes[hint], key)
      false -> MapSet.new([key])
    end
    state = %{state | hinted_nodes: Map.put(state.hinted_nodes, hint, k_list)}
    new_hinted_data = case Map.has_key?(state.hinted_data, key) do
      false -> Map.put(state.hinted_data, key, [{val, processed_context}])
      true -> Map.put(state.hinted_data, key, Context.merge_context(state.hinted_data[key], [{val, processed_context}]))
    end
    %{state | hinted_data: new_hinted_data}
  end

  @spec add_key_to_key_range(%DynamoNode{}, atom(), any()) :: %DynamoNode{}
  defp add_key_to_key_range(state, node_hash, key) do
    state = case Map.has_key?(state.key_range, node_hash) do
      true -> %{state | key_range: Map.put(state.key_range, node_hash, MapSet.put(state.key_range[node_hash], key))}
      false -> %{state | key_range: Map.put(state.key_range, node_hash, MapSet.new([key]))}
    end
  end

  @spec find_key_range_by_key(%DynamoNode{}, any()) :: any()
  defp find_key_range_by_key(state, key) do
    key_range = AVLTree.get_next_larger(state.hash_avl, String.to_integer(:crypto.hash(:md5, key) |> Base.encode16(), 16))
  end

  @spec get_data(%DynamoNode{}, non_neg_integer()) :: {any(), map()}
  defp get_data(state, key) do
    Map.get(state.data, key)
  end

  @spec get_hinted_data(%DynamoNode{}, atom(), non_neg_integer()) :: any()
  defp get_hinted_data(state, original_target, key) do
    Map.get(state.hinted_data, key)
  end

  @spec send_replica_to_nodes(%DynamoNode{}, atom(), non_neg_integer(), any(), any()) :: atom()
  def send_replica_to_nodes(state, client, key, hint, key_range) do
    data = state.data[key]
    message = ReplicaPutRequest.new(client, key, data, hint, key_range)
    broadcast_replica(state, message)
  end

  @spec get_replica_from_nodes(%DynamoNode{}, atom(), non_neg_integer(), any()) ::
          atom()
  def get_replica_from_nodes(state, client, key, hint) do
    case hint do
      nil -> message = ReplicaGetRequest.new(client, key, nil)
             broadcast_replica(state, message)
      h -> hint_hash = String.to_integer(
        :crypto.hash(:md5, Atom.to_string(h)) |> Base.encode16(),
        16
      )
           hint_idx = Enum.find_index(state.hash_ring, fn v -> v == hint_hash end)
           data = state.hinted_data[h][key]

           hint_preference_list =
             Enum.slice(
               state.hash_ring ++ state.hash_ring,
               (hint_idx + 1)..(hint_idx + min(state.replica_count + 1, length(state.node_list) - 1))
             ) |> Enum.map(fn v -> state.hash_to_node[v] end)
           me = whoami()
           Enum.each(hint_preference_list, fn v ->
             if elem(Map.get(state.gossip_table, v), 2) == :alive && v != me do
               send(v, ReplicaGetRequest.new(client, key, nil))
             end
           end)
    end
  end

  @spec send_hinted_replica_to_nodes(%DynamoNode{}, atom(), non_neg_integer(), any()) :: atom()
  def send_hinted_replica_to_nodes(state, client, key, hint) do
    hint_hash = String.to_integer(
      :crypto.hash(:md5, Atom.to_string(hint)) |> Base.encode16(),
      16
    )
    hint_idx = Enum.find_index(state.hash_ring, fn v -> v == hint_hash end)
    data = state.hinted_data[key]

    hint_preference_list =
      Enum.slice(
        state.hash_ring ++ state.hash_ring,
        (hint_idx + 1)..(hint_idx + min(state.replica_count + 1, length(state.node_list) - 1))
      ) |> Enum.map(fn v -> state.hash_to_node[v] end)

    Enum.each(List.zip([state.preference_list, hint_preference_list]), fn {dst, hint_src} ->
      if elem(Map.get(state.gossip_table, dst), 2) == :alive do
        send(dst, ReplicaPutRequest.new(client, key, data, hint_src, nil))
      end
    end)
  end

  # This funtions check all target_node in hinted data with gossip table
  # and send hinted data to target_node if gossip_table[target] is :alive.
  # state is unchanged, hinted_data is updated when receiving responses
  @spec send_hinted_data_ref_gossip_table(%DynamoNode{}, atom()) :: %DynamoNode{}
  defp send_hinted_data_ref_gossip_table(state, target_node) do
#    IO.puts("#{whoami()}: send_hinted_data_ref_gossip to #{target_node}}")
    if elem(Map.get(state.gossip_table, target_node), 2)  == :alive do
      Enum.each(state.hinted_nodes[target_node], fn key ->
        send(target_node, HintedDataRequest.new(key, state.hinted_data[key]))
      end)
    end
    state
  end

  @spec merkle_tree_hash_func(any()) :: (any() -> any())
  def merkle_tree_hash_func(x) do
    :crypto.hash(:md5, x) |> Base.encode16()
  end

  @spec data_sync_helper(%DynamoNode{}, [any()], map(), any()) :: %DynamoNode{}
  def data_sync_helper(state, keys, recv_blcoks, key_range) do
    case keys do
      [] -> state
      [key | tl] -> state = %{state | data: Map.put(state.data, key, Context.merge_context(state.data[key], recv_blcoks[key]))}
                    state = %{state | key_range: Map.put(state.key_range, key_range, MapSet.put(state.key_range[key_range], key))}
                    data_sync_helper(state, tl, recv_blcoks, key_range)
    end
  end

  @spec merkle_tree_data_sync(%DynamoNode{}, any(), %MerkleTree.Node{}, %MerkleTree.Node{}, map()) :: %DynamoNode{}
  def merkle_tree_data_sync(state, key_range, local_merkle_tree, recv_merkle_tree, recv_blocks) do
    case local_merkle_tree do
      [] -> case recv_merkle_tree do
              [] -> state
              node -> data_sync_helper(state, node.keys, recv_blocks, key_range)
            end
      node -> case recv_merkle_tree do
                [] -> state
                node1 -> if node.value == node1.value do
                            state
                         else
                            if node.children == [] || node1.children == [] do
                              data_sync_helper(state, node1.keys, recv_blocks, key_range)
                            else
                              [local_child1, local_child2] = node.children
                              [recv_child1, recv_child2] = node1.children
                              state = merkle_tree_data_sync(state, key_range, local_child1, recv_child1, recv_blocks)
                              merkle_tree_data_sync(state, key_range, local_child2, recv_child2, recv_blocks)
                            end
                         end
              end
    end
  end


  @doc """
  This function initialize a dynamo node
  """
  @spec node_init(%DynamoNode{}) :: no_return()
  def node_init(state) do
    cur_timestamp = System.os_time(:millisecond)

    gossip_table =
      Map.new(state.node_list, fn v -> {v, {0, cur_timestamp, :alive}} end)

    hash_to_node =
      state.node_list
      |> Enum.map(fn v ->
        {String.to_integer(
          :crypto.hash(:md5, Atom.to_string(v)) |> Base.encode16(),
          16
        ), v}
      end)
      |> Map.new()

    hash_avl =
      hash_to_node
      |> Enum.map(fn {k, v} -> k end)
      |> Enum.into(AVLTree.new())

    hash_ring = AVLTree.inorder_traverse(hash_avl)

    self_hash =
      String.to_integer(
        :crypto.hash(:md5, Atom.to_string(whoami())) |> Base.encode16(),
        16
      )

    self_idx = Enum.find_index(hash_ring, fn v -> v == self_hash end)
    preference_list =
      Enum.slice(
        hash_ring ++ hash_ring,
        (self_idx + 1)..(self_idx + min(state.replica_count + 1, length(state.node_list) - 1))
      ) |> Enum.map(fn v -> hash_to_node[v] end)
#    IO.puts(Enum.map(hash_ring, fn v -> hash_to_node[v] end))
    state = %{
      state
      | hash_ring: hash_ring,
        hash_avl: hash_avl,
        hash_to_node: hash_to_node,
        self_idx: self_idx,
        self_hash: self_hash,
        preference_list: preference_list,
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
  @spec run_node(%DynamoNode{}) :: no_return()
  defp run_node(state) do
    receive do
      :heartbeat ->
        broadcast_to_others(state, GossipMessage.new_gossip_message(nil))
        state = reset_heartbeat_timer(state)
        run_node(state)

      :gossip_check ->
        state = Gossip.gossip_table_periodical_check(state)
        state = reset_gossip_check_timer(state)
        run_node(state)

      :gossip_send ->
        broadcast_to_others(
          state,
          GossipMessage.new_gossip_message(state.gossip_table)
        )

        state = reset_gossip_send_timer(state)
        run_node(state)

      {sender, %GossipMessage{gossip_table: rev_gossip_table}} ->
        case rev_gossip_table do
          nil -> # IO.puts("#{whoami()} received heartbeat from #{sender}")
                 state = Gossip.gossip_table_update_heartbeat(state, sender)
                 run_node(state)

          _ ->  # IO.puts("#{whoami()} received gossip table from #{sender}")
               state = Gossip.gossip_table_set_alive_by_id(state, sender)
               state = Gossip.gossip_table_merge(state, rev_gossip_table)
               state = case Map.has_key?(state.hinted_nodes, sender) do
                 true -> send_hinted_data_ref_gossip_table(state, sender)
                 false -> state
               end

               run_node(state)
        end

      {sender,
       %CoordinateRequest{
         client: client,
         method: method,
         hint: hint,
         key: key,
         val: val,
         vector_clock: vector_clock,
         key_range: key_range
       }} ->
#        IO.puts("#{whoami()} received coordinate #{method} request from client #{client} with hint #{hint} key_range #{key_range}}")
        case hint do
          nil ->
            case method do
              :put ->
                case vector_clock do
                  nil ->
                    # check if this is a new key, only new keys are allowed to have no context passed in
                    case Map.has_key?(state.data, key) do
                      true -> {vals, context_summary} = Context.get_context_summary(state, key)
                              send(:loadbalancer, CoordinateResponse.new_put_response(client, :no_context, key, vals, context_summary))
                              run_node(state)
                      false -> state = put_key_from_client(state, key, val, %{})
                               state = add_key_to_key_range(state, key_range, key)
                               send_replica_to_nodes(state, client, key, hint, key_range)
                               {vals, context_summary} = Context.get_context_summary(state, key)
                               state = case state.w_cnt do
                                 1 -> send(:loadbalancer, CoordinateResponse.new_put_response(client, :ok, key, vals, context_summary))
                                      state
                                 _ -> timer_msg = LoadBalancer.get_hash_timer_msg(client, method, key)
                                      state = %{state | client_request_identifier: Map.put(state.client_request_identifier, client, timer_msg)}
                                      state = %{state | response_cnt: Map.put(state.response_cnt, timer_msg, {1, client, :put, key, timer(state.w_timeout, timer_msg)})}
                               end
                               run_node(state)
                    end

                  _ ->
                    state = put_key_from_client(state, key, val, vector_clock)
                    case length(Map.get(state.data, key)) do
                      1 -> {vals, context_summary} = Context.get_context_summary(state, key)
                           state = add_key_to_key_range(state, key_range, key)
                           send_replica_to_nodes(state, client, key, hint, key_range)
                           state = case state.w_cnt do
                             1 -> send(:loadbalancer, CoordinateResponse.new_put_response(client, :ok, key, vals, context_summary))
                                  state
                             _ -> timer_msg = LoadBalancer.get_hash_timer_msg(client, method, key)
                                  state = %{state | client_request_identifier: Map.put(state.client_request_identifier, client, timer_msg)}
                                  state = %{state | response_cnt: Map.put(state.response_cnt, timer_msg, {1, client, :put, key, timer(state.w_timeout, timer_msg)})}
                           end
                           run_node(state)

                      _ -> {vals, context_summary} = Context.get_context_summary(state, key)
                           send(:loadbalancer, CoordinateResponse.new_put_response(client, :conflict, key, vals, context_summary))
                           run_node(state)
                    end
                end

              :get ->
                  r_data =
                    get_data(state, key)
                    case r_data do
                      nil -> send(:loadbalancer, CoordinateResponse.new_get_response(client, :no_such_key, key, nil, nil))
                             run_node(state)

                      context ->
                        state = case state.r_cnt do
                          1 -> {vals, context_summary} = Context.get_context_summary(state, key)
                                #IO.puts("get response")
                               send(:loadbalancer, CoordinateResponse.new_get_response(client, :ok, key, vals, context_summary))
                               state
                          _ -> #IO.puts("get response 3")
                               get_replica_from_nodes(state, client, key, hint)
                               timer_msg = LoadBalancer.get_hash_timer_msg(client, method, key)
                               state = %{state | client_request_identifier: Map.put(state.client_request_identifier, client, timer_msg)}
                               %{state | response_cnt: Map.put(state.response_cnt, timer_msg, {1, client, :get, key, timer(state.r_timeout, timer_msg)})} #TODO timer
                        end
                        run_node(state)
                end
            end

          origin_target ->
            case method do
              :put ->
                case vector_clock do
                  nil ->
                    # check if this is a new key, only new keys are allowed to have no context passed in
                    case Map.has_key?(state.hinted_data, key) do
                      true -> {vals, context_summary} = Context.get_context_summary_hinted_data(state, key)
                              send(:loadbalancer, CoordinateResponse.new_put_response(client, :no_context, key, vals, context_summary))
                              run_node(state)
                      false -> state = put_hinted_data(state, origin_target, key, val, %{})
                               send_hinted_replica_to_nodes(state, client, key, hint)

                               state = case state.w_cnt do
                                 1 -> {vals, context_summary} = Context.get_context_summary_hinted_data(state, key)
                                       send(:loadbalancer, CoordinateResponse.new_put_response(client, :ok, key, vals, context_summary))
                                       state
                                 _ -> timer_msg = LoadBalancer.get_hash_timer_msg(client, method, key)
                                       state = %{state | client_request_identifier: Map.put(state.client_request_identifier, client, timer_msg)}
                                       state = %{state | response_cnt: Map.put(state.response_cnt, timer_msg, {1, client, :put, key, timer(state.w_timeout, timer_msg)})} #TODO timer
                               end
                               run_node(state)
                    end

                  _ ->
                    state = put_hinted_data(state, origin_target, key, val, vector_clock)
                    case length(Map.get(state.hinted_data, key)) do
                      1 -> {vals, context_summary} = Context.get_context_summary_hinted_data(state, key)
                           send_hinted_replica_to_nodes(state, client, key, hint)
                           state = case state.w_cnt do
                             1 -> send(:loadbalancer, CoordinateResponse.new_put_response(client, :ok, key, vals, context_summary))
                                  state
                             _ -> timer_msg = LoadBalancer.get_hash_timer_msg(client, method, key)
                                  state = %{state | client_request_identifier: Map.put(state.client_request_identifier, client, timer_msg)}
                                  state = %{state | response_cnt: Map.put(state.response_cnt, timer_msg, {1, client, :put, key, timer(state.w_timeout, timer_msg)})} #TODO timer
                           end
                           run_node(state)
                      _ -> {vals, context_summary} = Context.get_context_summary_hinted_data(state, key)
                           send(:loadbalancer, CoordinateResponse.new_put_response(client, :conflict, key, vals, context_summary))
                           run_node(state)
                    end
                end

              :get ->
                r_data = case Map.has_key?(state.data, key) do
                  true -> get_data(state, key)
                  false -> get_hinted_data(state, origin_target, key)
                end

                case r_data do
                  nil -> send(:loadbalancer, CoordinateResponse.new_get_response(client, :no_such_key, key, nil, nil))
                         run_node(state)
                  context -> state = case state.r_cnt do
                               1 -> {vals, context_summary} = Context.get_context_summary_hinted_data(state, key)
                                    send(:loadbalancer, CoordinateResponse.new_get_response(client, :ok, key, vals, context_summary))
                                    state
                               _ -> get_replica_from_nodes(state, client, key, hint)
                                    timer_msg = LoadBalancer.get_hash_timer_msg(client, method, key)
                                    state = %{state | client_request_identifier: Map.put(state.client_request_identifier, client, timer_msg)}
                                    %{state | response_cnt: Map.put(state.response_cnt, timer_msg, {1, client, :get, key, timer(state.r_timeout, timer_msg)})} #TODO timer
                             end
                             run_node(state)
                end

                run_node(state)
            end
        end

      {sender,
       %ReplicaPutRequest{
         client: client,
         key: k,
         context: context,
         hint: hint,
         key_range: key_range
       }} ->
#        IO.puts("#{whoami()} received replica put request from #{sender} with hint #{hint}")
        case hint do
          nil ->
            state = %{state | data: Map.put(state.data, k, Context.merge_context(state.data[k], context))}
            state = add_key_to_key_range(state, key_range, k)
            send(sender, ReplicaPutResponse.new(client, k, :ok, hint))
            run_node(state)

          h -> state = case Map.has_key?(state.hinted_nodes, h) do
                 true -> %{state | hinted_nodes: Map.put(state.hinted_nodes, h, MapSet.put(state.hinted_nodes[h], k))}
                 false -> %{state | hinted_nodes: Map.put(state.hinted_nodes, h, MapSet.new([k]))}
               end

               state = %{state | hinted_data: Map.put(state.hinted_data, k, Context.merge_context(state.hinted_data[k], context))}
               send(sender, ReplicaPutResponse.new(client, k, :ok, hint))
               run_node(state)
        end

      {sender,
       %ReplicaGetRequest{
         client: client,
         key: key,
         hint: hint
       }} ->
#        IO.puts("#{whoami()} received replica get request from #{sender} with hint #{hint}")
        case hint do
          nil ->
            case Map.has_key?(state.data, key) do
              true ->
                send(sender, ReplicaGetResponse.new(client, key, Map.get(state.data, key), :ok, hint))

              false ->
                send(sender, ReplicaGetResponse.new(client, key, nil, :fail, hint))
            end
            run_node(state)

          h ->
            case Map.has_key?(state.hinted_data, key) do
              true -> send(sender, ReplicaGetResponse.new(client, key, Map.get(state.hinted_data, key), :ok, h))
              false -> send(sender, ReplicaGetResponse.new(client, key, nil, :fail, hint))
            end
            run_node(state)
        end

      {sender,
       %ReplicaGetResponse{
         client: client,
         key: key,
         context: context,
         succ: succ,
         hint: hint
       }} ->
#        IO.puts("#{whoami()} received replica get response from #{sender} with hint #{hint}")
#        if Map.has_key?(state.client_request_identifier, client) do
#          IO.puts("#{whoami()} has client in state.client_request_identifier ")
#        end
        case hint do
          nil -> case Map.has_key?(state.client_request_identifier, client) do
                   true ->
                     timer_msg = state.client_request_identifier[client]
#                     if Map.has_key?(state.response_cnt, timer_msg) do
#                      IO.puts("#{whoami()} has timer_msg in response_cnt #{timer_msg}")
#                     end
                     state = case Map.has_key?(state.response_cnt, timer_msg) do
                       true ->{cnt, _, _, _, t} = state.response_cnt[timer_msg]
                              state = %{state | response_cnt: Map.put(state.response_cnt, timer_msg, {cnt + 1, client, :get, key, t})}
                              merged_context = Context.merge_context(state.data[key], context)
                              state = %{state | data: Map.put(state.data, key, merged_context)}
#                              IO.puts("#{whoami()} R: #{state.r_cnt}, cur_r: #{cnt + 1}")
                              if cnt + 1 >= state.r_cnt do
                                cancel_timer(t)
                                state = %{state | client_request_identifier: Map.delete(state.client_request_identifier, client)}
                                state = %{state | response_cnt: Map.delete(state.response_cnt, timer_msg)}
                                {val, context} = Context.get_context_summary(state, key)
#                                IO.puts("#{whoami()} send coord get response")
                                send(:loadbalancer, CoordinateResponse.new_get_response(client, :ok, key, val, context))
                                run_node(state)
                              end
                              state
                       false -> state
                     end
#                     {cnt, _, _, _, t} = state.response_cnt[timer_msg]
#                     state = %{state | response_cnt: Map.put(state.response_cnt, timer_msg, {cnt + 1, client, :get, key, t})}


                     run_node(state)

                   # TODO
                   false -> merged_context = Context.merge_context(state.data[key], context)
                            state = %{state | data: Map.put(state.data, key, merged_context)}
                            run_node(state)
                 end
          h -> case Map.has_key?(state.client_request_identifier, client) do
             true ->
               timer_msg = state.client_request_identifier[client]
               state = case Map.has_key?(state.response_cnt, timer_msg) do
                 true ->{cnt, _, _, _, t} = state.response_cnt[timer_msg]
                        state = %{state | response_cnt: Map.put(state.response_cnt, timer_msg, {cnt + 1, client, :get, key, t})}
                        merged_context = Context.merge_context(state.hinted_data[key], context)
                        state = %{state | hinted_data: Map.put(state.hinted_data, key, merged_context)}
                        if cnt + 1 >= state.r_cnt do
                          cancel_timer(t)
                          state = %{state | client_request_identifier: Map.delete(state.client_request_identifier, client)}
                          state = %{state | response_cnt: Map.delete(state.response_cnt, timer_msg)}
                          {val, context} = Context.get_context_summary_hinted_data(state, key)
                          send(:loadbalancer, CoordinateResponse.new_get_response(client, :ok, key, val, context))
                          run_node(state)
                        end
                        state
                 false -> state
               end
#               {cnt, _, _, _, t} = state.response_cnt[timer_msg]
#               state = %{state | response_cnt: Map.put(state.response_cnt, timer_msg, {cnt + 1, client, :get, key, t})}

               run_node(state)

             # TODO
             false ->merged_context = Context.merge_context(state.hinted_data[key], context)
                     state = %{state | hinted_data: Map.put(state.hinted_data, key, merged_context)}
                     run_node(state)
           end
        end

      {sender,
       %ReplicaPutResponse{
         client: client,
         key: key,
         succ: succ,
         hint: hint
       }} ->
#        IO.puts("#{whoami()} received replica put response from #{sender}")
        case Map.has_key?(state.client_request_identifier, client) do
          true ->
            timer_msg = state.client_request_identifier[client]
            state = case Map.has_key?(state.response_cnt, timer_msg) do
              true ->{cnt, _, _, _, t} = state.response_cnt[timer_msg]
                     state = %{state | response_cnt: Map.put(state.response_cnt, timer_msg, {cnt + 1, client, :put, key, t})}
                     if cnt + 1 >= state.w_cnt do
#                       IO.puts("response")
                       cancel_timer(t)
                       state = %{state | client_request_identifier: Map.delete(state.client_request_identifier, client)}
                       state = %{state | response_cnt: Map.delete(state.response_cnt, timer_msg)}
                       {vals, context_summary} = case hint do
                         nil -> Context.get_context_summary(state, key)
                         h -> Context.get_context_summary_hinted_data(state, key)
                       end
                       send(:loadbalancer, CoordinateResponse.new_put_response(client, :ok, key, vals, context_summary))
                       run_node(state)
                     end
                     state
              false -> state
            end
#            {cnt, _, _, _, t} = state.response_cnt[timer_msg]
#            state = %{state | response_cnt: Map.put(state.response_cnt, timer_msg, {cnt + 1, client, :put, key, t})}

            run_node(state)

          false ->
            run_node(state)
        end

      {sender,
       %HintedDataRequest{
         key: key,
         data_list: data_list
       }} ->
#        IO.puts("node #{whoami()} received hinted req from #{sender}")
        state = %{state | data: Map.put(state.data, key, Context.merge_context(state.data[key], data_list))}
        send(sender, HintedDataResponse.new(key, :ok))
        run_node(state)

      {sender,
       %HintedDataResponse{
         key: key,
         succ: succ
       }} ->
#        IO.puts("node #{whoami()} received hinted res from #{sender}")
        state = case succ do
          :ok ->
            updated_key_list = case Map.has_key?(state.hinted_nodes, sender) do
              true -> MapSet.delete(state.hinted_nodes[sender], key)
              false -> MapSet.new()
            end
            state = case MapSet.size(updated_key_list) do
              0 -> %{state | hinted_nodes: Map.delete(state.hinted_nodes, sender)}
              _ -> %{state | hinted_nodes: Map.put(state.hinted_nodes, sender, updated_key_list)}
            end
            state = %{state | hinted_data: Map.delete(state.hinted_data, key)}

          :fail ->
            state
        end

        run_node(state)

      {sender,
        %ReplicaSyncRequest{
          key_range: key_range
        }} -> #IO.puts("#{whoami()} received replica sync req from #{sender} with #{key_range}")
              keys = state.key_range[key_range]
              data_blocks = Enum.map(keys, fn key -> {key, state.data[key]} end) |> Map.new()
              send(sender, ReplicaSyncResponse.new(key_range, MerkleTree.new(data_blocks, &merkle_tree_hash_func/1)))
              run_node(state)

      {sender,
        %ReplicaSyncResponse{
          key_range: key_range,
          merkle_tree: recv_merkle_tree
        }} -> #IO.puts("#{whoami()} received replica sync res from #{sender}")
              keys = state.key_range[key_range]
              state = case keys do
                nil -> state = %{state | data: Map.merge(state.data, recv_merkle_tree.blocks)}
                       recv_keys = MapSet.new(Enum.map(recv_merkle_tree.blocks, fn {k, _} -> k end))
                       state = %{state | key_range: Map.put(state.key_range, key_range, recv_keys)}
                _ -> data_blocks = Enum.map(keys, fn key -> {key, state.data[key]} end) |> Map.new()
                     merkle_tree = MerkleTree.new(data_blocks, &merkle_tree_hash_func/1)
                     state = merkle_tree_data_sync(state, key_range, merkle_tree.root, recv_merkle_tree.root, recv_merkle_tree.blocks)
              end
              run_node(state)

      {sender, {:reset_heartbeat_timeout, t}} -> run_node(reset_heartbeat_timer(%{state | heartbeat_timeout: t}))

      {sender, {:reset_gossip_send_timeout, t}} -> run_node(reset_gossip_send_timer(%{state | gossip_send_timeout: t}))

      {sender, {:read_replica, key}} -> {val, context} = Context.get_context_summary(state, key)
                                        send(sender, {val, context})
                                        run_node(state)

      {sender, {:read_hinted_replica, key}} -> # IO.puts("#{whoami()} send hinted replica of key #{key}")
                                                      {val, context} = Context.get_context_summary_hinted_data(state, key)
                                                      send(sender, {val, context})
                                                      run_node(state)

      {sender, :read_gossip_table} -> send(sender, state.gossip_table)
                                      run_node(state)

      {sender, :data_sync} -> master_nodes = Enum.slice(state.hash_ring ++ state.hash_ring, (state.self_idx + length(state.node_list) - length(state.preference_list)..(state.self_idx + length(state.node_list) - 1)))
                              Enum.each(master_nodes, fn target -> send(state.hash_to_node[target], ReplicaSyncRequest.new(target)) end)
                              run_node(state)

      timer_msg -> case Map.has_key?(state.response_cnt, timer_msg) do
                    false -> run_node(state)
                    true -> {cnt, client, method, key, _} = Map.get(state.response_cnt, timer_msg)
                            send(:loadbalancer, CoordinateResponse.new_get_response(client, :timeout, key, nil, nil))
                            state = %{state | response_cnt: Map.delete(state.response_cnt, timer_msg)}
                            run_node(state)
                   end
    end
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
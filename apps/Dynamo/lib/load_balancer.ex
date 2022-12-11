defmodule LoadBalancer do
  @moduledoc """
  An implementation of the Dynamo Load Balancer.
  """
  import Emulation, only: [send: 2, timer: 2, cancel_timer: 1]

  import Kernel,
         except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]
  alias __MODULE__

  defstruct(
    view: nil,
    replica_cnt: nil,
    hash_ring: nil,
    hash_avl: nil,
    hash_to_node: nil,
    node_cnt: nil,
    node_status: nil,
    status_check_timeout: nil,
    status_check_timer: nil
  )
  require Fuzzers
  require Logger
  require System
  require AVLTree

  @doc """
  This function resets the status check timer
  """
  @spec reset_status_check_timer(%LoadBalancer{}) :: %LoadBalancer{}
  def reset_status_check_timer(state) do
    case state.status_check_timer do
      nil -> %{state | status_check_timer: timer(state.status_check_timeout, :check_status)}
      t -> cancel_timer(t)
           %{state | status_check_timer: timer(state.status_check_timeout, :check_status)}
    end
  end

  @doc """
  This function initializes a new load balancer config
  """
  @spec new_loadbalancer([atom()], non_neg_integer(), non_neg_integer()) :: %LoadBalancer{}
  def new_loadbalancer(view, status_check_timeout, replica_cnt) do
    hash_to_node =
      view
      |> Enum.map(fn v -> {String.to_integer(:crypto.hash(:md5, Atom.to_string(v)) |> Base.encode16(), 16), v} end)
      |> Map.new()

    hash_avl =
      hash_to_node
      |> Enum.map(fn {k, v} -> k end)
      |> Enum.into(AVLTree.new())

    hash_ring = AVLTree.inorder_traverse(hash_avl)

    cur_timestamp = System.os_time(:millisecond)
    node_status =
      view
        |> Enum.map(fn v -> {v, {0, cur_timestamp, :alive}} end)
        |> Map.new()

    node_cnt = length(view)
    %LoadBalancer{
      view: view,
      replica_cnt: replica_cnt,
      hash_ring: hash_ring,
      hash_avl: hash_avl,
      hash_to_node: hash_to_node,
      node_cnt: node_cnt,
      node_status: node_status,
      status_check_timeout: status_check_timeout,
      status_check_timer: nil
    }
  end

  @doc """
  This function returns the node's index in the view by a given key
  """
  @spec get_node_idx(non_neg_integer(), non_neg_integer()) :: non_neg_integer()
  def get_node_idx(k, node_cnt) do
    rem(k, node_cnt)
  end

  @doc """
  This function walks clockwise on the ring to find the first alive node, if all nodes failed then return :fail
  """
  @spec clockwise_walk(%LoadBalancer{}, [integer()]) :: atom()
  def clockwise_walk(state, hash_list) do
    case hash_list do
      [] -> :fail
      [hd | tl] ->
        node = state.hash_to_node[hd]
        case elem(state.node_status[node], 2) do
          :alive -> node
          _ ->clockwise_walk(state, tl)
        end
    end
  end

  @doc """
  This function checks the node's status periodically
  """
  @spec check_node_status(%LoadBalancer{}) :: %LoadBalancer{}
  def check_node_status(state) do
    state = reset_status_check_timer(state)
    cur_timestamp = System.os_time(:millisecond)
    timeout = state.status_check_timeout
    node_status =
      state.node_status
        |> Enum.map(fn {node, {heartbeat_cnt, timestamp, stat}} ->
          if cur_timestamp - timestamp > timeout do
            {node, {heartbeat_cnt, timestamp, :failed}}
          else
            {node, {heartbeat_cnt, timestamp, stat}}
          end
        end)
        |> Map.new()
    %{state | node_status: node_status}
  end

  @doc """
  This function merges two gossip table
  """
  @spec gossip_table_merge(%LoadBalancer{}, map()) :: %LoadBalancer{}
  defp gossip_table_merge(state, received_gossip_table) do
    cur_time = System.os_time(:millisecond)
    new_gossip_table = Map.new(state.node_status, fn {node_id, {cnt, time, stat}} ->
      case stat do
        :failed ->
          {node_id, {cnt, time, stat}}
        _ ->
          {ncnt, ntime, nstat} = received_gossip_table[node_id]
          temp =
            if nstat != :deleted && ncnt > cnt do
              {ncnt, cur_time, :alive}
            else
              {cnt, time, stat}
            end
          {node_id, temp}
      end
    end)
    %{state | node_status: new_gossip_table}
  end

  # This function updates the heartbeat cnt with particular node_id
  @spec gossip_table_update_heartbeat(%LoadBalancer{}, atom()) :: %LoadBalancer{}
  defp gossip_table_update_heartbeat(state, node_id) do
    {cnt, _, _} = state.node_status[node_id]
    %{state | node_status: Map.put(state.node_status, node_id, {cnt+1, System.os_time(:millisecond), :alive})}
  end


  @spec init_loadbalancer(%LoadBalancer{}) :: no_return()
  def init_loadbalancer(state) do
    run_loadbalancer(reset_status_check_timer(state))
  end

  @doc """
  This function implements the main logic of load balancer
  """
  @spec run_loadbalancer(%LoadBalancer{}) :: no_return()
  def run_loadbalancer(state) do
    receive do
      :check_status -> run_loadbalancer(check_node_status(state))

      {sender, %GossipMessage{gossip_table: recv_table}} ->
        case recv_table do
          nil -> run_loadbalancer(gossip_table_update_heartbeat(state, sender))  # heartbeat
          t -> run_loadbalancer(gossip_table_merge(state, t))   # gossip table
        end

      {sender,
        %ClientPutRequest{
          key: k,
          val: v,
          context: context
      }} -> IO.puts("received put req from #{sender}")
            hash_key = String.to_integer(:crypto.hash(:md5, k) |> Base.encode16(), 16)
            node_hash_val = AVLTree.get_next_larger(state.hash_avl, hash_key)
            origin_hash_idx = Enum.find_index(state.hash_ring, fn v -> v == node_hash_val end)
            origin_target_node = state.hash_to_node[node_hash_val]
            case elem(state.node_status[origin_target_node], 2) do
              :alive -> send(origin_target_node, CoordinateRequest.new_put_request(sender, nil, k, v, context))
              _ -> case clockwise_walk(
                          state,
                          Enum.slice(state.hash_ring, origin_hash_idx + state.replica_cnt .. state.node_cnt - 1)
                          ++
                          Enum.slice(state.hash_ring, max(0, -(state.node_cnt - origin_hash_idx - state.replica_cnt)) .. origin_hash_idx - 1)
                        ) do
                    :fail -> send(sender, ClientResponse.new_response(:fail, nil, nil))
                    n -> send(n, CoordinateRequest.new_put_request(sender, origin_target_node, k, v, context))
                   end
            end
            send(sender, {state.node_status, state.hash_ring, state.hash_avl})
            run_loadbalancer(state)
      {sender,
        %ClientGetRequest{
          key: k
        }} -> IO.puts("received get req from #{sender}")
              hash_key = String.to_integer(:crypto.hash(:md5, k) |> Base.encode16(), 16)
              node_hash_val = AVLTree.get_next_larger(state.hash_avl, hash_key)
              origin_hash_idx = Enum.find_index(state.hash_ring, node_hash_val)
              origin_target_node = state.hash_to_node[node_hash_val]
              case elem(state.node_status[origin_target_node], 2) do
                      :alive -> send(origin_target_node, CoordinateRequest.new_get_request(sender, nil, k))
                      _ -> case clockwise_walk(
                                  state,
                                  Enum.slice(state.hash_ring, origin_hash_idx + state.replica_cnt .. state.node_cnt - 1)
                                  ++
                                  Enum.slice(state.hash_ring, max(0, -(state.node_cnt - origin_hash_idx - state.replica_cnt)) .. origin_hash_idx - 1)
                                ) do
                       :fail -> send(sender, ClientResponse.new_response(:fail, nil))
                       n -> send(n, CoordinateRequest.new_get_request(sender, origin_target_node, k))
                     end
              end
              send(sender, {state.node_status, state.hash_ring, state.hash_avl})
              run_loadbalancer(state)
       {sender,
         %CoordinateResponse{
           client: client,
           succ: succ,
           method: method,
           key: k,
           val: val,
           context: context
         }} -> send(client, ClientResponse.new_response(succ, val, context))
    end
  end
end

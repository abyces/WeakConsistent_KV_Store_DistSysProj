defmodule MerkleTreeTest do
  use ExUnit.Case
#  doctest Node
  import Emulation, only: [spawn: 2, send: 2, whoami: 0]

  import Kernel,
         except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  test "test permanent failure recovery: merkle tree" do
    Emulation.init()
    new_loadbalancer = LoadBalancer.new_loadbalancer([:a, :b], 1000, 1, 500)

    spawn(:loadbalancer, fn ->
      LoadBalancer.init_loadbalancer(new_loadbalancer)
    end)

    # [node_list, replica_count, heartbeat_timeout, gossip_check_timeout, gossip_send_timeout, fail_timeout, cleanup_timeout, r_cnt, w_cnt]
    newnode = DynamoNode.new_configuration([:a, :b], 1, 50, 100, 100, 200, 200, 1, 1, 400, 400)

    spawn(:a, fn -> DynamoNode.node_init(newnode) end)
    spawn(:b, fn -> DynamoNode.node_init(newnode) end)
    caller = self()

    receive do
    after
      200 -> :ok
    end

    send(:b, {:reset_heartbeat_timeout, 100000})
    send(:b, {:reset_gossip_send_timeout, 100000})

    receive do
      after
        5000 -> :ok
    end

    send(:a, :read_gossip_table)
    receive do
      table ->
        assert elem(Map.get(table, :b), 2) == :deleted
    end

    client =
      spawn(:client, fn ->
        send(:loadbalancer, ClientPutRequest.new("1", 1, nil))

        receive do
          {:loadbalancer, {node_status, origin}} ->
            send(caller, {node_status, origin})

          {:loadbalancer,
            %ClientResponse{succ: succ, method: method, key: key, res: val, context: context}} ->
            send(caller, {succ, key, val, method})
        end
      end)
    receive do
      {succ, key, val, method} -> assert succ == :ok
                                  assert key == "1"
                                  assert val == [1]
                                  assert method == :put
    end

    send(:b, :data_sync)
    receive do
      after
        5000 -> :ok
    end


    send(:b, {:read_replica, "1"})
    receive do
      {val, context} -> assert val == [1]
    end

  after
    Emulation.terminate()
  end
end


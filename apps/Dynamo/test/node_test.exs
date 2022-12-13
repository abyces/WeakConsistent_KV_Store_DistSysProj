defmodule NodeTest do
  use ExUnit.Case
  doctest Node
  import Emulation, only: [spawn: 2, send: 2, whoami: 0]

  import Kernel,
         except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]


#  test "test put" do
#    Emulation.init()
#    new_loadbalancer = LoadBalancer.new_loadbalancer([:a, :b, :c], 1000, 1, 500)
#
#    spawn(:loadbalancer, fn ->
#      LoadBalancer.init_loadbalancer(new_loadbalancer)
#    end)
#
#    newnode = DynamoNode.new_configuration([:a, :b, :c], 0, 50, 100, 100, 2000, 2000, 1, 1, 400, 400)
#
#    spawn(:a, fn -> DynamoNode.node_init(newnode) end)
#    spawn(:b, fn -> DynamoNode.node_init(newnode) end)
#    spawn(:c, fn -> DynamoNode.node_init(newnode) end)
#    caller = self()
#
#    receive do
#    after
#      200 -> :ok
#    end
#
#    client =
#      spawn(:client, fn ->
#        send(:loadbalancer, ClientPutRequest.new("1", 1, nil))
#
#        receive do
#          {:loadbalancer, {node_status, origin}} ->
#            send(caller, {node_status, origin})
#
#          {:loadbalancer,
#            %ClientResponse{succ: succ, method: method, key: key, res: val, context: context}} ->
#            send(caller, {succ, key, val, method})
#        end
#      end)
#    receive do
#      {succ, key, val, method} -> assert succ == :ok
#                          assert key == "1"
#                          assert val == [1]
#                          assert method == :put
#    end
#    after
#      Emulation.terminate()
#  end
#
#  test "test get" do
#    Emulation.init()
#    new_loadbalancer = LoadBalancer.new_loadbalancer([:a, :b, :c], 1000, 1, 500)
#
#    spawn(:loadbalancer, fn ->
#      LoadBalancer.init_loadbalancer(new_loadbalancer)
#    end)
#
#    newnode = DynamoNode.new_configuration([:a, :b, :c], 0, 50, 100, 100, 2000, 2000, 1, 1, 400, 400)
#
#    spawn(:a, fn -> DynamoNode.node_init(newnode) end)
#    spawn(:b, fn -> DynamoNode.node_init(newnode) end)
#    spawn(:c, fn -> DynamoNode.node_init(newnode) end)
#    caller = self()
#
#    receive do
#    after
#      200 -> :ok
#    end
#
#    client =
#      spawn(:client, fn ->
#        send(:loadbalancer, ClientPutRequest.new("1", 1, nil))
#
#        receive do
#          {:loadbalancer,
#            %ClientResponse{succ: succ, method: method, key: key, res: val, context: context}} ->
#            send(caller, {succ, key, val, method, context})
#            IO.puts("1234")
#        end
#      end)
#    receive do
#      {succ, key, val, method, context} -> assert succ == :ok
#                                  assert key == "1"
#                                  assert val == [1]
#                                  assert method == :put
#    end
#    IO.puts("put success")
#    client =
#      spawn(:client1, fn ->
#        send(:loadbalancer, ClientGetRequest.new("1"))
#
#        receive do
#          {:loadbalancer,
#            %ClientResponse{succ: succ, method: method, key: key, res: val, context: context}} ->
#            send(caller, {succ, key, val, method})
#            IO.puts("123")
#        end
#      end)
#    receive do
#      {succ, key, val, method} -> assert succ == :ok
#                                  assert key == "1"
#                                  assert val == [1]
#                                  assert method == :get
#    end
#
#    client =
#      spawn(:client2, fn ->
#        send(:loadbalancer, ClientPutRequest.new("1", 2, %{:a => 1}))
#
#        receive do
#          {:loadbalancer,
#            %ClientResponse{succ: succ, method: method, key: key, res: val, context: context}} ->
#            send(caller, {succ, key, val, method})
#            IO.puts("123")
#        end
#      end)
#    receive do
#      {succ, key, val, method} -> assert succ == :ok
#                                  assert key == "1"
#                                  assert val == [2]
#                                  assert method == :put
#    end
#
#    client =
#      spawn(:client3, fn ->
#        send(:loadbalancer, ClientGetRequest.new("2"))
#
#        receive do
#          {:loadbalancer,
#            %ClientResponse{succ: succ, method: method, key: key, res: val, context: context}} ->
#            send(caller, {succ, key, val, method})
#            IO.puts("123")
#        end
#      end)
#    receive do
#      {succ, key, val, method} -> assert succ == :no_such_key
#                                  assert key == "2"
#                                  assert val == nil
#                                  assert method == :get
#    end
#  after
#    Emulation.terminate()
#  end

  test "hint data 1" do
    Emulation.init()
    new_loadbalancer = LoadBalancer.new_loadbalancer([:a, :b, :c, :d, :e], 1000, 2, 1000)

    spawn(:loadbalancer, fn ->
      LoadBalancer.init_loadbalancer(new_loadbalancer)
    end)
    # [node_list, replica_count, heartbeat_timeout, gossip_check_timeout, gossip_send_timeout, fail_timeout, cleanup_timeout, r_cnt, w_cnt]
    newnode = DynamoNode.new_configuration([:a, :b, :c, :d, :e], 2, 200, 500, 500, 2000, 2000, 1, 1, 400, 400)

    spawn(:a, fn -> DynamoNode.node_init(newnode) end)
    spawn(:b, fn -> DynamoNode.node_init(newnode) end)
    spawn(:c, fn -> DynamoNode.node_init(newnode) end)
    spawn(:d, fn -> DynamoNode.node_init(newnode) end)
    spawn(:e, fn -> DynamoNode.node_init(newnode) end)
    caller = self()

    send(:e, {:reset_heartbeat_timeout, 100_000})

    receive do
      after
        5000 -> :ok
    end

    client = spawn(:client, fn ->
      send(:loadbalancer, ClientPutRequest.new("1", 1, nil))

      receive do
        {:loadbalancer,
          %ClientResponse{succ: succ, method: method, key: key, res: val, context: context}} ->
          assert succ == :ok
          assert method == :put
          assert key == "1"
          assert val == [1]
          assert context == %{c: 1}
          send(caller, {succ, key, val, method})
          IO.puts("123")
      end

      receive do
        after
          1000 -> :ok
      end

      send(:loadbalancer, ClientGetRequest.new("1"))

      receive do
        {:loadbalancer,
          %ClientResponse{succ: succ, method: method, key: key, res: val, context: context}} ->
          assert succ == :ok
          assert method == :get
          assert key == "1"
          assert val == [1]
          assert context == %{c: 1}
          send(caller, {succ, key, val, method})
          IO.puts("1234")
      end

      send(:e, {:reset_heartbeat_timeout, 200})

      receive do
        after
          5000 -> :ok
      end
      send(:loadbalancer, ClientGetRequest.new("1"))

      receive do
        {:loadbalancer,
          %ClientResponse{succ: succ, method: method, key: key, res: val, context: context}} ->
          assert succ == :ok
          assert method == :get
          assert key == "1"
          assert val == [1]
          assert context == %{c: 1}
          send(caller, {succ, key, val, method})
          IO.puts("12345")
      end
    end)

    receive do
      {succ, key, val, method} -> assert succ == :ok
                                  assert method == :put
                                  assert key == "1"
                                  assert val == [1]
    end

    receive do
      {succ, key, val, method} -> assert succ == :ok
                                  assert method == :get
                                  assert key == "1"
                                  assert val == [1]
    end

    receive do
      {succ, key, val, method} -> assert succ == :ok
                                  assert method == :get
                                  assert key == "1"
                                  assert val == [1]
    end
    IO.puts("123456")
  after
    Emulation.terminate()
  end


  test "hint data" do
    Emulation.init()
    new_loadbalancer = LoadBalancer.new_loadbalancer([:a, :b, :c], 1000, 2, 500)

    spawn(:loadbalancer, fn ->
      LoadBalancer.init_loadbalancer(new_loadbalancer)
    end)
    # [node_list, replica_count, heartbeat_timeout, gossip_check_timeout, gossip_send_timeout, fail_timeout, cleanup_timeout, r_cnt, w_cnt]
    newnode = DynamoNode.new_configuration([:a, :b, :c], 2, 200, 300, 3000, 300, 300, 1, 1, 400, 400)

    spawn(:a, fn -> DynamoNode.node_init(newnode) end)
    spawn(:b, fn -> DynamoNode.node_init(newnode) end)
    spawn(:c, fn -> DynamoNode.node_init(newnode) end)
    caller = self()

    client = spawn(:client, fn ->
      send(:loadbalancer, ClientPutRequest.new("1", 1, nil))

      receive do
        {:loadbalancer,
          %ClientResponse{succ: succ, method: method, key: key, res: val, context: context}} ->
          assert succ == :ok
          assert method == :put
          assert key == "1"
          assert val == [1]
          assert context == %{a: 1}
          send(caller, {succ, key, val, method})
          IO.puts("123")
      end

      send(:a, {:reset_heartbeat_timeout, 10000})

      send(:b, {:read_replica, "1"})

      receive do
        {sender, {val, context}} -> assert val == [1]
                                    assert context == %{a: 1}
                                    assert sender == :b
      end

      receive do
        after
        5000 -> :ok
      end

      send(:a, {:read_replica, "1"})

      receive do
        {sender, {val, context}} -> assert val == [1]
                                    assert context == %{a: 1}
                                    assert sender == :a
      end

      send(:loadbalancer, ClientPutRequest.new("1", 2, %{a: 1}))

      receive do
        {:loadbalancer,
          %ClientResponse{succ: succ, method: method, key: key, res: val, context: context}} ->
          assert succ == :ok
          assert method == :put
          assert key == "1"
          assert context == %{a: 1, b: 1}
          assert val == [2]

          send(caller, {succ, key, val, method})
          IO.puts("1234")
      end

      send(:a, {:reset_heartbeat_timeout, 200})
      IO.puts("1234567")
      receive do
        after
          300 -> :ok
      end

      send(:a, {:read_hinted_replica, "1", :c})
      IO.puts("1234-")
      receive do
        {sender, {val, context}} -> assert val == [2]
                                    assert context == %{a: 1, b: 1}
      end
      IO.puts("1234*")
      receive do
      after
        1000 -> :ok
      end

      send(:loadbalancer, ClientPutRequest.new("1", 3, %{a: 1, c: 1}))

      receive do
        {:loadbalancer,
          %ClientResponse{succ: succ, method: method, key: key, res: val, context: context}} ->
          assert succ == :conflict
          assert method == :put
          assert key == "1"
          assert val == [2, 3]
          assert context == %{a: 2, c: 1, b: 1}
          send(caller, {succ, key, val, method})
          IO.puts("1234+")
      end

    end)

    receive do
      {succ, key, val, method} -> assert succ == :ok
                                  assert method == :put
                                  assert key == "1"
                                  assert val == [1]
    end

    receive do
      {succ, key, val, method} -> assert succ == :ok
                                  assert method == :put
                                  assert key == "1"
                                  assert val == [2]
    end

    receive do
      {succ, key, val, method} -> assert succ == :conflict
                                  assert method == :put
                                  assert key == "1"
                                  assert val == [2, 3]
    end
    IO.puts("123456")
  after
    Emulation.terminate()
  end
#
#  test "replica works" do
#    Emulation.init()
#    new_loadbalancer = LoadBalancer.new_loadbalancer([:a, :b, :c], 1000, 2, 500)
#
#    spawn(:loadbalancer, fn ->
#      LoadBalancer.init_loadbalancer(new_loadbalancer)
#    end)
#    # [node_list, replica_count, heartbeat_timeout, gossip_check_timeout, gossip_send_timeout, fail_timeout, cleanup_timeout, r_cnt, w_cnt]
#    newnode = DynamoNode.new_configuration([:a, :b, :c], 2, 200, 300, 3000, 300, 300, 1, 1, 100, 100)
#
#    spawn(:a, fn -> DynamoNode.node_init(newnode) end)
#    spawn(:b, fn -> DynamoNode.node_init(newnode) end)
#    spawn(:c, fn -> DynamoNode.node_init(newnode) end)
#    caller = self()
#
#    client = spawn(:client, fn ->
#
#      send(:a, {:reset_heartbeat_timeout, 100_000})
#
#      receive do
#        after
#          5000 -> :ok
#      end
#
#      send(:loadbalancer, ClientPutRequest.new("1", 1, nil))
#
#      receive do
#        {:loadbalancer,
#          %ClientResponse{succ: succ, method: method, key: key, res: val, context: context}} ->
#          assert succ == :ok
#          assert method == :put
#          assert key == "1"
#          assert val == [1]
#          assert context == %{b: 1}
#          send(caller, {succ, key, val, method})
#          IO.puts("123")
#      end
#
#      send(:b, {:read_hinted_replica, "1"})
#      receive do
#        {:b, {val, context}} -> assert val == [1]
#                                assert context == %{b: 1}
#                                send(caller, {val, context})
#      end
#    end)
#
#    receive do
#      {val, context} -> assert val == [1]
#                        assert context == %{b: 1}
#    end
#
#  after
#    Emulation.terminate()
#  end

#  test "r,w cnt" do
#    Emulation.init()
#    new_loadbalancer = LoadBalancer.new_loadbalancer([:a, :b, :c, :d], 1000, 3, 1000)
#
#    spawn(:loadbalancer, fn ->
#      LoadBalancer.init_loadbalancer(new_loadbalancer)
#    end)
#    # [node_list, replica_count, heartbeat_timeout, gossip_check_timeout, gossip_send_timeout, fail_timeout, cleanup_timeout, r_cnt, w_cnt]
#    newnode = DynamoNode.new_configuration([:a, :b, :c, :d], 3, 200, 500, 50000, 300, 300, 3, 3, 200, 200)
#
#    spawn(:a, fn -> DynamoNode.node_init(newnode) end)
#    spawn(:b, fn -> DynamoNode.node_init(newnode) end)
#    spawn(:c, fn -> DynamoNode.node_init(newnode) end)
#    spawn(:d, fn -> DynamoNode.node_init(newnode) end)
#    caller = self()
#
#
#
#    client = spawn(:client, fn ->
#        send(:loadbalancer, ClientPutRequest.new("1", 1, nil))
#
#        receive do
#          {:loadbalancer,
#            %ClientResponse{succ: succ, method: method, key: key, res: val, context: context}} ->
#            assert succ == :ok
#            assert method == :put
#            assert key == "1"
#            assert val == [1]
#            assert context == %{a: 1}
#            send(caller, {succ, key, val, method})
#            IO.puts("123")
#        end
#
#        send(:a, {:reset_heartbeat_timeout, 100_000})
#        send(:b, {:reset_heartbeat_timeout, 100_000})
#
#        receive do
#        after
#          5000 -> :ok
#        end
#
#        send(:loadbalancer, ClientGetRequest.new("1"))
#
#        receive do
#          {:loadbalancer,
#            %ClientResponse{succ: succ, method: method, key: key, res: val, context: context}} ->
#            assert succ == :timeout
#            assert method == :get
#            assert key == "1"
#            assert val == nil
#            assert context == nil
#            send(caller, {succ, key, val, method})
#            IO.puts("1234")
#        end
#    end)
#
#    receive do
#      {succ, key, val, method} -> assert succ == :ok
#                                  assert method == :put
#                                  assert key == "1"
#                                  assert val == [1]
#    end
#
#    receive do
#      {succ, key, val, method} -> assert succ == :timeout
#                                  assert method == :get
#                                  assert key == "1"
#                                  assert val == nil
#    end
#
#  after
#    Emulation.terminate()
#  end
end

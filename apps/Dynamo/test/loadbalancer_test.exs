defmodule LoadBalancerTest do
  use ExUnit.Case
  doctest LoadBalancer
  import Emulation, only: [spawn: 2, send: 2, whoami: 0]

  import Kernel,
         except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  def noderun(idx) do
    noderun(idx + 1)
  end

  test "load balancer can receive request from clients" do
    Emulation.init()
    new_loadbalancer = LoadBalancer.new_loadbalancer([:a, :b, :c], 100, 3)
    spawn(:loadbalancer, fn -> LoadBalancer.init_loadbalancer(new_loadbalancer) end)
    spawn(:a, fn -> noderun(1) end)
    spawn(:b, fn -> noderun(1) end)
    spawn(:c, fn -> noderun(1) end)
    caller = self()
    receive do
      after
        200 -> :ok
    end
    client =
      spawn(:client, fn ->
        send(:loadbalancer, ClientPutRequest.new("1", 1, nil))
        receive do
          {:loadbalancer, {node_status, origin}} ->
              send(caller, {node_status, origin})
          {:loadbalancer, %ClientResponse{succ: succ, res: val, context: context}} ->
              send(caller, {succ, val})
        end end)
    IO.puts("here")
    receive do
      {succ, val} ->
        assert succ == :fail
        assert val == nil
      {node_status, origin, avl} ->
#        IO.puts(AVLTree.view(avl))
        IO.puts(origin)
        IO.puts(node_status)
      m -> IO.puts("here: " + m)
    end
#  test "tree" do
#    tree = AVLTree.new()
#    tree = AVLTree.put(tree, 1)
#    tree = AVLTree.put(tree, 2)
#    tree = AVLTree.put(tree, 4)
#    tree = AVLTree.put(tree, 5)
#    tree = AVLTree.put(tree, 7)
#    tree = AVLTree.put(tree, 11)
#    tree = AVLTree.put(tree, 21)
#    tree = AVLTree.put(tree, 31)
#
#    assert AVLTree.inorder_traverse(tree) == [1, 2, 4, 5, 7, 11, 21, 31]
#    IO.puts(AVLTree.inorder_traverse(tree))
#    IO.puts(AVLTree.view(tree))
  end
end
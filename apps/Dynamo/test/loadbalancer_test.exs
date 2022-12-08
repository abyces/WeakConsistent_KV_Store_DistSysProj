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
    new_loadbalancer = LoadBalancer.new_loadbalancer([:a, :b, :c], 100000)
    spawn(:loadbalancer, fn -> LoadBalancer.run_loadbalancer(new_loadbalancer) end)
    spawn(:a, fn -> noderun(1) end)
    spawn(:b, fn -> noderun(1) end)
    spawn(:c, fn -> noderun(1) end)
    caller = self()
    client =
      spawn(:client, fn ->
        send(:loadbalancer, ClientPutRequest.new(1, 1))
        receive do
          {:loadbalancer, {node_status, origin}} ->
              send(caller, {node_status, origin})
        end end)
    IO.puts("here")
    receive do
      {node_status, origin} ->
        IO.puts(node_status)
      m -> IO.puts(m)
    end
  end
end
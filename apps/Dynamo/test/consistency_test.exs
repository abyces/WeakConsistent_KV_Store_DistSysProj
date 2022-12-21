defmodule ConsistencyTest do
  use ExUnit.Case
  @moduletag timeout: :infinity
  #  doctest Node
  import Emulation, only: [spawn: 2, send: 2, whoami: 0, timer: 2]

  import Kernel,
         except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  @spec test_with_diff_args([atom()], [atom()], float(), float(), non_neg_integer(), non_neg_integer(), non_neg_integer()) :: no_return()
  defp test_with_diff_args(view, msg_drop_rate, msg_delay, r, w, n, fail_rate) do
    Emulation.init()
    Emulation.append_fuzzers([Fuzzers.drop(msg_drop_rate), Fuzzers.delay(msg_delay)])
    correct_process = Enum.take_random(view, trunc(20 * (1 - fail_rate)))
    failed_process = Enum.filter(view, fn p -> Enum.member?(correct_process, p) == false end)
#    IO.puts(correct_process)
#    IO.puts(failed_process)
    # view, status_check_timeout, replica_cnt, request_timeout
    new_loadbalancer = LoadBalancer.new_loadbalancer(view, 200, n, trunc(5_000 + 2 * msg_delay))

    correct_process_config =
      DynamoNode.new_configuration(
        view, # node_list,
        n, # replica_count,
        50, # heartbeat_timeout,
        100, # gossip_check_timeout,
        100, # gossip_send_timeout,
        200, # fail_timeout,
        200, # cleanup_timeout,
        r, # r_cnt,
        w, # w_cnt,
        trunc(1_000 + 2 * msg_delay), # w_timeout,
        trunc(1_000 + 2 * msg_delay) # r_timeout
      )

    failed_process_config =
      DynamoNode.new_configuration(
        view, # node_list,
        n, # replica_count,
        100_000, # heartbeat_timeout,
        100, # gossip_check_timeout,
        100_000, # gossip_send_timeout,
        200, # fail_timeout,
        200, # cleanup_timeout,
        r, # r_cnt,
        w, # w_cnt,
        4_000 + 2 * msg_delay, # w_timeout,
        4_000 + 2 * msg_delay # r_timeout
      )

    Enum.each(correct_process, fn p -> spawn(p, fn -> DynamoNode.node_init(correct_process_config) end) end)
    Enum.each(failed_process, fn p -> spawn(p, fn -> DynamoNode.node_init(failed_process_config) end) end)
    spawn(:loadbalancer, fn ->
      LoadBalancer.init_loadbalancer(new_loadbalancer)
    end)

    caller = self()

    receive do
      after
        400 -> :ok
    end
    key = Integer.to_string(:rand.uniform(1000))
#    IO.puts("key: #{key}")
    client =
      spawn(:client, fn ->
        send(:loadbalancer, ClientPutRequest.new(key, 1, nil))
        receive do
        after
          1_000 -> :ok
        end

        send(:loadbalancer, ClientGetRequest.new(key))
        receive do
          {:loadbalancer,
            %ClientResponse{succ: succ, method: :get, key: key, res: val, context: context}} ->
#              IO.puts(succ)
              if succ == :ok && val == [1] do
                send(caller, :consistent)
              else
                send(caller, :inconsistent)
              end
          after
            5_000 -> #IO.puts("client timeout")
                      send(caller, :inconsistent)
        end
      end)
    receive do
        s -> case s do
               :consistent -> 1
               :inconsistent -> 0
             end
    end
    after
      Emulation.terminate()
  end

  test "measure consistency" do

    {:ok, file} = File.open("measure311.txt", [:append])

    view = [:a, :b, :c, :d, :e, :f, :g, :h, :i, :j, :k, :l, :m, :n, :o, :p, :q, :r, :s, :t]
#    msg_drop_rate = [0.0, 0.01, 0.05, 0.1, 0.2 ,0.25, 0.5]
    msg_drop_rate = [0.0]
#    msg_delay = [0.0, 1.0, 5.0, 10.0, 20.0, 50.0]
    msg_delay = [30.0, 40.0]
#    fail_rate = [0.0, 0.05, 0.1, 0.2, 0.25, 0.5]
    fail_rate = [0.0]
    r = 1
    w = 1
    n = 3
    for drop <- msg_drop_rate do
      for delay <- msg_delay do
        for fail <- fail_rate do
          res = for i <- 1..100 do
            test_with_diff_args(view, drop, delay, r, w, n, fail)
          end
          res = "20," <> Float.to_string(drop) <> "," <> Float.to_string(delay) <> ","  <> Float.to_string(fail) <> ",2" <> ",2" <> ",5," <> Integer.to_string(Enum.sum(res)) <> "\n"
          IO.puts(res)
          IO.write(file, res)
        end
      end
    end
    File.close(file)
  end
end
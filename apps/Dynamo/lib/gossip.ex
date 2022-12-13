defmodule Gossip do
  import Emulation,
         only: [send: 2, timer: 2, now: 0, whoami: 0, cancel_timer: 1]
  alias __MODULE__

#  require Emulation
#  require System
#  require Node

  # This function checks the node's gossip table periodically
  @spec gossip_table_periodical_check(%DynamoNode{}) :: %DynamoNode{}
  def gossip_table_periodical_check(state) do
    me = whoami()
    cur_time = System.os_time(:millisecond)


    new_gossip_table =
      Map.new(state.gossip_table, fn {node_id, {cnt, time, stat}} ->
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
  @spec gossip_table_merge(%DynamoNode{}, map()) :: %DynamoNode{}
  def gossip_table_merge(state, received_gossip_table) do
    me = whoami()
    cur_time = System.os_time(:millisecond)

    new_gossip_table =
      Map.new(state.gossip_table, fn {node_id, {cnt, time, stat}} ->
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
  @spec gossip_table_set_alive_by_id(%DynamoNode{}, atom()) :: %DynamoNode{}
  def gossip_table_set_alive_by_id(state, node_id) do

    {cnt, _, _} = state.gossip_table[node_id]

    %{
      state
      | gossip_table:
          Map.put(
            state.gossip_table,
            node_id,
            {cnt, System.os_time(:millisecond), :alive}
          )
    }
  end

  # This function updates the heartbeat cnt with particular node_id
  @spec gossip_table_update_heartbeat(%DynamoNode{}, atom()) :: %DynamoNode{}
  def gossip_table_update_heartbeat(state, node_id) do
    {cnt, _, _} = state.gossip_table[node_id]

    %{
      state
      | gossip_table:
          Map.put(
            state.gossip_table,
            node_id,
            {cnt + 1, System.os_time(:millisecond), :alive}
          )
    }

  end
end

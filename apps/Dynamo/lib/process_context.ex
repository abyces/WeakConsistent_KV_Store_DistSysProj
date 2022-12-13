defmodule Context do
  import Emulation,
         only: [send: 2, timer: 2, now: 0, whoami: 0, cancel_timer: 1]


  # This function merges the processed context from client with local contexts
  # (stored {val, contexts}, rev_context)
  @spec merge_context(list(), list()) :: list()
  def merge_context(stored_contexts, new_contexts) do
    res =
      case stored_contexts do
        nil -> new_contexts
        [head | rest] ->
          case rest do
            [] ->
              process_context_entry(head, new_contexts)

            _ ->
              process_context_entry(head, new_contexts) ++
              merge_context(rest, new_contexts)
          end
      end
    Enum.dedup(Enum.sort(res))
  end


  @spec process_context_entry(tuple(), list()) :: list()
  def process_context_entry({stored_val, stored_vector_clk}, rev_vector_clock) do
    compare =
      Enum.map(rev_vector_clock, fn {rev_val, rev_vec_clk} ->
        {rev_val, rev_vec_clk, VectorClock.compare_vectors(stored_vector_clk, rev_vec_clk)}
      end)

    res = []

    res =
      res ++
        case Enum.any?(compare, fn {v, vec_clk, cmp} -> cmp == :before end) do
          true -> []
          false -> [{stored_val, stored_vector_clk}]
        end

    res =
      res ++
      (compare
      |> Enum.filter(fn {v, vec_clk, cmp} -> cmp != :after end)
      |> Enum.map(fn {v, vec_clk, cmp} -> {v, vec_clk} end))
  end

  @spec increment_vector_clock(map()) :: map()
  def increment_vector_clock(vector_clk) do
    me = whoami()
    case Map.has_key?(vector_clk, me) do
      true ->
        Map.put(vector_clk, me, Map.get(vector_clk, me) + 1)

      false ->
        Map.put(vector_clk, me, 1)
    end
  end

  # This function process each context entry([map, map, ..]) from client
  # if me in context entry -> +1
  # else add(me => 1)
  @spec update_context_from_client(list(), list()) :: list()
  def update_context_from_client(context, local_data_list) do

    Enum.map(process_context_from_client(context, local_data_list),
      fn {context, cmp} -> increment_vector_clock(context) end)
  end

  @spec process_context_from_client(list(), list()) :: list()
  def process_context_from_client([head | rest], local_data_list) do
    res =
      case rest do
        [] ->
          [process_context_entry_without_vals_client(head, local_data_list)]

        _ ->
          [process_context_entry_without_vals_client(head, local_data_list)] ++
            process_context_from_client(rest, local_data_list)
      end
  end

  # This function compares each new context entry(vector clock) to each of original list [{v1, clk1}, ...]
  # and returns context without :before.
  @spec process_context_entry_without_vals_client(tuple(), list()) :: list()
  def process_context_entry_without_vals_client(context_entry, local_data_list) do
    compare =
      Enum.map(local_data_list, fn {v, vec_clk} ->
        {context_entry, VectorClock.compare_vectors(context_entry, vec_clk)}
      end)

    Enum.filter(compare, fn {context, cmp} -> cmp != :before end)
  end

  @spec make_vector_clock_full_length(%DynamoNode{}, [tuple()]) :: [tuple()]
  def make_vector_clock_full_length(state, context) do
    case context do
      [] ->
        []

      [{val, clk} | tl] ->
        [
          {val,
           state.node_list
           |> Enum.map(fn node ->
             case Map.hasKey?(clk, node) do
               true -> {node, clk[node]}
               false -> {node, 0}
             end
           end)
           |> Map.new()}
        ] ++ make_vector_clock_full_length(state, tl)
    end
  end

  @spec remove_zero_from_vector_clock([tuple()]) :: [tuple()]
  def remove_zero_from_vector_clock(context) do
    case context do
      [] ->
        []

      [{val, clk} | tl] ->
        [{val, clk |> Enum.filter(fn {_, v} -> v != 0 end) |> Map.new()}] ++
          remove_zero_from_vector_clock(tl)
    end
  end

  @spec context_merge_check_for_put_request(%DynamoNode{}, [tuple()], map()) :: [tuple()]
  def context_merge_check_for_put_request(state, local_context, client_context) do
      [full_length_client_context | _] = make_vector_clock_full_length(state, [client_context])
      merged_context = process_context_entry(client_context, local_context)
  end

  @spec context_summary_helper(map(), list()) :: tuple()
  def context_summary_helper(res_context, local_context) do
    case local_context do
      [] ->
        res_context

      [{val, context} | tl] ->
        context_summary_helper(
          Map.merge(
            res_context,
            context,
            fn _k, v1, v2 -> max(v1, v2) end
          ),
          tl
        )
    end
  end

  @spec get_context_summary(%DynamoNode{}, any()) :: tuple()
  def get_context_summary(state, key) do
    data = state.data[key]
    val = Enum.map(data, fn {v, clk} -> v end)
    context = context_summary_helper(%{}, data)
    {val, context}
  end

  @spec get_context_summary_hinted_data(%DynamoNode{}, any(), atom()) :: tuple()
  def get_context_summary_hinted_data(state, key, original_target) do

    data = case Map.has_key?(state.data, key) do
      true -> state.data[key]
      false -> state.hinted_data[original_target][key]
    end

#    local_data = Map.get(state.data, key)
#    local_hinted_data = Map.get(state.hinted_data[original_target], key)
#    data = case local_data do
#      nil -> local_hinted_data
#      _ -> case local_hinted_data do
#             nil -> local_data
#             _ -> Context.merge_context(local_data, local_hinted_data)
#           end
#    end

    val = Enum.map(data, fn {v, clk} -> v end)
    context = context_summary_helper(%{}, data)
    {val, context}

  end
end

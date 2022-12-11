defmodule VectorClock do

  @before :before
  @hafter :after
  @concurrent :concurrent

  # Combine a single component in a vector clock.
  @spec combine_component(non_neg_integer(), non_neg_integer()) :: non_neg_integer()
  defp combine_component(current, received) do
    if current > received do
      current
    else
      received
    end
  end

  # Combine two vector clocks
  @spec combine_vector_clocks(map(), map()) :: map()
  def combine_vector_clocks(current, received) do
    Map.merge(current, received, fn _k, c, r -> combine_component(c, r) end)
  end

  # update clock when event happens
  @spec update_vector_clock(atom(), map()) :: map()
  def update_vector_clock(proc, clock) do
    Map.update!(clock, proc, fn t -> t + 1 end)
  end

  # This function makes two vector clk equal length
  @spec make_vectors_equal_length(map(), map()) :: map()
  defp make_vectors_equal_length(v1, v2) do
    v1_add = for {k, _} <- v2, !Map.has_key?(v1, k), do: {k, 0}
    Map.merge(v1, Enum.into(v1_add, %{}))
  end

  @spec compare_component(non_neg_integer(),non_neg_integer()) :: :before | :after | :concurrent
  defp compare_component(c1, c2) do
    if c1 == c2 do
      :concurrent
    else
      if c1 < c2 do
        :before
      else
        :after
      end
    end
  end

  # Compare two vector clocks v1 and v2. :before -> v1 < v2.
  @spec compare_vectors(map(), map()) :: :before | :after | :concurrent
  def compare_vectors(v1, v2) do
    v1 = make_vectors_equal_length(v1, v2)
    v2 = make_vectors_equal_length(v2, v1)
    compare_result = Map.values(Map.merge(v1, v2, fn _k, c1, c2 -> compare_component(c1, c2) end))

    if Enum.all?(compare_result, fn x -> x == @before or x == @concurrent end) and
       Enum.any?(compare_result, fn x -> x == @before end) do
      :before
    else
      if Enum.all?(compare_result, fn x -> x == @hafter or x == @concurrent end) and
         Enum.any?(compare_result, fn x -> x == @hafter end) do
        :after
      else
        :concurrent
      end
    end
  end
end
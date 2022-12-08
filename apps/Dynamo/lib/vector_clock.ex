defmodule VectorClock do
  @doc """
  Combine vector clocks: this is called whenever a
  message is received, and should return the clock
  from combining the two.
  """
  @spec combine_vector_clocks(map(), map()) :: map()
  def combine_vector_clocks(current, received) do
    Map.merge(current, received, fn _k, c, r -> combine_component(c, r) end)
  end

  @doc """
  This function is called by the process `proc` whenever an
  event occurs, which for our purposes means whenever a message
  is received or sent.
  """
  @spec update_vector_clock(atom(), map()) :: map()
  def update_vector_clock(proc, clock) do
    Map.update!(clock, proc, fn t -> t + 1 end)
  end

  @before :before
  @hafter :after
  @concurrent :concurrent

  # Produce a new vector clock that is a copy of v1,
  # except for any keys (processes) that appear only
  # in v2, which we add with a 0 value. This function
  # is useful in making it so all process IDs do not
  # need to be known a-priori.
  @spec make_vectors_equal_length(map(), map()) :: map()
  defp make_vectors_equal_length(v1, v2) do
    v1_add = for {k, _} <- v2, !Map.has_key?(v1, k), do: {k, 0}
    Map.merge(v1, Enum.into(v1_add, %{}))
  end

  @spec compare_component(
          non_neg_integer(),
          non_neg_integer()
        ) :: :before | :after | :concurrent
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

  @doc """
  Compare two vector clocks v1 and v2.
  Returns @before if v1 happened before v2.
  Returns @hafter if v2 happened before v1.
  Returns @concurrent if neither of the above hold.
  """
  @spec compare_vectors(map(), map()) :: :before | :after | :concurrent
  def compare_vectors(v1, v2) do
    v1 = make_vectors_equal_length(v1, v2)
    v2 = make_vectors_equal_length(v2, v1)
    compare_result =
      Map.values(
        Map.merge(v1, v2, fn _k, c1, c2 -> compare_component(c1, c2) end)
      )

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
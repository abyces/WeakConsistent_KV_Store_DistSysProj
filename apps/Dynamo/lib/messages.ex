defmodule ClientPutRequest do
  alias __MODULE__

  @enforce_keys [:key, :val]
  defstruct(
    key: nil,
    val: nil
  )

  @spec new(non_neg_integer(), non_neg_integer()) :: %ClientPutRequest{}
  def new(k, v) do
    %ClientPutRequest{key: k, val: v}
  end

end

defmodule ClientGetRequest do
  alias __MODULE__

  @enforce_keys [:key]
  defstruct(
    key: nil
  )

  @spec new(non_neg_integer()) :: %ClientGetRequest{}
  def new(k) do
    %ClientGetRequest{key: k}
  end
end


defmodule ClientResponse do
  alias __MODULE__

  @enforce_keys [:status]
  defstruct(
    succ: nil,
    res: nil
  )

  @spec new_response(atom(), non_neg_integer()) :: %ClientResponse{}
  def new_response(succ, val) do
    %ClientResponse{succ: succ, res: val}
  end
end


defmodule CoordinateRequest do
  alias __MODULE__

  @enforce_keys [:client, :method, :hint, :key]
  defstruct(
    client: nil,
    method: nil,
    hint: nil,
    key: nil,
    val: nil
  )

  @spec new_put_request(atom(), atom(), non_neg_integer(), non_neg_integer()) :: %CoordinateRequest{}
  def new_put_request(client, hint, k, v) do
    %CoordinateRequest{client: client, method: :put, hint: hint, key: k, val: v}
  end

  @spec new_get_request(atom(), atom(), non_neg_integer()) :: %CoordinateRequest{}
  def new_get_request(client, hint, k) do
    %CoordinateRequest{client: client, method: :get, hint: hint, key: k}
  end
end


defmodule CoordinateResponse do
  alias __MODULE__

  @enforce_keys [:client, :succ, :method, :vector_clock, :key]
  defstruct(
    client: nil,
    succ: nil,
    method: nil,
    key: nil,
    val: nil,
  )

  @spec new_put_response(atom(), atom(), non_neg_integer) :: %CoordinateResponse{}
  def new_put_response(client, succ, k) do
    %CoordinateResponse{client: client, succ: succ, method: :put, key: k, val: nil}
  end
  @spec new_get_response(atom(), atom(), non_neg_integer(), non_neg_integer()) :: %CoordinateResponse{}
  def new_get_response(client, succ, k, val) do
    %CoordinateResponse{client: client, succ: succ, method: :get, key: k, val: val}
  end
end


defmodule GossipMessage do
  alias __MODULE__

  @enforce_keys [:gossip_table]
  defstruct(
    gossip_table: nil
  )

  @spec new_gossip_message(any()) :: %GossipMessage{}
  def new_gossip_message(gossip_table) do
    %GossipMessage{gossip_table: gossip_table}
  end
end


defmodule RelicaPutRequest do
  alias __MODULE__

  @enforce_keys [:key, :val, :vector_clock]
  defstruct(
    key: nil,
    val: nil,
    vector_clock: nil,
  )

  @spec new(map(), non_neg_integer(), non_neg_integer()) :: %RelicaPutRequest{}
  def new(k, v, vector_clock) do
    %RelicaPutRequest{key: k, val: v, vector_clock: vector_clock}
  end
end

defmodule RelicaPutResponse do
  alias __MODULE__

  @enforce_keys [:key, :succ, :vector_clock]
  defstruct(
    key: nil,
    succ: nil,
    vector_clock: nil
  )

  @spec new(map(), non_neg_integer(), non_neg_integer()) :: %RelicaPutResponset{}
  def new(k, succ, vector_clock) do
    %RelicaPutResponset{key: k, succ: succ, vector_clock: vector_clock}
  end
end

defmodule RelicaGetRequest do
  alias __MODULE__

  @enforce_keys [:key, :vector_clock]
  defstruct(
    key: nil,
    vector_clock: nil,
  )

  @spec new(map(), non_neg_integer(), non_neg_integer()) :: %RelicaPutReponse{}
  def new(k, v, vector_clock) do
    %RelicaPutReponse{key: k, vector_clock: vector_clock}
  end
end

defmodule RelicaGetResponse do
  alias __MODULE__

  @enforce_keys [:key, :val, :succ, :vector_clock]
  defstruct(
    key: nil,
    val: nil,
    succ: nil,
    vector_clock: nil
  )

  @spec new(map(), non_neg_integer(), non_neg_integer()) :: %RelicaGetResponset{}
  def new(k, v, succ, vector_clock) do
    %RelicaGetResponset{key: k, val: v, succ: succ, vector_clock: vector_clock}
  end
end
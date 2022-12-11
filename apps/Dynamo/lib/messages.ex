defmodule ClientPutRequest do
  alias __MODULE__

  @enforce_keys [:key, :val, :context]
  defstruct(
    key: nil,
    val: nil,
    context: nil
  )

  @spec new(non_neg_integer(), non_neg_integer(), map()) :: %ClientPutRequest{}
  def new(k, v, context) do
    %ClientPutRequest{key: k, val: v, context: context}
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

  @enforce_keys [:succ]
  defstruct(
    succ: nil,
    res: nil,
    context: nil
  )

  @spec new_response(atom(), non_neg_integer(), map()) :: %ClientResponse{}
  def new_response(succ, val, context) do
    %ClientResponse{succ: succ, res: val, context: context}
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
    val: nil,
    context: nil,
  )

  @spec new_put_request(atom(), atom(), non_neg_integer(), non_neg_integer(), list()) :: %CoordinateRequest{}
  def new_put_request(client, hint, k, v, context) do
    %CoordinateRequest{client: client, method: :put, hint: hint, key: k, val: v, context: context}
  end

  @spec new_get_request(atom(), atom(), non_neg_integer()) :: %CoordinateRequest{}
  def new_get_request(client, hint, k) do
    %CoordinateRequest{client: client, method: :get, hint: hint, key: k}
  end
end


defmodule CoordinateResponse do
  alias __MODULE__

  @enforce_keys [:client, :succ, :method, :key]
  defstruct(
    client: nil,
    succ: nil,
    method: nil,
    key: nil,
    val: nil,
    context: nil
  )

  @spec new_put_response(atom(), atom(), non_neg_integer) :: %CoordinateResponse{}
  def new_put_response(client, succ, k) do
    %CoordinateResponse{client: client, succ: succ, method: :put, key: k, val: nil, context: nil}
  end
  @spec new_get_response(atom(), atom(), non_neg_integer(), non_neg_integer(), map()) :: %CoordinateResponse{}
  def new_get_response(client, succ, k, val, context) do
    %CoordinateResponse{client: client, succ: succ, method: :get, key: k, val: val, context: context}
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


defmodule ReplicaPutRequest do
  alias __MODULE__

  @enforce_keys [:client, :key, :context, :hint]
  defstruct(
    client: nil,
    key: nil,
    context: nil,
    hint: nil
  )

  @spec new(atom(), any(), [tuple()], atom()) :: %ReplicaPutRequest{}
  def new(client, k, context, hint) do
    %ReplicaPutRequest{client: client, key: k, context: context, hint: hint}
  end
end

defmodule ReplicaPutResponse do
  alias __MODULE__

  @enforce_keys [:key, :succ, :client]
  defstruct(
    client: nil,
    key: nil,
    succ: nil
  )

  @spec new(atom(), any(), atom()) :: %ReplicaPutResponse{}
  def new(client, k, succ) do
    %ReplicaPutResponse{client: client, key: k, succ: succ}
  end
end

defmodule ReplicaGetRequest do
  alias __MODULE__

  @enforce_keys [:client, :key, :hint]
  defstruct(
    client: nil,
    key: nil,
    hint: nil
  )

  @spec new(atom(), any(), atom()) :: %ReplicaGetRequest{}
  def new(client, k, hint) do
    %ReplicaGetRequest{client: client, key: k, hint: hint}
  end
end

defmodule ReplicaGetResponse do
  alias __MODULE__

  @enforce_keys [:client, :key, :context, :succ]
  defstruct(
    client: nil,
    key: nil,
    context: nil,
    succ: nil,
  )

  @spec new(atom(), any(), atom(), [tuple()]) :: %ReplicaGetResponse{}
  def new(client, k, succ, context) do
    %ReplicaGetResponse{client: client, key: k, context: context, succ: succ}
  end
end

defmodule HintedDataRequest do
  alias __MODULE__

  @enforce_keys [:key, :val, :context]
  defstruct(
    key: nil,
    val: nil,
    context: nil
  )

  @spec new(non_neg_integer(), non_neg_integer(), map()) :: %HintedDataRequest{}
  def new(k, v, context) do
    %HintedDataRequest{key: k, val: v, context: context}
  end

end

defmodule HintedDataResponse do
  alias __MODULE__

  @enforce_keys [:key, :succ]
  defstruct(
    key: nil,
    succ: nil
  )

  @spec new(any(), atom()) :: %HintedDataResponse{}
  def new(key, succ) do
    %HintedDataResponse{key: key, succ: succ}
  end
end
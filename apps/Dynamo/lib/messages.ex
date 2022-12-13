# Put/Get Messages that client sends/receives to/from load balancer
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
  defstruct(key: nil)

  @spec new(non_neg_integer()) :: %ClientGetRequest{}
  def new(k) do
    %ClientGetRequest{key: k}
  end
end


#Put/Get Messages that client sends/receives to/from load balancer
defmodule ClientResponse do
  alias __MODULE__

  @enforce_keys [:succ, :key]
  defstruct(
    succ: nil,
    method: nil,
    key: nil,
    res: nil,
    context: nil
  )

  @spec new_response(atom(), atom(), any(), any(), map()) :: %ClientResponse{}
  def new_response(succ, method, key, val, context) do
    %ClientResponse{succ: succ, method: method, key: key, res: val, context: context}
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
    vector_clock: nil
  )

  @spec new_put_request(
          atom(),
          atom(),
          non_neg_integer(),
          non_neg_integer(),
          map()
        ) :: %CoordinateRequest{}
  def new_put_request(client, hint, k, v, vector_clock) do
    %CoordinateRequest{
      client: client,
      method: :put,
      hint: hint,
      key: k,
      val: v,
      vector_clock: vector_clock
    }
  end

  @spec new_get_request(atom(), atom(), any()) ::
          %CoordinateRequest{}
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

  @spec new_put_response(atom(), atom(), any(), any(), map()) ::
          %CoordinateResponse{}
  def new_put_response(client, succ, k, val, context) do
    %CoordinateResponse{
      client: client,
      succ: succ,
      method: :put,
      key: k,
      val: val,
      context: context
    }
  end

  @spec new_get_response(
          atom(),
          atom(),
          non_neg_integer(),
          non_neg_integer(),
          map()
        ) :: %CoordinateResponse{}
  def new_get_response(client, succ, k, val, context) do
    %CoordinateResponse{
      client: client,
      succ: succ,
      method: :get,
      key: k,
      val: val,
      context: context
    }
  end
end

defmodule GossipMessage do
  alias __MODULE__

  @enforce_keys [:gossip_table]
  defstruct(gossip_table: nil)

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
    %ReplicaPutRequest{
      client: client,
      key: k,
      context: context,
      hint: hint
    }
  end
end

defmodule ReplicaPutResponse do
  alias __MODULE__

  @enforce_keys [:key, :succ, :client, :hint]
  defstruct(
    client: nil,
    key: nil,
    succ: nil,
    hint: nil
  )

  @spec new(atom(), any(), atom(), atom()) :: %ReplicaPutResponse{}
  def new(client, k, succ, hint) do
    %ReplicaPutResponse{client: client, key: k, succ: succ, hint: hint}
  end
end

defmodule ReplicaGetRequest do
  alias __MODULE__

  @enforce_keys [:client, :key, :hint, :source]
  defstruct(
    client: nil,
    key: nil,
    hint: nil,
    source: nil,
  )

  @spec new(atom(), any(), atom(), atom()) :: %ReplicaGetRequest{}
  def new(client, k, hint, source) do
    %ReplicaGetRequest{client: client, key: k, hint: hint, source: source}
  end
end

defmodule ReplicaGetResponse do
  alias __MODULE__

  @enforce_keys [:client, :key, :context, :succ, :hint, :source]
  defstruct(
    client: nil,
    key: nil,
    context: nil,
    succ: nil,
    hint: nil,
    source: nil
  )

  @spec new(atom(), any(), [tuple()], atom(), atom(), atom()) :: %ReplicaGetResponse{}
  def new(client, k, context, succ, hint, source) do
    %ReplicaGetResponse{client: client, key: k, context: context, succ: succ, hint: hint, source: source}
  end
end

defmodule HintedDataRequest do
  alias __MODULE__

  @enforce_keys [:key, :data_list]
  defstruct(
    key: nil,
    data_list: nil
  )

  @spec new(non_neg_integer(), list()) :: %HintedDataRequest{}
  def new(k, data_list) do
    %HintedDataRequest{key: k, data_list: data_list}
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

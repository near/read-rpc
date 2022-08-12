use near_jsonrpc_client::errors::JsonRpcError;
use std::ops::{Deref, DerefMut};
type BoxedSerialize = Box<dyn erased_serde::Serialize + Send>;

#[derive(Debug)]
pub struct RPCError(pub(crate) near_jsonrpc_primitives::errors::RpcError);

impl RPCError {
    pub fn invalid_request() -> Self {
        Self {
            0: near_jsonrpc_primitives::errors::RpcError::new(
                -32600,
                String::from("Invalid Request"),
                None,
            ),
        }
    }
    pub fn invalid_params() -> Self {
        Self {
            0: near_jsonrpc_primitives::errors::RpcError::new(
                -32602,
                String::from("Invalid params"),
                None,
            ),
        }
    }
}

impl Deref for RPCError {
    type Target = near_jsonrpc_primitives::errors::RpcError;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for RPCError {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl std::fmt::Display for RPCError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl jsonrpc_v2::ErrorLike for RPCError {
    fn code(&self) -> i64 {
        self.code
    }
    fn message(&self) -> String {
        self.message.to_string()
    }
    fn data(&self) -> Option<BoxedSerialize> {
        Some(Box::new(self.data.clone()))
    }
}

impl From<near_jsonrpc_primitives::errors::RpcError> for RPCError {
    fn from(rpc_error: near_jsonrpc_primitives::errors::RpcError) -> Self {
        Self(rpc_error)
    }
}

impl From<near_jsonrpc_primitives::types::blocks::RpcBlockError> for RPCError {
    fn from(err: near_jsonrpc_primitives::types::blocks::RpcBlockError) -> Self {
        near_jsonrpc_primitives::errors::RpcError::from(err).into()
    }
}

impl From<near_jsonrpc_primitives::types::query::RpcQueryError> for RPCError {
    fn from(err: near_jsonrpc_primitives::types::query::RpcQueryError) -> Self {
        near_jsonrpc_primitives::errors::RpcError::from(err).into()
    }
}

impl<E> From<JsonRpcError<E>> for RPCError
where
    near_jsonrpc_primitives::errors::RpcError: std::convert::From<E>,
{
    fn from(err: JsonRpcError<E>) -> Self {
        if let Some(error) = err.handler_error() {
            near_jsonrpc_primitives::errors::RpcError::from(error).into()
        } else {
            Self {
                0: near_jsonrpc_primitives::errors::RpcError::serialization_error(
                    "Failed to serialize JsonRpcError".to_string(),
                ),
            }
        }
    }
}

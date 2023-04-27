use near_jsonrpc_client::errors::{JsonRpcError, JsonRpcServerError};
use std::ops::{Deref, DerefMut};
type BoxedSerialize = Box<dyn erased_serde::Serialize + Send>;

#[derive(Debug)]
pub struct RPCError(pub(crate) near_jsonrpc_primitives::errors::RpcError);

impl RPCError {
    pub(crate) fn unimplemented_error(msg: &str) -> Self {
        Self::from(near_jsonrpc_primitives::errors::RpcError::new(
            -32601,
            String::from(msg),
            None,
        ))
    }

    pub(crate) fn internal_error(msg: &str) -> Self {
        Self::from(near_jsonrpc_primitives::errors::RpcError::new(
            -32603,
            String::from(msg),
            None,
        ))
    }

    pub(crate) fn parse_error(msg: &str) -> Self {
        Self::from(near_jsonrpc_primitives::errors::RpcError::new(
            -32700,
            String::from("Parse error"),
            Some(serde_json::json!(format!("Parse error: {}", msg))),
        ))
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

    fn error_struct(&self) -> Option<BoxedSerialize> {
        Some(Box::new(self.error_struct.clone()))
    }
}

impl From<near_jsonrpc_primitives::errors::RpcError> for RPCError {
    fn from(rpc_error: near_jsonrpc_primitives::errors::RpcError) -> Self {
        Self(rpc_error)
    }
}

impl<E> From<JsonRpcError<E>> for RPCError
where
    near_jsonrpc_primitives::errors::RpcError: std::convert::From<E>,
{
    fn from(err: JsonRpcError<E>) -> Self {
        if let JsonRpcError::ServerError(JsonRpcServerError::HandlerError(error)) = err {
            near_jsonrpc_primitives::errors::RpcError::from(error).into()
        } else {
            Self(
                near_jsonrpc_primitives::errors::RpcError::serialization_error(
                    "Failed to serialize JsonRpcError".to_string(),
                ),
            )
        }
    }
}

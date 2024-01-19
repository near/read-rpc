use std::ops::{Deref, DerefMut};

use near_jsonrpc_client::errors::{JsonRpcError, JsonRpcServerError};

type BoxedSerialize = Box<dyn erased_serde::Serialize + Send>;

#[derive(Debug, serde::Serialize)]
#[serde(transparent)]
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
    near_jsonrpc_primitives::errors::RpcError: From<E>,
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

#[derive(thiserror::Error, Debug, Clone)]
pub enum FunctionCallError {
    #[error("Account ID \"{requested_account_id}\" is invalid")]
    InvalidAccountId {
        requested_account_id: near_primitives::types::AccountId,
    },
    #[error("Account ID #{requested_account_id} does not exist")]
    AccountDoesNotExist {
        requested_account_id: near_primitives::types::AccountId,
    },
    #[error("Internal error: #{error_message}")]
    InternalError { error_message: String },
    #[error("VM error occurred: #{error_message}")]
    VMError { error_message: String },
}

impl FunctionCallError {
    pub fn to_rpc_query_error(
        &self,
        block_height: near_primitives::types::BlockHeight,
        block_hash: near_primitives::hash::CryptoHash,
    ) -> near_jsonrpc_primitives::types::query::RpcQueryError {
        match self.clone() {
            Self::InvalidAccountId {
                requested_account_id,
            } => near_jsonrpc_primitives::types::query::RpcQueryError::InvalidAccount {
                requested_account_id,
                block_height,
                block_hash,
            },
            Self::AccountDoesNotExist {
                requested_account_id,
            } => near_jsonrpc_primitives::types::query::RpcQueryError::UnknownAccount {
                requested_account_id,
                block_height,
                block_hash,
            },
            Self::InternalError { error_message } => {
                near_jsonrpc_primitives::types::query::RpcQueryError::InternalError {
                    error_message,
                }
            }
            Self::VMError { error_message } => {
                near_jsonrpc_primitives::types::query::RpcQueryError::ContractExecutionError {
                    vm_error: error_message,
                    block_height,
                    block_hash,
                }
            }
        }
    }
}

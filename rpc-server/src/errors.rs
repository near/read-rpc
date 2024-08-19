use std::ops::{Deref, DerefMut};

use near_jsonrpc_client::errors::{JsonRpcError, JsonRpcServerError};

type BoxedSerialize = Box<dyn erased_serde::Serialize + Send + 'static>;

#[derive(Debug, serde::Serialize)]
#[serde(transparent)]
pub struct RPCError(pub(crate) near_jsonrpc::primitives::errors::RpcError);

impl From<RPCError> for near_jsonrpc::primitives::errors::RpcError {
    fn from(err: RPCError) -> Self {
        err.0
    }
}

impl RPCError {
    pub(crate) fn unimplemented_error(method_name: &str) -> Self {
        Self::from(near_jsonrpc::primitives::errors::RpcError::new(
            -32601,
            format!(
                "Method `{}` is not implemented on this type of node. \
                Please send a request to NEAR JSON RPC instead.",
                method_name
            ),
            None,
        ))
    }

    pub(crate) fn internal_error(msg: &str) -> Self {
        Self::from(near_jsonrpc::primitives::errors::RpcError::new(
            -32603,
            String::from(msg),
            None,
        ))
    }
}

impl Deref for RPCError {
    type Target = near_jsonrpc::primitives::errors::RpcError;

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

impl From<near_jsonrpc::primitives::errors::RpcParseError> for RPCError {
    fn from(parse_error: near_jsonrpc::primitives::errors::RpcParseError) -> Self {
        Self(near_jsonrpc::primitives::errors::RpcError::parse_error(
            parse_error.0,
        ))
    }
}

impl From<near_jsonrpc::primitives::errors::RpcError> for RPCError {
    fn from(rpc_error: near_jsonrpc::primitives::errors::RpcError) -> Self {
        Self(rpc_error)
    }
}

impl<E> From<JsonRpcError<E>> for RPCError
where
    near_jsonrpc::primitives::errors::RpcError: From<E>,
{
    fn from(err: JsonRpcError<E>) -> Self {
        if let JsonRpcError::ServerError(JsonRpcServerError::HandlerError(error)) = err {
            near_jsonrpc::primitives::errors::RpcError::from(error).into()
        } else {
            Self(
                near_jsonrpc::primitives::errors::RpcError::serialization_error(
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
    ) -> near_jsonrpc::primitives::types::query::RpcQueryError {
        match self.clone() {
            Self::InvalidAccountId {
                requested_account_id,
            } => near_jsonrpc::primitives::types::query::RpcQueryError::InvalidAccount {
                requested_account_id,
                block_height,
                block_hash,
            },
            Self::AccountDoesNotExist {
                requested_account_id,
            } => near_jsonrpc::primitives::types::query::RpcQueryError::UnknownAccount {
                requested_account_id,
                block_height,
                block_hash,
            },
            Self::InternalError { error_message } => {
                near_jsonrpc::primitives::types::query::RpcQueryError::InternalError {
                    error_message,
                }
            }
            Self::VMError { error_message } => {
                near_jsonrpc::primitives::types::query::RpcQueryError::ContractExecutionError {
                    vm_error: error_message,
                    block_height,
                    block_hash,
                }
            }
        }
    }
}

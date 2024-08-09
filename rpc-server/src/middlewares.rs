use crate::metrics::{METHOD_CALLS_COUNTER, RPC_METHODS};
use actix_web::dev::{forward_ready, Service, ServiceRequest, ServiceResponse, Transform};
use futures::future::LocalBoxFuture;
use futures::StreamExt;
use jsonrpc_v2::{Params, RequestObject};
use near_jsonrpc::RpcRequest;
use serde_json::Value;
use std::future::{ready, Ready};

// Middleware to count requests and methods calls
// This middleware is used to count the number of requests and the number of calls to each method
pub struct RequestsCounters;

impl<S, B> Transform<S, ServiceRequest> for RequestsCounters
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = actix_web::Error> + 'static,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = actix_web::Error;
    type Transform = RequestsCountersMiddleware<S>;
    type InitError = ();
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ready(Ok(RequestsCountersMiddleware {
            service: std::sync::Arc::new(service),
        }))
    }
}

pub struct RequestsCountersMiddleware<S> {
    service: std::sync::Arc<S>,
}

impl<S, B> Service<ServiceRequest> for RequestsCountersMiddleware<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = actix_web::Error> + 'static,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = actix_web::Error;
    type Future = LocalBoxFuture<'static, Result<Self::Response, Self::Error>>;

    forward_ready!(service);

    fn call(&self, request: ServiceRequest) -> Self::Future {
        let service_clone = self.service.clone();
        Box::pin(async move {
            if request.path() != "/" {
                return service_clone.call(request).await;
            }

            let (req, mut payload) = request.into_parts();
            let mut body = actix_web::web::BytesMut::new();
            while let Some(chunk) = &payload.next().await {
                match chunk {
                    Ok(chunk) => body.extend_from_slice(chunk),
                    Err(e) => {
                        tracing::error!("Error receiving payload: {:?}", e);
                    }
                };
            }

            if let Ok(obj) = serde_json::from_slice::<RequestObject>(&body) {
                let method = obj.method_ref();
                if method == "query" {
                    let params = match Params::from_request_object(&obj) {
                        Ok(Params(params)) => params,
                        Err(_) => {
                            tracing::error!("Error parsing request params");
                            Value::default()
                        }
                    };
                    if let Ok(query_request) =
                        near_jsonrpc::primitives::types::query::RpcQueryRequest::parse(params)
                    {
                        let method = match &query_request.request {
                            near_primitives::views::QueryRequest::ViewAccount { .. } => {
                                "query_view_account"
                            }
                            near_primitives::views::QueryRequest::ViewCode { .. } => {
                                "query_view_code"
                            }
                            near_primitives::views::QueryRequest::ViewState { .. } => {
                                "query_view_state"
                            }
                            near_primitives::views::QueryRequest::ViewAccessKey { .. } => {
                                "query_view_access_key"
                            }
                            near_primitives::views::QueryRequest::ViewAccessKeyList { .. } => {
                                "query_view_access_key_list"
                            }
                            near_primitives::views::QueryRequest::CallFunction { .. } => {
                                "query_call_function"
                            }
                        };
                        METHOD_CALLS_COUNTER.with_label_values(&[method]).inc()
                    }
                } else if RPC_METHODS.get(method).await.is_some() {
                    METHOD_CALLS_COUNTER.with_label_values(&[method]).inc()
                } else {
                    METHOD_CALLS_COUNTER
                        .with_label_values(&["method_not_found"])
                        .inc()
                }
            };

            let mut request = ServiceRequest::from_request(req);
            let (_, mut payload) = actix_http::h1::Payload::create(true);
            payload.unread_data(body.into());
            request.set_payload(payload.into());
            service_clone.call(request).await
        })
    }
}

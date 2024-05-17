use crate::metrics::{METHODS_CALLS_COUNTER, TOTAL_REQUESTS_COUNTER};
use actix_web::dev::{forward_ready, Service, ServiceRequest, ServiceResponse, Transform};
use futures::future::LocalBoxFuture;
use futures::{StreamExt, TryFutureExt};
use jsonrpc_v2::{FromRequest, Params, RequestObject};
use near_jsonrpc::RpcRequest;
use serde_json::Value;
use std::future::{ready, Ready};

pub struct RequestsCounters;

impl<S, B> Transform<S, ServiceRequest> for RequestsCounters
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = actix_web::Error> + 'static,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = actix_web::Error;
    type InitError = ();
    type Transform = RequestsCountersMiddleware<S>;
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

impl<'a, S, B> Service<ServiceRequest> for RequestsCountersMiddleware<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = actix_web::Error> + 'static,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = actix_web::Error;
    type Future = LocalBoxFuture<'static, Result<Self::Response, Self::Error>>;

    forward_ready!(service);

    fn call(&self, req: ServiceRequest) -> Self::Future {
        let ss = self.service.clone();
        TOTAL_REQUESTS_COUNTER.inc();
        Box::pin(async move {
            let (hreq, mut payload) = req.into_parts();
            let mut body = actix_web::web::BytesMut::new();
            while let Some(chunk) = &payload.next().await {
                match chunk {
                    Ok(chunk) => body.extend_from_slice(chunk),
                    Err(e) => {
                        tracing::error!("Error receiving payload: {:?}", e);
                    }
                };
            }

            if let Ok(obj) = serde_json::from_slice::<jsonrpc_v2::RequestObject>(&body) {
                let method = obj.method_ref();
                if method == "query" {
                    let params = match Params::from_request_object(&obj) {
                        Ok(Params(params)) => params,
                        Err(e) => {
                            tracing::error!("Error parsing request params: {:?}", e);
                            Value::default()
                        }
                    };
                    let query_request =
                        near_jsonrpc::primitives::types::query::RpcQueryRequest::parse(params).unwrap();
                    METHODS_CALLS_COUNTER.with_label_values(&[method]).inc()
                } else {
                    METHODS_CALLS_COUNTER.with_label_values(&[method]).inc()
                }
            };

            let mut request = actix_web::dev::ServiceRequest::from_request(hreq);
            let (_, mut payload) = actix_http::h1::Payload::create(true);
            payload.unread_data(body.into());
            request.set_payload(payload.into());
            ss.call(request).await
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IngestRequest {
    #[prost(message, repeated, tag = "1")]
    pub doc_batches: ::prost::alloc::vec::Vec<DocBatch>,
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IngestResponse {}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FetchRequest {
    #[prost(string, tag = "1")]
    pub index_id: ::prost::alloc::string::String,
    #[prost(uint64, optional, tag = "2")]
    pub start_after: ::core::option::Option<u64>,
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FetchResponse {
    #[prost(uint64, optional, tag = "1")]
    pub first_position: ::core::option::Option<u64>,
    #[prost(message, optional, tag = "2")]
    pub doc_batch: ::core::option::Option<DocBatch>,
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DocBatch {
    #[prost(string, tag = "1")]
    pub index_id: ::prost::alloc::string::String,
    #[prost(bytes = "vec", tag = "2")]
    pub concat_docs: ::prost::alloc::vec::Vec<u8>,
    #[prost(uint64, repeated, tag = "3")]
    pub doc_lens: ::prost::alloc::vec::Vec<u64>,
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SuggestTruncateRequest {
    #[prost(string, tag = "1")]
    pub index_id: ::prost::alloc::string::String,
    #[prost(uint64, tag = "2")]
    pub up_to_position_included: u64,
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SuggestTruncateResponse {}
#[doc = r" Generated client implementations."]
pub mod push_api_service_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[derive(Debug, Clone)]
    pub struct PushApiServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl PushApiServiceClient<tonic::transport::Channel> {
        #[doc = r" Attempt to create a new client by connecting to a given endpoint."]
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> PushApiServiceClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::ResponseBody: Body + Send + 'static,
        T::Error: Into<StdError>,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> PushApiServiceClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<http::Request<tonic::body::BoxBody>>>::Error:
                Into<StdError> + Send + Sync,
        {
            PushApiServiceClient::new(InterceptedService::new(inner, interceptor))
        }
        #[doc = r" Compress requests with `gzip`."]
        #[doc = r""]
        #[doc = r" This requires the server to support it otherwise it might respond with an"]
        #[doc = r" error."]
        pub fn send_gzip(mut self) -> Self {
            self.inner = self.inner.send_gzip();
            self
        }
        #[doc = r" Enable decompressing responses with `gzip`."]
        pub fn accept_gzip(mut self) -> Self {
            self.inner = self.inner.accept_gzip();
            self
        }
        #[doc = "/ Ingests document in a given queue."]
        #[doc = "/"]
        #[doc = "/ Upon any kind of error, the client should"]
        #[doc = "/ - retry to get at least once delivery."]
        #[doc = "/ - not retry to get at most once delivery."]
        #[doc = "/"]
        #[doc = "/ Exactly once delivery is not supported yet."]
        pub async fn ingest(
            &mut self,
            request: impl tonic::IntoRequest<super::IngestRequest>,
        ) -> Result<tonic::Response<super::IngestResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/quickwit_push_api.PushAPIService/Ingest");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = "/ Suggest to truncate the queue."]
        #[doc = "/"]
        #[doc = "/ This function allows the queue to remove all records up to and"]
        #[doc = "/ including `up_to_offset_included`."]
        #[doc = "/"]
        #[doc = "/ The role of this truncation is to release memory and disk space."]
        #[doc = "/"]
        #[doc = "/ There are no guarantees that the record will effectively be removed."]
        #[doc = "/ Nothing might happen, or the truncation might be partial."]
        #[doc = "/"]
        #[doc = "/ In other words, truncating from a position, and fetching records starting"]
        #[doc = "/ earlier than this position can yield undefined result:"]
        #[doc = "/ the truncated records may or may not be returned."]
        pub async fn suggest_truncate(
            &mut self,
            request: impl tonic::IntoRequest<super::SuggestTruncateRequest>,
        ) -> Result<tonic::Response<super::SuggestTruncateResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/quickwit_push_api.PushAPIService/SuggestTruncate",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = "/ Fetches record from a given queue."]
        #[doc = "/"]
        #[doc = "/ Records are returned in order."]
        #[doc = "/"]
        #[doc = "/ The returned `FetchResponse` object is meant to be read with the"]
        #[doc = "/ `crate::iter_records` function."]
        #[doc = "/"]
        #[doc = "/ Fetching does not necessarily return all of the available records."]
        #[doc = "/ If returning all records would exceed `FETCH_PAYLOAD_LIMIT` (2MB),"]
        #[doc = "/ the reponse will be partial."]
        pub async fn fetch(
            &mut self,
            request: impl tonic::IntoRequest<super::FetchRequest>,
        ) -> Result<tonic::Response<super::FetchResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/quickwit_push_api.PushAPIService/Fetch");
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
}
#[doc = r" Generated server implementations."]
pub mod push_api_service_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[doc = "Generated trait containing gRPC methods that should be implemented for use with PushApiServiceServer."]
    #[async_trait]
    pub trait PushApiService: Send + Sync + 'static {
        #[doc = "/ Ingests document in a given queue."]
        #[doc = "/"]
        #[doc = "/ Upon any kind of error, the client should"]
        #[doc = "/ - retry to get at least once delivery."]
        #[doc = "/ - not retry to get at most once delivery."]
        #[doc = "/"]
        #[doc = "/ Exactly once delivery is not supported yet."]
        async fn ingest(
            &self,
            request: tonic::Request<super::IngestRequest>,
        ) -> Result<tonic::Response<super::IngestResponse>, tonic::Status>;
        #[doc = "/ Suggest to truncate the queue."]
        #[doc = "/"]
        #[doc = "/ This function allows the queue to remove all records up to and"]
        #[doc = "/ including `up_to_offset_included`."]
        #[doc = "/"]
        #[doc = "/ The role of this truncation is to release memory and disk space."]
        #[doc = "/"]
        #[doc = "/ There are no guarantees that the record will effectively be removed."]
        #[doc = "/ Nothing might happen, or the truncation might be partial."]
        #[doc = "/"]
        #[doc = "/ In other words, truncating from a position, and fetching records starting"]
        #[doc = "/ earlier than this position can yield undefined result:"]
        #[doc = "/ the truncated records may or may not be returned."]
        async fn suggest_truncate(
            &self,
            request: tonic::Request<super::SuggestTruncateRequest>,
        ) -> Result<tonic::Response<super::SuggestTruncateResponse>, tonic::Status>;
        #[doc = "/ Fetches record from a given queue."]
        #[doc = "/"]
        #[doc = "/ Records are returned in order."]
        #[doc = "/"]
        #[doc = "/ The returned `FetchResponse` object is meant to be read with the"]
        #[doc = "/ `crate::iter_records` function."]
        #[doc = "/"]
        #[doc = "/ Fetching does not necessarily return all of the available records."]
        #[doc = "/ If returning all records would exceed `FETCH_PAYLOAD_LIMIT` (2MB),"]
        #[doc = "/ the reponse will be partial."]
        async fn fetch(
            &self,
            request: tonic::Request<super::FetchRequest>,
        ) -> Result<tonic::Response<super::FetchResponse>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct PushApiServiceServer<T: PushApiService> {
        inner: _Inner<T>,
        accept_compression_encodings: (),
        send_compression_encodings: (),
    }
    struct _Inner<T>(Arc<T>);
    impl<T: PushApiService> PushApiServiceServer<T> {
        pub fn new(inner: T) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
            }
        }
        pub fn with_interceptor<F>(inner: T, interceptor: F) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for PushApiServiceServer<T>
    where
        T: PushApiService,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = Never;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/quickwit_push_api.PushAPIService/Ingest" => {
                    #[allow(non_camel_case_types)]
                    struct IngestSvc<T: PushApiService>(pub Arc<T>);
                    impl<T: PushApiService> tonic::server::UnaryService<super::IngestRequest> for IngestSvc<T> {
                        type Response = super::IngestResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::IngestRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).ingest(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = IngestSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/quickwit_push_api.PushAPIService/SuggestTruncate" => {
                    #[allow(non_camel_case_types)]
                    struct SuggestTruncateSvc<T: PushApiService>(pub Arc<T>);
                    impl<T: PushApiService>
                        tonic::server::UnaryService<super::SuggestTruncateRequest>
                        for SuggestTruncateSvc<T>
                    {
                        type Response = super::SuggestTruncateResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::SuggestTruncateRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).suggest_truncate(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = SuggestTruncateSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/quickwit_push_api.PushAPIService/Fetch" => {
                    #[allow(non_camel_case_types)]
                    struct FetchSvc<T: PushApiService>(pub Arc<T>);
                    impl<T: PushApiService> tonic::server::UnaryService<super::FetchRequest> for FetchSvc<T> {
                        type Response = super::FetchResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::FetchRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).fetch(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = FetchSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => Box::pin(async move {
                    Ok(http::Response::builder()
                        .status(200)
                        .header("grpc-status", "12")
                        .header("content-type", "application/grpc")
                        .body(empty_body())
                        .unwrap())
                }),
            }
        }
    }
    impl<T: PushApiService> Clone for PushApiServiceServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: PushApiService> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: PushApiService> tonic::transport::NamedService for PushApiServiceServer<T> {
        const NAME: &'static str = "quickwit_push_api.PushAPIService";
    }
}

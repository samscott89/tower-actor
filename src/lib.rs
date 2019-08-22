//! Experiment to try applying actor-like message passing syntax to `tower`,
//! to allow passing arbitrary message types to a buffered `Service`.

use futures::prelude::*;
use tower_buffer::Buffer;
use tower_service::Service;

use std::any::Any;
use std::sync::{Arc, Mutex};

pub trait Message: Send + 'static {
    type Response: Send + 'static;
    type Error: Send + 'static;
}

type StdError = Box<dyn std::error::Error + Send + Sync + 'static>;
type AnyBox = Box<dyn Any + Send + 'static>;

/// Type-erased message for a particular service
pub struct Envelope<Svc> {
    _svc_marker: std::marker::PhantomData<Svc>,
    msg: AnyBox,
    f: fn(
        &mut Svc,
        AnyBox,
    ) -> Box<dyn Future<Item = AnyBox, Error = crate::StdError> + Send + 'static>,
}

unsafe impl<Svc> Send for Envelope<Svc> {}

/// Effectively a wrapper for `<Svc as Service<M>::call`, which
/// boxes the output and future.
fn box_service_call<Svc, M>(
    svc: &mut Svc,
    msg: AnyBox,
) -> Box<dyn Future<Item = AnyBox, Error = crate::StdError> + Send + 'static>
where
    Svc: Service<M>,
    Svc::Response: Send + 'static,
    Svc::Future: Send + 'static,
    Svc::Error: Into<crate::StdError> + 'static,
    M: 'static,
{
    let msg = msg.downcast().unwrap();
    let fut = <Svc as Service<M>>::call(svc, *msg);
    let fut = fut
        .map(|r| Box::new(r) as AnyBox)
        .map_err(std::convert::Into::into);
    Box::new(fut) as Box<dyn Future<Item = AnyBox, Error = crate::StdError> + Send + 'static>
}

impl<Svc> Envelope<Svc> {
    /// Create a new envelope from a message
    pub fn new<M>(msg: M) -> Self
    where
        Svc: Service<M>,
        Svc::Response: Send + 'static,
        Svc::Future: Send + 'static,
        Svc::Error: Into<crate::StdError> + 'static,
        M: Send + 'static,
    {
        Self {
            _svc_marker: std::marker::PhantomData,
            msg: Box::new(msg) as AnyBox,
            f: box_service_call::<Svc, M>,
        }
    }
}

/// A service capable of receiving enveloped message
pub struct Addr<Svc> {
    target: std::marker::PhantomData<Svc>,
    addr: Buffer<Actor<Svc>, Envelope<Svc>>,
    error: Arc<Mutex<Option<crate::StdError>>>,
}

impl<Svc> Addr<Svc>
where
    Svc: Send + 'static,
{
    pub fn new(service: Svc) -> Self {
        let buffer = Buffer::new(Actor(service), 1);
        Self {
            target: std::marker::PhantomData,
            addr: buffer,
            error: Default::default(),
        }
    }

    pub fn recipient<Response, Error>(&self) -> Recipient<Svc, Response, Error> {
        Recipient {
            addr: self.clone(),
            resp_marker: std::marker::PhantomData,
            err_marker: std::marker::PhantomData,
        }
    }
}

/// The type-refined version of `Addr` which expects
/// certain responses/errors
pub struct Recipient<Svc, Response, Error> {
    addr: Addr<Svc>,
    resp_marker: std::marker::PhantomData<Response>,
    err_marker: std::marker::PhantomData<Error>,
}

impl<Svc> Clone for Addr<Svc> {
    fn clone(&self) -> Self {
        Self {
            target: self.target,
            addr: self.addr.clone(),
            error: self.error.clone(),
        }
    }
}

impl<Request, Svc> Service<Request> for Addr<Svc>
where
    Request: Message + Send + 'static,
    Request::Response: 'static,
    Svc::Response: Send + 'static,
    Svc::Error: Into<crate::StdError> + 'static,
    Svc::Future: Send + 'static,

    Svc: Service<Request>,
    Actor<Svc>: Service<Envelope<Svc>, Response = AnyBox>,
    <Actor<Svc> as Service<Envelope<Svc>>>::Error: Into<crate::StdError>,
    <Actor<Svc> as Service<Envelope<Svc>>>::Future: Send + 'static,
    Request::Error: From<crate::StdError> + 'static,
{
    type Response = Box<<Request as Message>::Response>;
    type Error = <Request as Message>::Error;
    type Future = Box<dyn Future<Item = Self::Response, Error = Self::Error> + Send + 'static>;

    fn poll_ready(&mut self) -> futures::Poll<(), Self::Error> {
        Service::<Envelope<Svc>>::poll_ready(&mut self.addr)
            .map_err(<Request as Message>::Error::from)
    }

    fn call(&mut self, request: Request) -> Self::Future {
        let envelope = Envelope::new(request);
        let e = self.error.clone();
        Box::new(
            Service::<Envelope<Svc>>::call(&mut self.addr, envelope)
                .map(|b| b.downcast().unwrap())
                .map_err(move |_| {
                    e.lock()
                        .unwrap()
                        .take()
                        .expect("should always have a shared error when the future errors")
                })
                .map_err(<Request as Message>::Error::from),
        )
    }
}

impl<Request, Svc, Response, Error> Service<Request> for Recipient<Svc, Response, Error>
where
    Request: Send + 'static,
    Response: 'static,
    Svc::Response: Send + 'static,
    Svc::Error: Into<crate::StdError> + 'static,
    Svc::Future: Send + 'static,

    Svc: Service<Request>,
    Actor<Svc>: Service<Envelope<Svc>, Response = AnyBox>,
    <Actor<Svc> as Service<Envelope<Svc>>>::Error: Into<crate::StdError>,
    <Actor<Svc> as Service<Envelope<Svc>>>::Future: Send + 'static,
    Error: From<crate::StdError> + 'static,
{
    type Response = Box<Response>;
    type Error = Error;
    type Future = Box<dyn Future<Item = Self::Response, Error = Self::Error> + Send + 'static>;

    fn poll_ready(&mut self) -> futures::Poll<(), Self::Error> {
        Service::<Envelope<Svc>>::poll_ready(&mut self.addr.addr).map_err(Error::from)
    }

    fn call(&mut self, request: Request) -> Self::Future {
        let envelope = Envelope::new(request);
        let e = self.addr.error.clone();
        Box::new(
            Service::<Envelope<Svc>>::call(&mut self.addr.addr, envelope)
                .map(|b| b.downcast().unwrap())
                .map_err(move |_| {
                    e.lock()
                        .unwrap()
                        .take()
                        .expect("should always have a shared error when the future errors")
                })
                .map_err(Error::from),
        )
    }
}

/// The wrapper to handle messages
pub struct Actor<Svc>(Svc);

impl<Svc> Service<Envelope<Svc>> for Actor<Svc> {
    type Response = AnyBox;
    type Error = crate::StdError;
    type Future = Box<dyn Future<Item = Self::Response, Error = Self::Error> + Send + 'static>;

    fn poll_ready(&mut self) -> futures::Poll<(), Self::Error> {
        Ok(Async::Ready(()))
    }

    fn call(&mut self, request: Envelope<Svc>) -> Self::Future {
        (request.f)(&mut self.0, request.msg)
    }
}

#[cfg(test)]
mod tests {
    use tower::util::ServiceExt;

    use super::*;

    struct TestService;

    impl Service<String> for TestService {
        type Response = String;
        type Error = crate::StdError;
        type Future = Box<dyn Future<Item = Self::Response, Error = Self::Error> + Send + 'static>;

        fn poll_ready(&mut self) -> futures::Poll<(), Self::Error> {
            Ok(Async::Ready(()))
        }

        fn call(&mut self, request: String) -> Self::Future {
            Box::new(futures::future::ok(request + " -- response"))
        }
    }

    impl Service<u64> for TestService {
        type Response = u64;
        type Error = crate::StdError;
        type Future = Box<dyn Future<Item = Self::Response, Error = Self::Error> + Send + 'static>;

        fn poll_ready(&mut self) -> futures::Poll<(), Self::Error> {
            Ok(Async::Ready(()))
        }

        fn call(&mut self, request: u64) -> Self::Future {
            Box::new(futures::future::ok(request * 2))
        }
    }

    #[test]
    fn name() {
        let fut = futures::future::lazy(|| {
            let addr = Addr::new(TestService);
            let fut1 = ServiceExt::<String>::ready(addr.recipient())
                .and_then(|mut svc| svc.call("test".to_string()));
            let fut2 = ServiceExt::<u64>::ready(addr.recipient()).and_then(|mut svc| svc.call(12));

            fut1.join(fut2)
        })
        .map(|_: (Box<String>, Box<u64>)| ())
        .map_err(|e: crate::StdError| {
            panic!(e);
        });

        tokio::run(fut);
    }
}

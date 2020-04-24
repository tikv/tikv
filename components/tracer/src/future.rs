use std::pin::Pin;
use std::task::Context;

#[pin_project::pin_project]
pub struct Instrumented<T> {
    #[pin]
    pub inner: T,
    pub span: crate::OSpanGuard,
}

impl<T: Sized> Instrument for T {}

pub trait Instrument: Sized {
    fn instrument(self, span: crate::SpanGuard) -> Instrumented<Self> {
        Instrumented {
            inner: self,
            span: crate::OSpanGuard(Some(span)),
        }
    }

    fn in_current_span(self, tag: &'static str) -> Instrumented<Self> {
        Instrumented {
            inner: self,
            span: crate::new_span(tag),
        }
    }
}

impl<T: std::future::Future> std::future::Future for Instrumented<T> {
    type Output = T::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> std::task::Poll<Self::Output> {
        let this = self.project();
        let _enter = this.span.enter();
        this.inner.poll(cx)
    }
}

impl<T: futures_01::Future> futures_01::Future for Instrumented<T> {
    type Item = T::Item;
    type Error = T::Error;

    fn poll(&mut self) -> futures_01::Poll<Self::Item, Self::Error> {
        let _enter = self.span.enter();
        self.inner.poll()
    }
}

impl<T: futures_01::Stream> futures_01::Stream for Instrumented<T> {
    type Item = T::Item;
    type Error = T::Error;

    fn poll(&mut self) -> futures_01::Poll<Option<Self::Item>, Self::Error> {
        let _enter = self.span.enter();
        self.inner.poll()
    }
}

impl<T: futures_01::Sink> futures_01::Sink for Instrumented<T> {
    type SinkItem = T::SinkItem;
    type SinkError = T::SinkError;

    fn start_send(
        &mut self,
        item: Self::SinkItem,
    ) -> futures_01::StartSend<Self::SinkItem, Self::SinkError> {
        let _enter = self.span.enter();
        self.inner.start_send(item)
    }

    fn poll_complete(&mut self) -> futures_01::Poll<(), Self::SinkError> {
        let _enter = self.span.enter();
        self.inner.poll_complete()
    }
}

impl<T: futures_03::Stream> futures_03::Stream for Instrumented<T> {
    type Item = T::Item;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> futures_03::task::Poll<Option<Self::Item>> {
        let this = self.project();
        let _enter = this.span.enter();
        T::poll_next(this.inner, cx)
    }
}

impl<I, T: futures_03::Sink<I>> futures_03::Sink<I> for Instrumented<T>
where
    T: futures_03::Sink<I>,
{
    type Error = T::Error;

    fn poll_ready(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> futures_03::task::Poll<Result<(), Self::Error>> {
        let this = self.project();
        let _enter = this.span.enter();
        T::poll_ready(this.inner, cx)
    }

    fn start_send(self: Pin<&mut Self>, item: I) -> Result<(), Self::Error> {
        let this = self.project();
        let _enter = this.span.enter();
        T::start_send(this.inner, item)
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> futures_03::task::Poll<Result<(), Self::Error>> {
        let this = self.project();
        let _enter = this.span.enter();
        T::poll_flush(this.inner, cx)
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> futures_03::task::Poll<Result<(), Self::Error>> {
        let this = self.project();
        let _enter = this.span.enter();
        T::poll_close(this.inner, cx)
    }
}

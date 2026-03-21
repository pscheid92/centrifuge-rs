use std::sync::Arc;

use crate::protocol::types::*;

/// Type alias for event callback closures.
type Cb<T> = Option<Arc<dyn Fn(T) + Send + Sync + 'static>>;

/// Client-level event handlers.
#[derive(Default, Clone)]
pub struct ClientEventHandlers {
    pub on_connecting: Cb<ConnectingContext>,
    pub on_connected: Cb<ConnectedContext>,
    pub on_disconnected: Cb<DisconnectedContext>,
    pub on_error: Cb<ErrorContext>,
    pub on_message: Cb<MessageContext>,
    pub on_server_subscribed: Cb<ServerSubscribedContext>,
    pub on_server_subscribing: Cb<ServerSubscribingContext>,
    pub on_server_unsubscribed: Cb<ServerUnsubscribedContext>,
    pub on_server_publication: Cb<ServerPublicationContext>,
    pub on_server_join: Cb<ServerJoinContext>,
    pub on_server_leave: Cb<ServerLeaveContext>,
}

impl ClientEventHandlers {
    pub fn on_connecting(mut self, f: impl Fn(ConnectingContext) + Send + Sync + 'static) -> Self {
        self.on_connecting = Some(Arc::new(f));
        self
    }

    pub fn on_connected(mut self, f: impl Fn(ConnectedContext) + Send + Sync + 'static) -> Self {
        self.on_connected = Some(Arc::new(f));
        self
    }

    pub fn on_disconnected(
        mut self,
        f: impl Fn(DisconnectedContext) + Send + Sync + 'static,
    ) -> Self {
        self.on_disconnected = Some(Arc::new(f));
        self
    }

    pub fn on_error(mut self, f: impl Fn(ErrorContext) + Send + Sync + 'static) -> Self {
        self.on_error = Some(Arc::new(f));
        self
    }

    pub fn on_message(mut self, f: impl Fn(MessageContext) + Send + Sync + 'static) -> Self {
        self.on_message = Some(Arc::new(f));
        self
    }

    pub fn on_server_subscribed(
        mut self,
        f: impl Fn(ServerSubscribedContext) + Send + Sync + 'static,
    ) -> Self {
        self.on_server_subscribed = Some(Arc::new(f));
        self
    }

    pub fn on_server_subscribing(
        mut self,
        f: impl Fn(ServerSubscribingContext) + Send + Sync + 'static,
    ) -> Self {
        self.on_server_subscribing = Some(Arc::new(f));
        self
    }

    pub fn on_server_unsubscribed(
        mut self,
        f: impl Fn(ServerUnsubscribedContext) + Send + Sync + 'static,
    ) -> Self {
        self.on_server_unsubscribed = Some(Arc::new(f));
        self
    }

    pub fn on_server_publication(
        mut self,
        f: impl Fn(ServerPublicationContext) + Send + Sync + 'static,
    ) -> Self {
        self.on_server_publication = Some(Arc::new(f));
        self
    }

    pub fn on_server_join(
        mut self,
        f: impl Fn(ServerJoinContext) + Send + Sync + 'static,
    ) -> Self {
        self.on_server_join = Some(Arc::new(f));
        self
    }

    pub fn on_server_leave(
        mut self,
        f: impl Fn(ServerLeaveContext) + Send + Sync + 'static,
    ) -> Self {
        self.on_server_leave = Some(Arc::new(f));
        self
    }
}

/// Subscription-level event handlers.
#[derive(Default, Clone)]
pub struct SubscriptionEventHandlers {
    pub on_subscribing: Cb<SubscribingContext>,
    pub on_subscribed: Cb<SubscribedContext>,
    pub on_unsubscribed: Cb<UnsubscribedContext>,
    pub on_publication: Cb<PublicationContext>,
    pub on_join: Cb<JoinContext>,
    pub on_leave: Cb<LeaveContext>,
    pub on_error: Cb<ErrorContext>,
}

impl SubscriptionEventHandlers {
    pub fn on_subscribing(
        mut self,
        f: impl Fn(SubscribingContext) + Send + Sync + 'static,
    ) -> Self {
        self.on_subscribing = Some(Arc::new(f));
        self
    }

    pub fn on_subscribed(mut self, f: impl Fn(SubscribedContext) + Send + Sync + 'static) -> Self {
        self.on_subscribed = Some(Arc::new(f));
        self
    }

    pub fn on_unsubscribed(
        mut self,
        f: impl Fn(UnsubscribedContext) + Send + Sync + 'static,
    ) -> Self {
        self.on_unsubscribed = Some(Arc::new(f));
        self
    }

    pub fn on_publication(
        mut self,
        f: impl Fn(PublicationContext) + Send + Sync + 'static,
    ) -> Self {
        self.on_publication = Some(Arc::new(f));
        self
    }

    pub fn on_join(mut self, f: impl Fn(JoinContext) + Send + Sync + 'static) -> Self {
        self.on_join = Some(Arc::new(f));
        self
    }

    pub fn on_leave(mut self, f: impl Fn(LeaveContext) + Send + Sync + 'static) -> Self {
        self.on_leave = Some(Arc::new(f));
        self
    }

    pub fn on_error(mut self, f: impl Fn(ErrorContext) + Send + Sync + 'static) -> Self {
        self.on_error = Some(Arc::new(f));
        self
    }
}

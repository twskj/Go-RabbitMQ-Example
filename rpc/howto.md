How to setup RPC Message Queue

1. Set up a call back queue for each client connected
2. Client connect to the work queue (RPC Queue)
3. Server pick up work from the work queue and read reply address from field `ReplyTo`
4. Each RPC request will attach an ID in `CorrelationId` field to match request-response
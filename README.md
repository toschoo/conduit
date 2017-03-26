# Package conduit

Package conduit provides abstractions over computation chains
similar to classic coroutines and inspired mostly by Haskell's
conduit library.

Its main abstractions are chains consisting of a data producer,
a data consumer and, potentially, a pipe of conduits transforming
or filtering data on their way down the processing chain.

Each component of a chain, producer, consumer and each conduit,
is running in its own goroutine receiving and forwarding
data through a channel. Chains are therefore not only a way
for separating of concerns in code design, but also a way to speed up
processing exploiting multicore architectures.


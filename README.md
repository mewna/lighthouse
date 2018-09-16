# lighthouse

(Kubernetes is already full of ship puns, so I figured I could continue
the trend with this name.)

lighthouse helps you distributing your Java Discord bots, built with
kubernetes in mind. lighthouse is built on top of
[vert.x](https://vertx.io), and is fully async.

## How do it do?

lighthouse is built for the one-shard-container model, mainly for making
it slightly easier to implement. As each lighthouse instance creates its
own vert.x instance, it is HIGHLY recommended that you not run multiple
lighthouse instances in a single JVM.

Consul is used for distributed locking and service management. It allows
for doing locking, service discovery (for pubsub), ... with minimal
effort, as well as not having to implement these basic concurrency
primitives on top of eg. Redis. Also I just like Consul more than etcd
or Zookeeper.

To use, simply spin up a number `N` of workers where
`N = desired shard count` (ex. want 4 shards = create 4 workers). Once
all workers are started and can communicate, lighthouse will begin
sharding for you.

Since lighthouse is meant to be used with kubernetes, if sharding goes
**boom** and fails massively (ie. future failures), you should just exit
the process and let kubernetes restart it. If, for whatever reason, you
don't want to do this, you have to implement the full-retry logic
yourself. I'm open to PRs to solve this, but will not be implementing it
at this time.

**WARNING**

Due to how Consul services work, it may take a little bit for a reshard
to fully work, and you may end up with a little more downtime (probably
~5m at most) than you might expect!

## Installation

Install it to your maven local somehow, idk

I'll put a jitpack link here eventually

## Usage

lighthouse uses [Consul](https://consul.io) (for distributed locking and
coordination) and [Redis](https://redis.io) (for pubsub). You need
working instances of both of these to use lighthouse.

Sample use:
```Java
public class DistributedShard {
    private Lighthouse lighthouse;
    private final Logger logger = LoggerFactory.getLogger(getClass());

    public static void main(final String[] args) {
        new DistributedShard().start();
    }

    private void start() {
        final int shardCount = 4;
        final String consulHost = System.getenv("CONSUL_HOST");
        final String redisHost = System.getenv("REDIS_HOST");
        final String redisAuth = System.getenv("REDIS_AUTH");
        // Used for consul health checks
        // In production, this should probably just be port 80
        final int healthPort = 9000 + new Random().nextInt(1000);
        logger.info("Running healthcheck on port {}...", healthPort);
        lighthouse = Lighthouse.lighthouse(shardCount, consulHost, healthPort, redisHost, redisAuth,
                this::handleSharding, this::handlePubsub);
        lighthouse.init().setHandler(res -> {
            if(res.succeeded()) {
                logger.info("Started lighthouse!");
                lighthouse.startShard().setHandler(shardRes -> {
                    if(shardRes.succeeded()) {
                        logger.info("Fully booted!");
                    } else {
                        logger.error("Couldn't start shard!", res.cause());
                    }
                });
            } else {
                logger.error("Couldn't start lighthouse!", res.cause());
            }
        });
    }

    // Called when the shard is to be started
    // Arguments are the shard id and the shard count
    // Return `true` if the shard resumed, return `false` otherwise
    // If you don't get it, just return false always
    private boolean handleSharding(final int id, final int limit) {
        logger.info("Booted shard {} / {}", id, limit);
        return false;
    }

    // Handles non-internal pubsub messages
    // Internal pubsub messages have a `__lighthouse:type` field,
    // which your messages should NOT have.
    private JsonObject handlePubsub(final JsonObject payload) {
        return new JsonObject();
    }
}
```
// SPDX-License-Identifier: Apache-2.0
package org.hiero.otter.test.performance.benchmark.fixtures;

import com.swirlds.common.utility.InstantUtils;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.hiero.otter.fixtures.Network;
import org.hiero.otter.fixtures.Node;
import org.hiero.otter.fixtures.TestEnvironment;

/**
 * Utility class that submits transactions to network nodes with rate limiting and even distribution.
 * <p>
 * <ul>
 *   <li>Uses a transaction factory to create transactions on demand</li>
 *   <li>Distributes transactions evenly across all active nodes</li>
 *   <li>Rate limits submissions to achieve a target rate (transactions per second)</li>
 * </ul>
 */
public class LoadThrottler {

    private final Network network;

    /**
     * Creates a new LoadThrottler.
     *
     * @param environment the environment containing nodes to submit transactions to
     *
     */
    public LoadThrottler(@NonNull final TestEnvironment environment) {
        this.network = Objects.requireNonNull(environment).network();
    }

    /**
     * Submits the specified number of transactions using the configured factory.
     *
     * <p>This is a blocking method that:
     * <ol>
     *   <li>Creates transactions using the factory</li>
     *   <li>Distributes them evenly across nodes (1 turn per node among active nodes in the network)</li>
     *   <li>Rate limits to achieve target rate</li>
     * </ol>
     *
     * @param length the length of time to submit transactions for
     * @param maxTransactionsPerSecond the maximum rate in seconds to send transactions to the network
     * @throws IllegalArgumentException if count or maxTransactionsPerSecond are less than zero or
     *  there are non-active nodes
     */
    public int submitWithRate(final Duration length, final int maxTransactionsPerSecond, final Consumer<Node> submitter) {
        if (!length.isPositive()) {
            throw new IllegalArgumentException("length must be positive, got: " + length);
        }
        if (maxTransactionsPerSecond <= 0) {
            throw new IllegalArgumentException(
                    "maxTransactionsPerSecond must be positive, got: " + maxTransactionsPerSecond);
        }

        final long intervalNanos =
                InstantUtils.NANOS_IN_MICRO * InstantUtils.MICROS_IN_SECOND / maxTransactionsPerSecond;
        final long startNanos = System.nanoTime();
        final List<Node> candidates =
                network.nodes().stream().filter(Node::isActive).toList();
        if (candidates.isEmpty()) {
            throw new IllegalArgumentException("No active nodes available in the network");
        }
        final Instant endTime = Instant.now().plus(length);
        int numSubmitted = 0;
        while (endTime.isAfter(Instant.now())) {
            // Select node with even distribution
            final Node targetNode = candidates.get(numSubmitted % candidates.size());

            // Submit transaction and track count
            submitter.accept(targetNode);

            // Rate limit to achieve target rate (compensating for work time)
            final long expectedNanos = (numSubmitted + 1) * intervalNanos;
            final long elapsedNanos = System.nanoTime() - startNanos;
            final long waitTime = expectedNanos - elapsedNanos;
            if (waitTime > 0) {
                try {
                    TimeUnit.NANOSECONDS.sleep(waitTime);
                } catch (final InterruptedException e) {
                    throw new AssertionError(e);
                }
            }
            numSubmitted++;
        }
        return numSubmitted;
    }
}

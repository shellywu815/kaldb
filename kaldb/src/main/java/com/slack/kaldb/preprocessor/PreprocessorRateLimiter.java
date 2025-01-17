package com.slack.kaldb.preprocessor;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.RateLimiter;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.kafka.streams.kstream.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The PreprocessorRateLimiter provides a thread-safe Kafka streams predicate for determining if a
 * Kafka message should be filtered or not. Metrics are provided for visibility when a rate limiter
 * is dropping messages.
 */
@ThreadSafe
@SuppressWarnings("UnstableApiUsage")
public class PreprocessorRateLimiter {
  private static final Logger LOG = LoggerFactory.getLogger(PreprocessorRateLimiter.class);

  private final MeterRegistry meterRegistry;
  private final int preprocessorCount;

  private final int maxBurstSeconds;

  private final boolean initializeWarm;

  public static final String MESSAGES_DROPPED = "preprocessor_rate_limit_messages_dropped";
  public static final String BYTES_DROPPED = "preprocessor_rate_limit_bytes_dropped";

  public enum MessageDropReason {
    MISSING_SERVICE_NAME,
    NOT_PROVISIONED,
    OVER_LIMIT
  }

  public PreprocessorRateLimiter(
      final MeterRegistry meterRegistry,
      final int preprocessorCount,
      final int maxBurstSeconds,
      final boolean initializeWarm) {
    Preconditions.checkArgument(preprocessorCount > 0, "Preprocessor count must be greater than 0");
    Preconditions.checkArgument(
        maxBurstSeconds >= 1, "Preprocessor maxBurstSeconds must be greater than or equal to 1");

    this.meterRegistry = meterRegistry;
    this.preprocessorCount = preprocessorCount;
    this.maxBurstSeconds = maxBurstSeconds;
    this.initializeWarm = initializeWarm;
  }

  /**
   * Creates a burstable rate limiter based on Guava rate limiting. This is supported by Guava, but
   * isn't exposed due to some philosophical arguments about needing a major refactor first -
   * https://github.com/google/guava/issues/1707.
   *
   * @param permitsPerSecond how many permits to grant per second - will require warmup period
   * @param maxBurstSeconds how many seconds permits can be accumulated - default guava value is 1s
   * @param initializeWarm if stored permits are initialized to the max value that can be
   *     accumulated
   */
  protected static RateLimiter smoothBurstyRateLimiter(
      double permitsPerSecond, double maxBurstSeconds, boolean initializeWarm) {
    try {
      Class<?> sleepingStopwatchClass =
          Class.forName("com.google.common.util.concurrent.RateLimiter$SleepingStopwatch");
      Method createFromSystemTimerMethod =
          sleepingStopwatchClass.getDeclaredMethod("createFromSystemTimer");
      createFromSystemTimerMethod.setAccessible(true);
      Object stopwatch = createFromSystemTimerMethod.invoke(null);

      Class<?> burstyRateLimiterClass =
          Class.forName("com.google.common.util.concurrent.SmoothRateLimiter$SmoothBursty");
      Constructor<?> burstyRateLimiterConstructor =
          burstyRateLimiterClass.getDeclaredConstructors()[0];
      burstyRateLimiterConstructor.setAccessible(true);

      RateLimiter result =
          (RateLimiter) burstyRateLimiterConstructor.newInstance(stopwatch, maxBurstSeconds);
      result.setRate(permitsPerSecond);

      if (initializeWarm) {
        Field storedPermitsField =
            result.getClass().getSuperclass().getDeclaredField("storedPermits");
        storedPermitsField.setAccessible(true);
        storedPermitsField.set(result, permitsPerSecond * maxBurstSeconds);
      }

      return result;
    } catch (Exception e) {
      LOG.error(
          "Error creating smooth bursty rate limiter, defaulting to non-bursty rate limiter", e);
      return RateLimiter.create(permitsPerSecond);
    }
  }

  public Predicate<String, Trace.Span> createRateLimiter(
      Map<String, Long> serviceNameToThroughput) {
    Map<String, RateLimiter> rateLimiterMap =
        serviceNameToThroughput
            .entrySet()
            .stream()
            .collect(
                Collectors.toMap(
                    (Map.Entry::getKey),
                    (entry -> {
                      double permitsPerSecond = (double) entry.getValue() / preprocessorCount;
                      LOG.info(
                          "Rate limiter initialized for {} at {} bytes per second (target throughput {} / processorCount {})",
                          entry.getKey(),
                          permitsPerSecond,
                          entry.getValue(),
                          preprocessorCount);
                      return smoothBurstyRateLimiter(
                          permitsPerSecond, maxBurstSeconds, initializeWarm);
                    })));

    return (key, value) -> {
      if (value == null) {
        LOG.warn("Message was dropped, was null span");
        return false;
      }

      String serviceName = PreprocessorValueMapper.getServiceName(value);
      int bytes = value.getSerializedSize();
      if (serviceName == null || serviceName.isEmpty()) {
        // service name wasn't provided
        LOG.debug("Message was dropped due to missing service name - '{}'", value);
        // todo - we may consider adding a logging BurstFilter so that a bad actor cannot
        //  inadvertently swamp the system if we want to increase this logging level
        //  https://logging.apache.org/log4j/2.x/manual/filters.html#BurstFilter
        meterRegistry
            .counter(MESSAGES_DROPPED, getMeterTags("", MessageDropReason.MISSING_SERVICE_NAME))
            .increment();
        meterRegistry
            .counter(BYTES_DROPPED, getMeterTags("", MessageDropReason.MISSING_SERVICE_NAME))
            .increment(bytes);
        return false;
      }

      if (!rateLimiterMap.containsKey(serviceName)) {
        // service isn't provisioned in our rate limit map
        meterRegistry
            .counter(MESSAGES_DROPPED, getMeterTags(serviceName, MessageDropReason.NOT_PROVISIONED))
            .increment();
        meterRegistry
            .counter(BYTES_DROPPED, getMeterTags(serviceName, MessageDropReason.NOT_PROVISIONED))
            .increment(bytes);
        LOG.debug(
            "Message was dropped from service '{}' as it not currently provisioned", serviceName);
        return false;
      }

      if (rateLimiterMap.get(serviceName).tryAcquire(bytes)) {
        return true;
      }

      // message should be dropped due to rate limit
      meterRegistry
          .counter(MESSAGES_DROPPED, getMeterTags(serviceName, MessageDropReason.OVER_LIMIT))
          .increment();
      meterRegistry
          .counter(BYTES_DROPPED, getMeterTags(serviceName, MessageDropReason.OVER_LIMIT))
          .increment(bytes);
      LOG.debug(
          "Message was dropped from service '{}' due to rate limiting ({} bytes per second), wanted {} bytes",
          serviceName,
          rateLimiterMap.get(serviceName).getRate(),
          serviceName);
      return false;
    };
  }

  private static List<Tag> getMeterTags(String serviceName, MessageDropReason reason) {
    return List.of(Tag.of("service", serviceName), Tag.of("reason", reason.toString()));
  }
}

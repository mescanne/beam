/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.extensions.timeseries.fs;

import com.google.auto.value.AutoValue;
import java.util.Iterator;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.OrderedListState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerMap;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.Min;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base transform that will accept raw input of data in the general format of (sequence, data) where
 * sequence is a monotonically increasing value. Data will include an id that identifies an
 * instrument.
 *
 * <p>TODO Add start seqnum logic, right now min(first window) becomes start seqnum TODO Objects for
 * the G
 */
@AutoValue
@Experimental
@SuppressWarnings({"nullness", "TypeNameShadowing"})
public abstract class TickerStream<V extends OrderBook>
    extends PTransform<PCollection<Tick>, PCollection<V>> {

  //  private final static Logger LOG = LoggerFactory.getLogger(TickerStream.class);

  public abstract Mode getMode();

  public abstract Class<V> getClazz();

  public static <V extends OrderBook> TickerStream<V> create(Mode mode, Class<V> clazz) {
    return new AutoValue_TickerStream.Builder<V>().setMode(mode).setClazz(clazz).build();
  }

  @AutoValue.Builder
  public abstract static class Builder<V extends OrderBook> {
    public abstract Builder<V> setMode(Mode value);

    public abstract Builder<V> setClazz(Class<V> value);

    public abstract TickerStream<V> build();
  }

  static final Duration BATCH_DURATION = Duration.standardSeconds(5);

  static final String SIDE_INPUT_NAME = "GlobalSeqWM";

  /** Storing window strategy of incoming stream, to allow reapplication post transform. */
  @Nullable private WindowingStrategy<?, ?> incomingWindowStrategy = null;

  public @Nullable WindowingStrategy<?, ?> getIncomingWindowStrategy() {
    return this.incomingWindowStrategy;
  }

  public void setIncomingWindowStrategy(WindowingStrategy<?, ?> value) {
    this.incomingWindowStrategy = value;
  }

  @Override
  public PCollection<V> expand(PCollection<Tick> input) {

    setIncomingWindowStrategy(input.getWindowingStrategy());

    // TODO Reduce byte[] to the single thread
    // This step will push all values to a single thread for processing of the GlobalTicks

    // Strip out extra bytes
    //    PCollection<Row> rows = input.apply(Convert.toRows());

    PCollectionView<Iterable<KV<Instant, Long>>> i =
        input
            .apply(
                "Win1",
                Window.<Tick>into(new GlobalWindows())
                    .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()))
                    .withAllowedLateness(Duration.ZERO)
                    .discardingFiredPanes())
            .apply(
                "MapToKV<1,Tick>",
                MapElements.into(
                        TypeDescriptors.kvs(TypeDescriptors.integers(), TypeDescriptors.longs()))
                    .via(x -> KV.of(1, x.getGlobalSequence())))
            .apply(
                ParDo.of(
                    GlobalSequenceTracker.builder()
                        .setBatchSize(TickerStream.BATCH_DURATION)
                        .setEstimateFirstSeqNumSize(0L)
                        .build()))
            .apply(View.asIterable());

    Coder<V> ob = null;

    try {
      ob = input.getPipeline().getCoderRegistry().getCoder(getClazz());
    } catch (CannotProvideCoderException e) {
      throw new RuntimeException(e);
    }

    return input
        .apply(
            "MapToKV<String,Tick>",
            MapElements.into(
                    TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptor.of(Tick.class)))
                .via(x -> KV.of(x.id, x)))
        .apply(ParDo.of(new PerSymbolSequencer<V>(getClazz(), ob)).withSideInput("GlobalSeqWM", i))
        .setCoder(ob);
  }

  /** TODO Work with AutoValue and StateSpec where Coder is passed in. */
  public static class PerSymbolSequencer<T extends OrderBook> extends DoFn<KV<String, Tick>, T> {

    private static final Logger LOG = LoggerFactory.getLogger(PerSymbolSequencer.class);

    private final Class<T> clazz;

    PerSymbolSequencer(Class<T> clazz, Coder<T> stateStateSpec) {
      orderBook = StateSpecs.value(stateStateSpec);
      this.clazz = clazz;
      assert this.clazz != null;
    }

    @DoFn.StateId("buffer")
    @SuppressWarnings("unused")
    private final StateSpec<OrderedListState<Tick>> bufferedEvents =
        StateSpecs.orderedList(SerializableCoder.of(Tick.class));

    @DoFn.StateId("releaseSignals")
    @SuppressWarnings("unused")
    private final StateSpec<MapState<Instant, Long>> releaseSignals =
        StateSpecs.map(InstantCoder.of(), VarLongCoder.of());

    @SuppressWarnings("unused")
    @DoFn.StateId("orderbook")
    private final StateSpec<ValueState<T>> orderBook;

    @TimerFamily("mutateOrderBook")
    @SuppressWarnings("unused")
    private final TimerSpec expirySpec = TimerSpecs.timerMap(TimeDomain.EVENT_TIME);

    @ProcessElement
    public void process(
        @SideInput("GlobalSeqWM") Iterable<KV<Instant, Long>> maxSeqRelease,
        @StateId("releaseSignals") MapState<Instant, Long> releaseSignals,
        @StateId("buffer") OrderedListState<Tick> buffer,
        @TimerFamily("mutateOrderBook") TimerMap expiryTimers,
        @Timestamp Instant instant,
        @Element KV<String, Tick> tick) {

      long sequence = tick.getValue().getGlobalSequence();

      // Add elements to OrderedList, we do not do any processing @Process only in the EventTimer
      buffer.add(TimestampedValue.of(tick.getValue(), Instant.ofEpochMilli(sequence)));
      Iterator<KV<Instant, Long>> i = maxSeqRelease.iterator();
      while (i.hasNext()) {
        KV<Instant, Long> k = i.next();
        releaseSignals.put(k.getKey(), k.getValue());
        // Set timer for Release time
        expiryTimers.set(String.valueOf(k.getKey().getMillis()), k.getKey());
        LOG.info("Setting Release Timers in key {} to {}", tick.getKey(), k.getKey());
      }
    }

    @OnTimerFamily("mutateOrderBook")
    public void onExpiry(
        OutputReceiver<T> context,
        OnTimerContext timerContext,
        @StateId("releaseSignals") MapState<Instant, Long> releaseSignals,
        @StateId("orderbook") ValueState<T> orderBookState,
        @StateId("buffer") OrderedListState<Tick> buffer) {

      // TODO making assumption that the sequence starts from a positive number that is increasing
      // -1 is magic number which indicates this is the first ever value
      long releaseSignal =
          Optional.ofNullable(releaseSignals.get(timerContext.timestamp()).read()).orElse(0L);

      LOG.info(
          "OnTimer Time {} release {} batch {}",
          timerContext.timestamp(),
          releaseSignal,
          buffer.isEmpty().read());

      if (!buffer.isEmpty().read()) {
        Iterable<TimestampedValue<Tick>> batch =
            buffer.readRange(Instant.EPOCH, Instant.ofEpochMilli(releaseSignal));

        Iterator<TimestampedValue<Tick>> batchItr = batch.iterator();
        T orderBook;

        try {
          orderBook =
              Optional.ofNullable(orderBookState.read())
                  .orElse(clazz.getDeclaredConstructor().newInstance());
        } catch (Exception e) {
          throw new RuntimeException(e);
        }

        int count = 0;
        while (batchItr.hasNext()) {
          Tick tick = batchItr.next().getValue();
          orderBook.add(tick.getOrder());
          count++;
        }

        if (count > 0) {
          LOG.info("Outputting Order Book {} {}", timerContext.timestamp(), orderBook.getAsks());
          context.outputWithTimestamp(orderBook, timerContext.timestamp());
          orderBookState.write(orderBook);
        }

        buffer.clearRange(Instant.EPOCH, Instant.ofEpochMilli(releaseSignal));
        // Clear TimerMapValue
        releaseSignals.remove(timerContext.timestamp());
      }
    }
  }

  @AutoValue
  public abstract static class GlobalSequenceTracker
      extends DoFn<KV<Integer, Long>, KV<Instant, Long>> {

    private static final Logger LOG = LoggerFactory.getLogger(GlobalSequenceTracker.class);

    public abstract Duration getBatchSize();

    public abstract Long getEstimateFirstSeqNumSize();

    public static GlobalSequenceTracker.Builder builder() {
      return new AutoValue_TickerStream_GlobalSequenceTracker.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setBatchSize(Duration value);

      public abstract Builder setEstimateFirstSeqNumSize(Long value);

      public abstract GlobalSequenceTracker build();
    }

    @DoFn.StateId("buffer")
    @SuppressWarnings("unused")
    private final StateSpec<OrderedListState<TimestampedValue<Long>>> bufferedEvents =
        StateSpecs.orderedList(TimestampedValue.TimestampedValueCoder.of(VarLongCoder.of()));

    @DoFn.StateId("timerSet")
    @SuppressWarnings("unused")
    private final StateSpec<ValueState<Boolean>> timerState = StateSpecs.value(BooleanCoder.of());

    @StateId("internWaterMark")
    @SuppressWarnings("unused")
    private final StateSpec<ValueState<Long>> internalWaterMark =
        StateSpecs.value(VarLongCoder.of());

    @StateId("maxSequence")
    @SuppressWarnings("unused")
    private final StateSpec<CombiningState<Long, long[], Long>> maxSequence =
        StateSpecs.combining(Max.ofLongs());

    @StateId("minSequence")
    @SuppressWarnings("unused")
    private final StateSpec<CombiningState<Long, long[], Long>> minSequence =
        StateSpecs.combining(Min.ofLongs());

    @TimerId("expiry")
    @SuppressWarnings("unused")
    private final TimerSpec expirySpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @ProcessElement
    public void process(
        @StateId("buffer") OrderedListState<TimestampedValue<Long>> buffer,
        @StateId("timerSet") ValueState<Boolean> isTimerSet,
        @StateId("maxSequence") CombiningState<Long, long[], Long> maxSequence,
        @StateId("minSequence") CombiningState<Long, long[], Long> minSequence,
        @TimerId("expiry") Timer expiryTimer,
        @Timestamp Instant instant,
        @Element KV<Integer, Long> globalSeq) {

      long sequence = globalSeq.getValue();
      maxSequence.add(sequence);
      minSequence.add(sequence);

      // Add elements to OrderedList, we do not do any processing @Process only in the EventTimer
      buffer.add(
          TimestampedValue.of(
              TimestampedValue.of(globalSeq.getValue(), instant), Instant.ofEpochMilli(sequence)));

      if (!Optional.ofNullable(isTimerSet.read()).orElse(false)) {
        expiryTimer
            .withOutputTimestamp(instant.plus(getBatchSize()))
            .set(instant.plus(getBatchSize()));
        isTimerSet.write(true);
        LOG.info("Setting Timers to {}", instant.plus(getBatchSize()));
      }
    }

    @OnTimer("expiry")
    public void sequenceRelease(
        OutputReceiver<KV<Instant, Long>> context,
        OnTimerContext timerContext,
        @StateId("internWaterMark") ValueState<Long> internalWaterMarkState,
        @StateId("buffer") OrderedListState<TimestampedValue<Long>> buffer,
        @StateId("maxSequence") CombiningState<Long, long[], Long> maxSequence,
        @StateId("minSequence") CombiningState<Long, long[], Long> minSequence,
        @StateId("timerSet") ValueState<Boolean> isTimerSet,
        @TimerId("expiry") Timer expiryTimer) {

      // TODO making assumption that the sequence starts from a positive number that is increasing
      // -1 is magic number which indicates this is the first ever value
      long internalWaterMark = Optional.ofNullable(internalWaterMarkState.read()).orElse(-1L);
      Instant endRead = Instant.ofEpochMilli(internalWaterMark + getBatchSize().getMillis());
      // If internalWaterMark is -1 then we are in startup mode
      if (internalWaterMark == -1) {
        // We have to page through the list buffer, as we are mapping globalsequence onto timestamp,
        // find
        // min value
        Instant readSize =
            Instant.ofEpochMilli(
                Optional.ofNullable(minSequence.read()).orElse(0L) + getBatchSize().getMillis());
        endRead = readSize.isAfter(endRead) ? readSize : endRead;
      }
      minSequence.clear();

      if (!buffer.isEmpty().read()) {

        Iterable<TimestampedValue<TimestampedValue<Long>>> batch =
            buffer.readRange(
                Instant.ofEpochMilli(internalWaterMark == -1 ? 0 : internalWaterMark), endRead);

        Iterator<TimestampedValue<TimestampedValue<Long>>> batchItr = batch.iterator();

        boolean npGapDetected = true;
        long lastSequence = internalWaterMark;

        while (npGapDetected && batchItr.hasNext()) {

          // Recall timestamp here is just a proxy for sequence number
          TimestampedValue<TimestampedValue<Long>> current = batchItr.next();
          long nextSequence = current.getTimestamp().getMillis();

          //                    LOG.info("next {}, last {} ", nextSequence, lastSequence);

          if ((current.getValue().getTimestamp().isBefore(timerContext.timestamp()))
              && (lastSequence == -1 || (nextSequence - lastSequence) == 1)) {
            lastSequence = nextSequence;

          } else {
            npGapDetected = false;
          }
        }

        if (lastSequence != internalWaterMark) {
          context.output(KV.of(timerContext.timestamp(), lastSequence + 1));
        }

        buffer.clearRange(
            Instant.ofEpochMilli(internalWaterMark == -1 ? 0 : internalWaterMark),
            Instant.ofEpochMilli(lastSequence + 1));

        // Update our internal watermark to be the last good sequence
        internalWaterMarkState.write(lastSequence);

        // Are we at the end of our BagState

        if (buffer.isEmpty().read().booleanValue()
            && (lastSequence == maxSequence.read().longValue())) {
          // Then we unset the timerIsSet Value
          isTimerSet.write(false);
        } else {
          // We need to set a timer for 5 sec from now
          expiryTimer
              .withOutputTimestamp(timerContext.timestamp().plus(getBatchSize()))
              .set(timerContext.timestamp().plus(getBatchSize()));
          //          LOG.info("Setting loop Timers to {}",
          // timerContext.timestamp().plus(getBatchSize()));
        }
      }
    }
  }

  enum Mode {
    SINGLE_STREAM,
    MULTIPLEX_STREAM
  }
}

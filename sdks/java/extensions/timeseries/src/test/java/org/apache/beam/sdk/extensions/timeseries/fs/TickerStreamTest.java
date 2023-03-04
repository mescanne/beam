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

import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reify;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.LoggerFactory;

@RunWith(JUnit4.class)
public class TickerStreamTest {

  public static final Instant START = Instant.parse("2000-01-01T00:00");

  @Rule public final transient TestPipeline p = TestPipeline.create();

  @Test
  public void basicTest() {

    TestStream.Builder<Long> stream =
        TestStream.create(VarLongCoder.of()).advanceWatermarkTo(START);

    for (long i = 0; i < 10; i++) {
      stream = stream.addElements(i).advanceWatermarkTo(START.plus(Duration.standardSeconds(i)));
    }

    PCollection<KV<Integer, Long>> ticks =
        p.apply(stream.advanceWatermarkToInfinity()).apply(WithKeys.of(1));

    PCollection<KV<Instant, Long>> output =
        ticks.apply(
            ParDo.of(
                TickerStream.GlobalSequenceTracker.builder()
                    .setBatchSize(Duration.standardSeconds(5))
                    .setEstimateFirstSeqNumSize(0L)
                    .build()));

    PAssert.that(output)
        .containsInAnyOrder(
            KV.of(START.plus(Duration.standardSeconds(5)), 6L),
            KV.of(START.plus(Duration.standardSeconds(10)), 10L));

    p.run();
  }

  @Test
  public void lateData() throws NoSuchSchemaException {

    TestStream<KV<Integer, Long>> stream =
        TestStream.create(KvCoder.of(VarIntCoder.of(), VarLongCoder.of()))
            .advanceWatermarkTo(START)
            // Sequence is sent as 0,2,1 with 1 sent 10 mins after 2.
            .addElements(KV.of(1, 0L))
            .advanceWatermarkTo(START.plus(Duration.standardSeconds(1)))
            .addElements(KV.of(1, 2L))
            .advanceWatermarkTo(START.plus(Duration.standardSeconds(100)))
            .addElements(KV.of(1, 1L))
            .advanceWatermarkToInfinity();

    PCollection<KV<Integer, Long>> ticks = p.apply(stream);

    PCollection<KV<Instant, Long>> output =
        ticks.apply(
            ParDo.of(
                TickerStream.GlobalSequenceTracker.builder()
                    .setBatchSize(Duration.standardSeconds(5))
                    .setEstimateFirstSeqNumSize(0L)
                    .build()));

    PAssert.that(output)
        .containsInAnyOrder(
            KV.of(START.plus(Duration.standardSeconds(5)), 1L),
            KV.of(START.plus(Duration.standardSeconds(105)), 3L));

    p.run();
  }

  @Test
  public void nonZeroSTARTValue() throws NoSuchSchemaException {

    TestStream<KV<Integer, Long>> stream =
        TestStream.create(KvCoder.of(VarIntCoder.of(), VarLongCoder.of()))
            .advanceWatermarkTo(START)
            // Sequence is sent as 0,2,1 with 1 sent 10 mins after 2.
            .addElements(KV.of(1, 3L))
            .advanceWatermarkTo(START.plus(Duration.standardSeconds(1)))
            .addElements(KV.of(1, 5L))
            .advanceWatermarkTo(START.plus(Duration.standardSeconds(100)))
            .addElements(KV.of(1, 4L))
            .advanceWatermarkToInfinity();

    PCollection<KV<Integer, Long>> ticks = p.apply(stream);

    PCollection<KV<Instant, Long>> output =
        ticks.apply(
            ParDo.of(
                TickerStream.GlobalSequenceTracker.builder()
                    .setBatchSize(Duration.standardSeconds(5))
                    .setEstimateFirstSeqNumSize(0L)
                    .build()));

    PAssert.that(output)
        .containsInAnyOrder(
            KV.of(START.plus(Duration.standardSeconds(5)), 4L),
            KV.of(START.plus(Duration.standardSeconds(105)), 6L));

    p.run();
  }

  @Test
  public void basicReleaseChecker() {

    TestStream<KV<Instant, Long>> releaseMessage =
        TestStream.create(KvCoder.of(InstantCoder.of(), VarLongCoder.of()))
            .advanceWatermarkTo(START)
            .addElements(
                KV.of(START.plus(Duration.standardSeconds(5).minus(Duration.millis(1))), 1L))
            .advanceWatermarkToInfinity();

    Tick t0 =
        new Tick()
            .setGlobalSequence(0L)
            .setId("G")
            .setOrder(new Order("G", 1.0, false, Order.TYPE.ADD));

    Tick t1 =
        new Tick()
            .setGlobalSequence(1L)
            .setId("G")
            .setOrder(new Order("G", 1.0, false, Order.TYPE.ADD));

    Tick t2 =
        new Tick()
            .setGlobalSequence(2L)
            .setId("G")
            .setOrder(new Order("G", 1.0, false, Order.TYPE.ADD));

    TestStream<Tick> stream =
        TestStream.create(SerializableCoder.of(Tick.class))
            .advanceWatermarkTo(START)
            // Sequence is sent as 0,2,1 with 1 sent 10 mins after 2.
            .addElements(t0)
            .advanceWatermarkTo(START.plus(Duration.standardSeconds(1)))
            .addElements(t2)
            .advanceWatermarkTo(START.plus(Duration.standardSeconds(1)))
            .addElements(t1)
            .advanceWatermarkToInfinity();

    NaiveOrderBook book = new NaiveOrderBook();

    book.add(t0.getOrder()).add(t1.getOrder()).add(t2.getOrder());

    PCollection<NaiveOrderBook> o =
        p.apply("S1", stream)
            .apply(WithKeys.<String, Tick>of(x -> x.getId()))
            .setCoder(KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(Tick.class)))
            .apply(
                ParDo.of(
                        new TickerStream.PerSymbolSequencer<NaiveOrderBook>(
                            NaiveOrderBook.class, SerializableCoder.of(NaiveOrderBook.class)))
                    .withSideInput(
                        TickerStream.SIDE_INPUT_NAME,
                        p.apply("S2", releaseMessage)
                            .apply(
                                "Win1",
                                Window.<KV<Instant, Long>>into(new GlobalWindows())
                                    .triggering(
                                        Repeatedly.forever(
                                            AfterProcessingTime.pastFirstElementInPane()))
                                    .withAllowedLateness(Duration.ZERO)
                                    .discardingFiredPanes())
                            .apply(View.asIterable())))
            .setCoder(SerializableCoder.of(NaiveOrderBook.class));

    PAssert.that(o).containsInAnyOrder(book);
    p.run();
  }

  @Test
  public void completeReleaseChecker() {

    TestStream<KV<Instant, Long>> releaseMessage =
        TestStream.create(KvCoder.of(InstantCoder.of(), VarLongCoder.of()))
            .advanceWatermarkTo(START)
            .addElements(
                KV.of(START.plus(Duration.standardSeconds(5).minus(Duration.millis(1))), 1L))
            .addElements(
                KV.of(START.plus(Duration.standardSeconds(15).minus(Duration.millis(1))), 3L))
            .advanceWatermarkToInfinity();

    Tick t0 =
        new Tick()
            .setGlobalSequence(0L)
            .setId("G")
            .setOrder(new Order("G", 1.0, false, Order.TYPE.ADD));

    Tick t1 =
        new Tick()
            .setGlobalSequence(1L)
            .setId("G")
            .setOrder(new Order("G", 2.0, false, Order.TYPE.ADD));

    Tick t2 =
        new Tick()
            .setGlobalSequence(2L)
            .setId("G")
            .setOrder(new Order("G", 3.0, false, Order.TYPE.ADD));

    TestStream<Tick> stream =
        TestStream.create(SerializableCoder.of(Tick.class))
            .advanceWatermarkTo(START)
            // Sequence is sent as 0,2,1 with 1 sent 10 mins after 2.
            .addElements(t0)
            .advanceWatermarkTo(START.plus(Duration.standardSeconds(5)).minus(Duration.millis(1)))
            .addElements(t2)
            .advanceWatermarkTo(START.plus(Duration.standardSeconds(15)).minus(Duration.millis(1)))
            .addElements(t1)
            .advanceWatermarkToInfinity();

    NaiveOrderBook book0 = new NaiveOrderBook();

    book0.add(t0.getOrder());

    NaiveOrderBook book1 = new NaiveOrderBook();

    book1.add(t0.getOrder()).add(t1.getOrder());

    NaiveOrderBook book2 = new NaiveOrderBook();

    book2.add(t0.getOrder()).add(t1.getOrder()).add(t2.getOrder());

    PCollection<NaiveOrderBook> o =
        p.apply("S1", stream)
            .apply(WithKeys.<String, Tick>of(x -> x.getId()))
            .setCoder(KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(Tick.class)))
            .apply(
                ParDo.of(
                        new TickerStream.PerSymbolSequencer<NaiveOrderBook>(
                            NaiveOrderBook.class, SerializableCoder.of(NaiveOrderBook.class)))
                    .withSideInput(
                        "GlobalSeqWM",
                        p.apply("S2", releaseMessage)
                            .apply(
                                "Win1",
                                Window.<KV<Instant, Long>>into(new GlobalWindows())
                                    .triggering(
                                        Repeatedly.forever(
                                            AfterProcessingTime.pastFirstElementInPane()))
                                    .withAllowedLateness(Duration.ZERO)
                                    .discardingFiredPanes())
                            .apply(View.asIterable())))
            .setCoder(SerializableCoder.of(NaiveOrderBook.class))
            .apply(Window.into(FixedWindows.of(TickerStream.BATCH_DURATION)));

    o.apply(Reify.windows()).apply(ParDo.of(new Print()));

    PAssert.that(o)
        .inWindow(new IntervalWindow(START, START.plus(TickerStream.BATCH_DURATION)))
        .containsInAnyOrder(book0)
        .inWindow(
            new IntervalWindow(
                START.plus(Duration.standardSeconds(10)), START.plus(Duration.standardSeconds(15))))
        .containsInAnyOrder(book2);
    PAssert.that(o).containsInAnyOrder(book0, book2);

    p.run();
  }

  static class Print extends DoFn<ValueInSingleWindow<NaiveOrderBook>, String> {

    @ProcessElement
    public void blah(ProcessContext pc) {
      LoggerFactory.getLogger(TickerStreamTest.class)
          .info("Contains: Window {} Value {}", pc.element().getWindow(), pc.element());
    }
  }

  @Test
  //  @Ignore("Orderlist bug?")
  public void maxSequenceNumberTest() {

    TestStream<KV<Integer, Long>> stream =
        TestStream.create(KvCoder.of(VarIntCoder.of(), VarLongCoder.of()))
            .advanceWatermarkTo(START)
            .addElements(KV.of(1, Long.MAX_VALUE - TickerStream.BATCH_DURATION.getMillis() - 2))
            .addElements(KV.of(1, Long.MAX_VALUE - TickerStream.BATCH_DURATION.getMillis() - 1))
            .advanceWatermarkTo(START.plus(Duration.standardSeconds(1)))
            .advanceWatermarkToInfinity();

    PCollection<KV<Integer, Long>> ticks = p.apply(stream);

    PCollection<KV<Instant, Long>> output =
        ticks.apply(
            ParDo.of(
                TickerStream.GlobalSequenceTracker.builder()
                    .setBatchSize(Duration.standardSeconds(5))
                    .setEstimateFirstSeqNumSize(Long.MAX_VALUE - 5)
                    .build()));

    PAssert.that(output)
        .containsInAnyOrder(
            KV.of(
                START.plus(Duration.standardSeconds(5)),
                Long.MAX_VALUE - TickerStream.BATCH_DURATION.getMillis()));
    p.run();
  }

  @Test
  public void endToEndTest() {

    Tick t0 =
        new Tick()
            .setGlobalSequence(0L)
            .setId("G")
            .setOrder(new Order("G", 1.0, false, Order.TYPE.ADD));

    Tick t1 =
        new Tick()
            .setGlobalSequence(1L)
            .setId("G")
            .setOrder(new Order("G", 2.0, false, Order.TYPE.ADD));

    Tick t2 =
        new Tick()
            .setGlobalSequence(2L)
            .setId("G")
            .setOrder(new Order("G", 3.0, false, Order.TYPE.ADD));

    TestStream<Tick> stream =
        TestStream.create(SerializableCoder.of(Tick.class))
            .advanceWatermarkTo(START)
            .addElements(t0)
            .advanceWatermarkTo(START.plus(Duration.standardSeconds(5)))
            .addElements(t2)
            .advanceWatermarkTo(START.plus(Duration.standardSeconds(15)).minus(Duration.millis(1)))
            .addElements(t1)
            .advanceWatermarkToInfinity();

    NaiveOrderBook book0 = new NaiveOrderBook();

    book0.add(t0.getOrder());

    NaiveOrderBook book1 = new NaiveOrderBook();

    book1.add(t0.getOrder()).add(t1.getOrder());

    NaiveOrderBook book2 = new NaiveOrderBook();

    book2.add(t0.getOrder()).add(t1.getOrder()).add(t2.getOrder());

    SerializableCoder<NaiveOrderBook> coder = SerializableCoder.of(NaiveOrderBook.class);

    assert coder != null;

    PCollection<NaiveOrderBook> o =
        p.apply("S1", stream)
            .apply(
                TickerStream.<NaiveOrderBook>create(
                    TickerStream.Mode.MULTIPLEX_STREAM, NaiveOrderBook.class))
            .apply(Window.into(FixedWindows.of(TickerStream.BATCH_DURATION)));

    o.apply(Reify.windows()).apply(ParDo.of(new Print()));

    // As Timers are going to be set 5 sec into the future, the first release will not happen until
    // [5,10)
    PAssert.that(o)
        .inWindow(
            new IntervalWindow(
                START.plus(Duration.standardSeconds(5)), START.plus(Duration.standardSeconds(10))))
        .containsInAnyOrder(book0)
        .inWindow(
            new IntervalWindow(
                START.plus(Duration.standardSeconds(15)), START.plus(Duration.standardSeconds(20))))
        .containsInAnyOrder(book2);
    PAssert.that(o).containsInAnyOrder(book0, book2);

    p.run();
  }

  public static class GenerateTicks extends DoFn<Long, Tick> {

    @ProcessElement
    public void process(ProcessContext pc, @Element Long id) {

      String s = id % 2 == 0 ? "G" : "A";
      pc.output(
          new Tick()
              .setGlobalSequence(pc.element())
              .setId(s)
              .setOrder(new Order(s, 1.0, false, Order.TYPE.ADD)));
    }
  }
}

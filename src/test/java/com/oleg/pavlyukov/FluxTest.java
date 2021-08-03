package com.oleg.pavlyukov;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
class FluxTest {

    @Test
    void fluxJust() {
        Flux<String> stringFlux = Flux.just("One", "Two", "Three")
                .log()
                ;

        StepVerifier.create(stringFlux)
                .expectNext("One", "Two", "Three")
                .verifyComplete();
    }

    @Test
    void fluxRange() {
        Flux<Integer> numFlux = Flux.range(0, 10)
                ;

        numFlux.subscribe(num -> log.info("Num: {}", num));
    }

    @Test
    void fluxFromIterable() {
        Flux<Integer> numFlux = Flux.fromIterable(List.of(1, 2, 3, 4, 5, 6, 7))
                ;

        numFlux.subscribe(num -> log.info("Num: {}", num));
    }

    @Test
    void fluxSubOnError() {
        Flux<Integer> numFlux = Flux.range(0, 10)
                .map(n -> {
                    if (n > 7) {
                        throw new RuntimeException("Too big num: " + n);
                    }
                    return n;
                })
                .onErrorContinue((throwable, o) -> log.error("Error: {}; Object: {}", throwable.getClass().getName(), o))
//                .onErrorReturn(-1)
                .log()
                ;

        numFlux.subscribe(
                num -> log.info("Num: {}", num),
                e -> log.error("We got error! -> {}", e.getMessage()),
                () -> log.info("Operation complete!")
        );
    }

    @Test
    void fluxSubWithBaseSubNotSoUgly() {
        Flux<Integer> numFlux = Flux.range(0, 10)
//                .log()
                ;

        numFlux.subscribeWith(new BaseSubscriber<Integer>() {
            private int count = 0;
            private final int requestCount = 2;
            @Override
            protected void hookOnSubscribe(Subscription subscription) {
                request(requestCount);
            }

            @Override
            protected void hookOnNext(Integer value) {
                count++;
                if (count >= requestCount) {
                    count = 0;
                    request(requestCount);
                    log.info("------------Next batch------------");
                }
                log.info("Num: {}", value);
            }
        });
    }

    @Test
    void fluxInterval() throws InterruptedException {
        Flux<Long> fluxInterval = Flux.interval(Duration.ofMillis(100))
                .log()
                .take(3)
                ;

        fluxInterval.subscribe(i -> log.info("Try count: {}", i));

        TimeUnit.SECONDS.sleep(1);
    }

    @Test
    void fluxIntervalTest() {
        StepVerifier.withVirtualTime(this::createFluxInterval)
                .expectSubscription()
                .expectNoEvent(Duration.ofHours(24))
                .thenAwait(Duration.ofDays(1))
                .expectNext(0L)
                .thenAwait(Duration.ofDays(1))
                .expectNext(1L)
                .thenCancel()
                .verify();
    }

    Flux<Long> createFluxInterval() {
        return Flux.interval(Duration.ofDays(1))
                .doOnNext(n -> log.info("{} days off", n + 1))
                ;
    }

    @Test
    void fluxRangeBackpressurePretty() {
        Flux<Integer> numFlux = Flux.range(0, 10)
                .log()
                .limitRate(3)
                ;

        numFlux.subscribe(num -> log.info("Num: {}", num));
    }

    @Test
    void connectableFlux() throws InterruptedException {
        ConnectableFlux<Integer> publisher = Flux.range(1, 20)
                .log()
                .delayElements(Duration.ofMillis(100))
                .publish();

        Flux<Integer> integerFlux = publisher.autoConnect(2);

        System.out.println("Stage 0");

        TimeUnit.MILLISECONDS.sleep(600);
        System.out.println("Stage 1");

        integerFlux.subscribe(el -> log.info("Sub1 got new element: {}", el));

        TimeUnit.MILLISECONDS.sleep(1000);
        System.out.println("Stage 2");

        integerFlux.subscribe(el -> log.info("Sub2 got new element: {}", el));
        TimeUnit.MILLISECONDS.sleep(500);

        TimeUnit.MILLISECONDS.sleep(1000);
        System.out.println("Stage 3");

        integerFlux.subscribe(el -> log.info("Sub3 got new element: {}", el));
        TimeUnit.MILLISECONDS.sleep(1000);
    }

    @Test
    void fluxZip() {
        Flux<String> fluxFruits = Flux.just("apple", "pear", "plum");
        Flux<String> fluxColors = Flux.just("red", "green", "blue");
        Flux<Integer> fluxAmounts = Flux.just(10, 20, 30);
        Flux.zip(fluxFruits, fluxColors, fluxAmounts).subscribe(System.out::println);
    }
}

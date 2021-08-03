package com.oleg.pavlyukov;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@Slf4j
class MonoTest {

    @Test
    void testMonoSubscriber() {
        String str = "Some info";

        Mono<String> stringMono = Mono.just(str).log();

        stringMono.subscribe();

        StepVerifier.create(stringMono)
                .expectNext(str)
                .verifyComplete();
        log.info("Mono: {}", stringMono);
    }

    @Test
    void testMonoSubscriberConsumer() {
        String str = "Some info";

        Mono<String> stringMono = Mono.just(str).log();

        stringMono.subscribe(s -> log.info("Str value: {}", str));

        StepVerifier.create(stringMono)
                .expectNext(str)
                .verifyComplete();
        log.info("Mono: {}", stringMono);
    }

    @Test
    void testMonoSubscriberConsumerError() {
        String str = "Some info";

        Mono<String> stringMono = Mono.just(str)
                .map(s -> {throw new RuntimeException("Test ex");});

        stringMono.subscribe(
                s -> log.info("Str value: {}", str),
                e -> log.error("We got error! Msg: {}", e.getMessage())
        );

        StepVerifier.create(stringMono)
                .expectError(RuntimeException.class)
                .verify();
        log.info("Mono: {}", stringMono);
    }

    @Test
    void testMonoSubscriberConsumerComplete() {
        String str = "Some info";

        Mono<String> stringMono = Mono.just(str)
                .log()
                .map(String::toUpperCase);

        stringMono.subscribe(
                s -> log.info("Str value: {}", s),
                Throwable::printStackTrace,
                () -> log.info("Stage completed!")
        );

        StepVerifier.create(stringMono)
                .expectNext(str.toUpperCase())
                .verifyComplete();
        log.info("Mono: {}", stringMono);
    }

    @Test
    void testMonoSubscriberConsumerSubscription() {
        String str = "Some info";

        Mono<String> stringMono = Mono.just(str)
                .log()
                .map(String::toUpperCase);

        stringMono.subscribe(
                s -> log.info("Str value: {}", s),
                Throwable::printStackTrace,
                () -> log.info("Stage completed!"),
                Subscription::cancel
        );

        StepVerifier.create(stringMono)
                .expectNext(str.toUpperCase())
                .verifyComplete();
        log.info("Mono: {}", stringMono);
    }

    @Test
    void testMonoSubscriberConsumerSubscriptionBound() {
        String str = "Some info";

        Mono<String> stringMono = Mono.just(str)
                .log()
                .map(String::toUpperCase);

        stringMono.subscribe(
                s -> log.info("Str value: {}", s),
                Throwable::printStackTrace,
                () -> log.info("Stage completed!"),
                subscription -> subscription.request(1)
        );

        StepVerifier.create(stringMono)
                .expectNext(str.toUpperCase())
                .verifyComplete();
        log.info("Mono: {}", stringMono);
    }

    @Test
    void testMonoDoOnMethods() {
        String fighterName = "Oleg Pavlyukov";

        Mono<String> stringMono = Mono.just(fighterName)
                .log()
                .doOnSubscribe(subscription -> log.info("New subscription!"))
                .doOnRequest(longNumber -> log.info("We need {} fighters for new sub!", longNumber))
                .doOnNext(s -> log.info("We got {} fighter", s))
                .map(String::toUpperCase)
                .doOnNext(s -> log.info("We got {} fighter", s))
                .doOnSuccess(s -> log.info("Army gotten successfully!"));


        stringMono.subscribe(
                s -> log.info("Sub 1 Str value: {}", s),
                Throwable::printStackTrace,
                () -> log.info("Stage completed!")
        );

        log.info("Mono: {}", stringMono);
    }

    @Test
    void testMonoDoOnError() {
        String fighterName = "Oleg Pavlyukov";

        Mono<Object> errorMono = Mono.error(new IllegalArgumentException("Bad thing happend!"))
                .log()
                .doOnNext(o -> log.info("Printing next 1: {}", o))
                .onErrorReturn("Default value on error")
                .onErrorResume(e -> {
                    log.error("Wee got error! -> {}", e.getMessage());
                    return Mono.just(fighterName);
                })
                .doOnNext(o -> log.info("Printing next 2: {}", o))
                ;


        errorMono.subscribe(
                s -> log.info("Sub 1 Str value: {}", s),
                e -> {},
                () -> log.info("Stage completed!")
        );

//        StepVerifier.create(errorMono)
//                .expectError(IllegalArgumentException.class)
//                .verify();
    }
}

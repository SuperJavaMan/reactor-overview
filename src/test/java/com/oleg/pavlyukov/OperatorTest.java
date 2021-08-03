package com.oleg.pavlyukov;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuple3;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

@Slf4j
public class OperatorTest {

    @Test
    void subscribeOnSimple() {
        Flux<Integer> flux = Flux.range(1, 5)
                .map(el -> {
                    log.info("Map: 1 | Element: {} | Thread: {}", el, Thread.currentThread().getName());
                    return el;
                })
                .map(el -> {
                    log.info("Map: 2 | Element: {} | Thread: {}", el, Thread.currentThread().getName());
                    return el;
                })
                .subscribeOn(Schedulers.boundedElastic(), false)
                ;

        flux.subscribe(el -> log.info("Sub 1 Get Element: {}", el))
        ;
//        flux.subscribe(el -> log.info("Sub 2 Get Element: {}", el))
//        ;
//        flux.subscribe(el -> log.info("Sub 3 Get Element: {}", el))
//        ;

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1, 2, 3, 4, 5)
                .verifyComplete();

    }

    @Test
    void publishOnSimple() {
        Flux<Integer> flux = Flux.range(1, 2)
//                .publishOn(Schedulers.boundedElastic())
                .map(el -> {
                    log.info("Map: 1 | Element: {} | Thread: {}", el, Thread.currentThread().getName());
                    return el;
                })
                .publishOn(Schedulers.boundedElastic())
                .map(el -> {
                    log.info("Map: 2 | Element: {} | Thread: {}", el, Thread.currentThread().getName());
                    return el;
                })
//                .publishOn(Schedulers.boundedElastic())
                ;

        flux.subscribe(el -> log.info("Sub 1 Get Element: {}", el))
        ;
        flux.subscribe(el -> log.info("Sub 2 Get Element: {}", el))
        ;
//        flux.subscribe(el -> log.info("Sub 3 Get Element: {}", el))
//        ;

//        StepVerifier.create(flux)
//                .expectSubscription()
//                .expectNext(1, 2)
//                .verifyComplete();

    }

    @Test
    void subscribeOnIO() throws InterruptedException {
        Mono<List<String>> monoText = Mono.fromCallable(() -> Files.readAllLines(Path.of("text-file.txt")))
                .log()
                .subscribeOn(Schedulers.boundedElastic());

        monoText.subscribe(data -> log.info("Data: " + data.toString()));

        Thread.sleep(2000);
    }

    @Test
    void deferOperator() throws InterruptedException {
        Mono<Long> mono = Mono.defer(() -> Mono.just(System.currentTimeMillis()))
                .subscribeOn(Schedulers.boundedElastic())
                ;

        mono.subscribe(v -> log.info("Current time is {}", v));
        mono.subscribe(v -> log.info("Current time is {}", v));
        mono.subscribe(v -> log.info("Current time is {}", v));
    }

    @Test
    void concatAndCombineFlux() throws InterruptedException {
        Mono<String> monoA = Mono.just("a");
        Mono<String> monoB = Mono.just("b");
        Flux<String> fluxCD = Flux.just("c", "d");
        Flux<String> fluxEFG = Flux.just("e", "f", "g");
        Flux<Integer> rangeFlux = Flux.range(1, 7)
                .delayElements(Duration.ofMillis(100));

        Flux<String> fluxAB = Flux.concat(monoA, monoB);
        Flux<String> unitedFlux = fluxAB.concatWith(fluxCD).concatWith(fluxEFG)
                .delayElements(Duration.ofMillis(150));

        Flux<String> resultFlux = Flux.combineLatest(
                unitedFlux,
                rangeFlux,
                (f1, f2) -> f1 + f2
        );

        resultFlux.subscribe(System.out::println);
        Thread.sleep(1000);
    }

    @Test
    void mergeAndMergeWithFlux() throws InterruptedException {
        Flux<Integer> flux1 = Flux.range(1, 5)
                .delayElements(Duration.ofMillis(100));
        Flux<Integer> flux2 = Flux.range(6, 5)
                .delayElements(Duration.ofMillis(100));
        Flux<Integer> flux3 = Flux.range(10, 5)
                .delayElements(Duration.ofMillis(150));

        Flux<Integer> mergeFlux = Flux.merge(flux1, flux2)
                .delayElements(Duration.ofSeconds(1));
        Flux<Integer> unitedMergeFlux = mergeFlux.mergeWith(flux3);

        unitedMergeFlux.subscribe(el -> log.info(el.toString()));

        Thread.sleep(10000);
    }

    @Test
    void mergeSequenceVsConcat() throws InterruptedException {
        Flux<Integer> integerFlux1 = Flux.range(1, 5)
                .delayElements(Duration.ofMillis(400));
        Flux<Integer> integerFlux2 = Flux.range(6, 5)
                .delayElements(Duration.ofMillis(300));

        // подписывается на все сразу и собирает данные из обеих паблишеров одновременно,
        // а потом выдает последовательность
        Flux<Integer> mergeSequential = Flux.mergeSequential(integerFlux1, integerFlux2);

        mergeSequential.subscribe(el -> log.info("MergeSequence El: {}", el));

        Flux<Integer> integerFlux3 = Flux.range(1, 5)
                .delayElements(Duration.ofMillis(400));
        Flux<Integer> integerFlux4 = Flux.range(6, 5)
                .delayElements(Duration.ofMillis(300));

        // подписывается последовательно после завершения вычитки из предыдущего.
        // Поэтому подписчик получает меньше элементов,
        // т.к. первый паблишер медленный, а подписка на второго произойдет после завершения первого
        Flux<Integer> concat = Flux.concat(integerFlux3, integerFlux4);

        concat.subscribe(el -> log.info("Concat El: {}", el));
        Thread.sleep(2500);
    }

    @Test
    void mergeDelayError() throws InterruptedException {
        Flux<Integer> flux1 = Flux.range(1, 5)
                .delayElements(Duration.ofMillis(100));
        Flux<Integer> flux2 = Flux.range(6, 5);
        Flux<Integer> flux3 = Flux.range(10, 5)
                .map(el -> {
                    if (el > 12) {
                        throw new IllegalArgumentException();
                    }
                    return el;
                });

        Flux<Integer> mergeFlux = Flux.merge(flux1, flux2);
        Flux<Integer> unitedMergeFlux = Flux.mergeDelayError(1, mergeFlux, flux3)
                .doOnError(e -> log.error("We got error!"));

        unitedMergeFlux.subscribe(el -> log.info(el.toString()));

        Thread.sleep(10000);
    }

    @Test
    void flatMapAndSequence() throws InterruptedException {
        Flux<Flux<Integer>> fluxFlux = Flux.just(
                Flux.range(1, 5)
                        .delayElements(Duration.ofMillis(100)),
                Flux.range(6, 5)
        );

        Flux<Integer> casualFlux = fluxFlux.flatMap(f -> f);

        Flux<Integer> sequenceFlux = fluxFlux.flatMapSequential(f -> f);

        casualFlux.subscribe(el -> log.info("Casual flux el: {}", el.toString()));

        Thread.sleep(1000);
        System.out.println("---------------------------------------------------------------------------------------");
        sequenceFlux.subscribe(el -> log.info("Sequential flux el: {}", el.toString()));

        Thread.sleep(1000);
    }

    @Test
    void zipFlux() {
        Flux<String> uuidFlux = Flux.generate(synchronousSink -> synchronousSink.next(UUID.randomUUID().toString()));
        Flux<Integer> productNameFlux = Flux.range(1, 40);
        Flux<Integer> productCodeFlux = Flux.range(1000, 150);

        Flux<Tuple3<String, Integer, Integer>> productInfo = Flux.zip(uuidFlux, productNameFlux, productCodeFlux);

        productInfo.subscribe(tuple -> log.info("Product id: {}, name: {}, code: {}", tuple.getT1(), tuple.getT2(), tuple.getT3()));

        Flux<Product> productFlux = productInfo.map(tuple -> new Product(tuple.getT1(), tuple.getT2(), tuple.getT3()));

        productFlux.subscribe(p -> log.info(p.toString()));
    }

    @Data
    @AllArgsConstructor
    @ToString
    class Product {
        private String uuid;
        private Integer name;
        private Integer code;
    }
}

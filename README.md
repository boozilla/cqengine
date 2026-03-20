# CQEngine - Collection Query Engine

[![License](https://img.shields.io/github/license/boozilla/cqengine)](https://github.com/boozilla/cqengine/blob/main/LICENSE.txt)
[![Release](https://img.shields.io/github/v/release/boozilla/cqengine?sort=semver)](https://github.com/boozilla/cqengine/releases)

CQEngine은 Java 컬렉션을 SQL 유사 방식으로 질의할 수 있게 해주는 인메모리 쿼리 엔진입니다.
`IndexedCollection`에 인덱스를 구성해 반복(iteration) 기반 조회의 비용을 크게 줄이고, 낮은 지연 시간으로 조회할 수 있도록 설계되었습니다.

## 핵심 특징

- 타입 안전한 쿼리 API (`QueryFactory`)
- 다양한 인덱스 지원(Hash, Navigable, Radix/Suffix, Compound, Standing Query 등)
- 온힙/오프힙/디스크 기반 지속성 지원
- 기존 persistence를 감싸는 읽기 전용 `ReadOnlyPersistence` 지원
- primary key 기반 identity 및 기본 primary ascending 정렬 계약
- MVCC 기반 트랜잭션 격리(`TransactionalIndexedCollection`)
- Java Stream과 결합한 후처리(집계/변환) 지원

## 빠른 시작

### Gradle

```kotlin
dependencies {
    implementation("io.github.boozilla:cqengine:3.6.2")
}
```

최신 버전은 [릴리스 노트](https://github.com/boozilla/cqengine/wiki/ReleaseNotes)에서 확인하세요.

## 빌드 요구사항

- Gradle Wrapper(`gradle-9.3.1`) 실행에는 JDK 17 이상이 필요합니다.
- 라이브러리 바이트코드 타겟은 Java 8(`--release 8`)을 유지합니다.

### 예제

```java
import com.googlecode.cqengine.ConcurrentIndexedCollection;
import com.googlecode.cqengine.IndexedCollection;
import com.googlecode.cqengine.index.hash.HashIndex;
import com.googlecode.cqengine.resultset.ResultSet;

import static com.googlecode.cqengine.query.QueryFactory.equal;

IndexedCollection<Car> cars = new ConcurrentIndexedCollection<>();
cars.addIndex(HashIndex.onAttribute(Car.MANUFACTURER));

cars.add(new Car(1, "Ford", "Focus"));
cars.add(new Car(2, "Honda", "Civic"));

ResultSet<Car> result = cars.retrieve(equal(Car.MANUFACTURER, "Ford"));
try {
    result.forEach(System.out::println);
}
finally {
    result.close();
}
```

## 문서

### 입문/개요

- [문서 허브(Home)](https://github.com/boozilla/cqengine/wiki/Home)
- [반복의 한계](https://github.com/boozilla/cqengine/wiki/TheLimitsOfIteration)
- [벤치마크](https://github.com/boozilla/cqengine/wiki/Benchmark)
- [자주 묻는 질문](https://github.com/boozilla/cqengine/wiki/FrequentlyAskedQuestions)

### 쿼리/전략

- [중복 제거 전략](https://github.com/boozilla/cqengine/wiki/DeduplicationStrategies)
- [정렬 전략](https://github.com/boozilla/cqengine/wiki/OrderingStrategies)
- [병합 전략](https://github.com/boozilla/cqengine/wiki/MergeStrategies)
- [인덱스 양자화](https://github.com/boozilla/cqengine/wiki/IndexQuantization)
- [JOIN 수행](https://github.com/boozilla/cqengine/wiki/Joins)

### 속성/모델링

- [람다 속성](https://github.com/boozilla/cqengine/wiki/LambdaAttributes)
- [속성 자동 생성](https://github.com/boozilla/cqengine/wiki/AutoGenerateAttributes)
- [기타 JVM 언어](https://github.com/boozilla/cqengine/wiki/OtherJVMLanguages)

### 운영/배포

- [트랜잭션 격리(MVCC)](https://github.com/boozilla/cqengine/wiki/TransactionIsolation)
- [다운로드](https://github.com/boozilla/cqengine/wiki/Downloads)
- [릴리스 노트](https://github.com/boozilla/cqengine/wiki/ReleaseNotes)

## 문자열 기반 질의

CQEngine은 프로그램적 쿼리 API 외에도 SQL/CQN 문자열 파서를 지원합니다.
관련 예제 코드는 아래 경로를 참고하세요.

- SQL 예제: [SQLQueryDemo.java](src/test/java/com/googlecode/cqengine/examples/parser/SQLQueryDemo.java)
- CQN 예제: [CQNQueryDemo.java](src/test/java/com/googlecode/cqengine/examples/parser/CQNQueryDemo.java)

## 지속성 및 트랜잭션

온힙/오프힙/디스크 지속성 조합 및 트랜잭션 격리는 아래 문서를 참고하세요.

### Primary-keyed Persistence 기본 계약

`OnHeapPersistence.onPrimaryKey(...)`, `OffHeapPersistence.onPrimaryKey(...)`, `DiskPersistence.onPrimaryKey(...)`,
`WrappingPersistence.aroundCollectionOnPrimaryKey(...)`처럼 primary key를 지정하면, CQEngine은 그 값을 저장소
identity와 기본 정렬 기준으로 함께 사용합니다.

- collection iteration과 top-level `retrieve(query)`는 명시적 `orderBy(...)`가 없으면 primary ascending입니다.
- primary-keyed persistence에서 명시적 `orderBy(...)`를 사용해도, 동률은 primary key ascending으로 안정화되어 pagination 결과가 결정적입니다.
- ordered backing index를 사용할 수 있는 primary-keyed persistence에서는 이 기본 순서를 위해 매 쿼리마다 별도 materialized sort를 수행하지 않습니다.
- `update()`, `removeAll()`, `retainAll()` 같은 write path는 primary key 기준으로 교체/제거를 동기화합니다.
- `TransactionalIndexedCollection`은 primary-keyed persistence에서 same-primary replacement를 원자적 교체로 처리합니다.
- primary key가 없는 persistence는 기존처럼 기본 순서를 보장하지 않습니다.

명시적 `orderBy(...)`는 항상 이 기본 순서보다 우선하며, primary-keyed persistence에서는 primary key를 마지막 tie-breaker로 자동 추가합니다.

### 읽기 전용 persistence

기존 persistence를 CQEngine 조회 API로만 노출하고 싶다면 `ReadOnlyPersistence.around(...)`로 감쌀 수 있습니다.
이 API는 CQEngine을 통한 객체 수정만 막고, 조회와 인덱스 사용은 그대로 유지합니다.

```java
import com.googlecode.cqengine.persistence.wrapping.ReadOnlyPersistence;

IndexedCollection<Car> cars = new ConcurrentIndexedCollection<>(
        ReadOnlyPersistence.around(WrappingPersistence.aroundCollectionOnPrimaryKey(backingCollection, Car.CAR_ID))
);
```

- `add()`, `remove()`, `update()`, `clear()`, `iterator().remove()`는 `UnsupportedOperationException`을 던집니다.
- primary-keyed persistence를 감싼 경우에도 기본 primary ascending 순서와 backing index 최적화는 유지됩니다.
- backing collection 또는 backing database를 CQEngine 밖에서 직접 수정하는 것까지 막지는 않습니다.

- [트랜잭션 격리(MVCC)](https://github.com/boozilla/cqengine/wiki/TransactionIsolation)
- [병합 전략](https://github.com/boozilla/cqengine/wiki/MergeStrategies)
- [정렬 전략](https://github.com/boozilla/cqengine/wiki/OrderingStrategies)

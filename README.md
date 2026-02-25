# CQEngine - Collection Query Engine

[![License](https://img.shields.io/github/license/boozilla/cqengine)](https://github.com/boozilla/cqengine/blob/main/LICENSE.txt)
[![Release](https://img.shields.io/github/v/release/boozilla/cqengine?sort=semver)](https://github.com/boozilla/cqengine/releases)

CQEngine은 Java 컬렉션을 SQL 유사 방식으로 질의할 수 있게 해주는 인메모리 쿼리 엔진입니다.
`IndexedCollection`에 인덱스를 구성해 반복(iteration) 기반 조회의 비용을 크게 줄이고, 낮은 지연 시간으로 조회할 수 있도록 설계되었습니다.

## 핵심 특징

- 타입 안전한 쿼리 API (`QueryFactory`)
- 다양한 인덱스 지원(Hash, Navigable, Radix/Suffix, Compound, Standing Query 등)
- 온힙/오프힙/디스크 기반 지속성 지원
- MVCC 기반 트랜잭션 격리(`TransactionalIndexedCollection`)
- Java Stream과 결합한 후처리(집계/변환) 지원

## 빠른 시작

### Gradle

```kotlin
dependencies {
    implementation("io.github.boozilla:cqengine:3.6.1")
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

- [트랜잭션 격리(MVCC)](https://github.com/boozilla/cqengine/wiki/TransactionIsolation)
- [병합 전략](https://github.com/boozilla/cqengine/wiki/MergeStrategies)
- [정렬 전략](https://github.com/boozilla/cqengine/wiki/OrderingStrategies)

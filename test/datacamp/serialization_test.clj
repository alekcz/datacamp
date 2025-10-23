(ns datacamp.serialization-test
  (:require [clojure.test :refer [deftest is testing]]
            [datacamp.serialization :as sut]))

(deftest serialize-deserialize-roundtrip-test
  (testing "Fressian format roundtrip"
    (let [data {:key "value" :number 42 :nested {:list [1 2 3]}}
          serialized (sut/serialize-to-bytes data :fressian)
          deserialized (sut/deserialize-from-bytes serialized :fressian)]
      (is (= data deserialized))))

  (testing "CBOR format roundtrip"
    (let [data {:key "value" :number 42 :nested {:list [1 2 3]}}
          serialized (sut/serialize-to-bytes data :cbor)
          deserialized (sut/deserialize-from-bytes serialized :cbor)]
      (is (= data deserialized)))))

(deftest double-precision-preservation-test
  (testing "CBOR preserves double precision"
    (let [test-doubles [0.1 0.2 0.3 3.141592653589793 1.7976931348623157E308]
          data {:doubles test-doubles
                :nested {:double-value 42.123456789012345
                         :schema-value 0.0}}
          serialized (sut/serialize-to-bytes data :cbor)
          deserialized (sut/deserialize-from-bytes serialized :cbor)]

      ;; Verify all doubles are preserved
      (is (= (map double test-doubles)
             (map double (:doubles deserialized))))

      ;; Verify nested doubles
      (is (instance? Double (get-in deserialized [:nested :double-value])))
      (is (= 42.123456789012345 (get-in deserialized [:nested :double-value])))

      ;; Verify schema compatibility (the key issue from #633)
      (is (instance? Double (get-in deserialized [:nested :schema-value])))
      (is (= 0.0 (get-in deserialized [:nested :schema-value])))))

  (testing "Fressian preserves double precision naturally"
    (let [test-doubles [0.1 0.2 0.3 3.141592653589793]
          data {:doubles test-doubles}
          serialized (sut/serialize-to-bytes data :fressian)
          deserialized (sut/deserialize-from-bytes serialized :fressian)]

      (is (= test-doubles (:doubles deserialized)))
      (is (every? #(instance? Double %) (:doubles deserialized))))))

(deftest datom-serialization-test
  (testing "Datom chunk serialization with CBOR preserves doubles"
    (let [chunk-id "test-chunk"
          ;; Simulate datoms with double values (as maps that behave like datom records)
          datoms [{:e 1 :a :user/score :v 99.5 :tx 1000 :added true}
                  {:e 2 :a :user/rating :v 4.75 :tx 1000 :added true}]
          ;; serialize-datom-chunk will convert these to vectors internally
          serialized (sut/serialize-datom-chunk chunk-id datoms :cbor)
          deserialized (sut/deserialize-datom-chunk serialized :cbor)]

      (is (= chunk-id (:chunk/id deserialized)))
      (is (= :datom-chunk (:format/type deserialized)))
      (is (= :cbor (:format/serialization deserialized)))

      ;; Verify double values are preserved in datoms
      (let [restored-datoms (:datoms deserialized)]
        (is (= (count datoms) (count restored-datoms)))
        ;; Check that double values in the :v position (index 2) are preserved
        (is (instance? Double (nth (first restored-datoms) 2)))
        (is (= 99.5 (nth (first restored-datoms) 2)))
        (is (= 4.75 (nth (second restored-datoms) 2))))))

  (testing "Datom chunk serialization with Fressian"
    (let [chunk-id "test-chunk-fressian"
          datoms [{:e 1 :a :user/score :v 99.5 :tx 1000 :added true}]
          serialized (sut/serialize-datom-chunk chunk-id datoms :fressian)
          deserialized (sut/deserialize-datom-chunk serialized :fressian)]

      (is (= chunk-id (:chunk/id deserialized)))
      (is (= :datom-chunk (:format/type deserialized))))))

(deftest transaction-serialization-test
  (testing "Transaction serialization with CBOR preserves doubles"
    (let [tx-data {:tx-id 1000
                   :timestamp 1234567890
                   ;; Datoms as maps (like datom records) - will be converted to vectors internally
                   :datoms [{:e 1 :a :user/score :v 99.5 :tx 1000 :added true}
                            {:e 2 :a :user/rating :v 4.75 :tx 1000 :added true}]
                   :metadata {:source "test"}}
          serialized (sut/serialize-transaction tx-data :cbor)
          deserialized (sut/deserialize-transaction serialized :cbor)]

      (is (= :transaction (:format/type deserialized)))
      (is (= :cbor (:format/serialization deserialized)))
      (is (= 1000 (:tx-id deserialized)))

      ;; Verify doubles in datoms are preserved
      (let [restored-datoms (:datoms deserialized)]
        (is (instance? Double (nth (first restored-datoms) 2)))
        (is (= 99.5 (nth (first restored-datoms) 2)))
        (is (= 4.75 (nth (second restored-datoms) 2))))))

  (testing "Transaction serialization with Fressian"
    (let [tx-data {:tx-id 1000
                   :timestamp 1234567890
                   :datoms [{:e 1 :a :user/score :v 99.5 :tx 1000 :added true}]
                   :metadata {:source "test"}}
          serialized (sut/serialize-transaction tx-data :fressian)
          deserialized (sut/deserialize-transaction serialized :fressian)]

      (is (= :transaction (:format/type deserialized)))
      (is (= 1000 (:tx-id deserialized))))))

(deftest invalid-format-test
  (testing "Invalid serialization format throws error"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"Unsupported serialization format"
         (sut/serialize-to-bytes {:test "data"} :invalid-format))))

  (testing "Invalid deserialization format throws error"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"Unsupported deserialization format"
         (sut/deserialize-from-bytes (byte-array [1 2 3]) :invalid-format)))))

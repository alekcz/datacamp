(ns datacamp.parallel-test
  "Test parallel chunk upload functionality"
  (:require [clojure.test :refer :all]
            [datahike.api :as d]
            [datacamp.core :as backup]
            [datacamp.test-helpers :refer :all]))

(deftest test-parallel-backup
  (testing "Backup with different parallel values"
    (with-test-dir test-dir
      (with-test-db config {:store {:backend :mem :id "test-parallel-db"}}
        (let [conn (d/connect config)
              _ (populate-test-db conn :user-count 100 :post-count 500)
              ;; Test with small chunk size to force multiple chunks
              chunk-size (* 10 1024)]  ; 10KB - very small to create many chunks

          (testing "Sequential backup (parallel=1)"
            (let [start (System/currentTimeMillis)
                  result (backup/backup-to-directory
                          conn
                          {:path test-dir}
                          :chunk-size chunk-size
                          :parallel 1
                          :database-id "test-parallel-seq")
                  duration (- (System/currentTimeMillis) start)]
              (is (:success result))
              (println "Sequential (parallel=1) took:" duration "ms")
              (println "  Chunks:" (:chunk-count result))))

          (testing "Parallel backup (parallel=4)"
            (let [start (System/currentTimeMillis)
                  result (backup/backup-to-directory
                          conn
                          {:path test-dir}
                          :chunk-size chunk-size
                          :parallel 4
                          :database-id "test-parallel-p4")
                  duration (- (System/currentTimeMillis) start)]
              (is (:success result))
              (println "Parallel (parallel=4) took:" duration "ms")
              (println "  Chunks:" (:chunk-count result))))

          (testing "Parallel backup (parallel=8)"
            (let [start (System/currentTimeMillis)
                  result (backup/backup-to-directory
                          conn
                          {:path test-dir}
                          :chunk-size chunk-size
                          :parallel 8
                          :database-id "test-parallel-p8")
                  duration (- (System/currentTimeMillis) start)]
              (is (:success result))
              (println "Parallel (parallel=8) took:" duration "ms")
              (println "  Chunks:" (:chunk-count result)))))))))

(deftest test-parallel-api-acceptance
  (testing "API accepts parallel parameter"
    (with-test-dir test-dir
      (with-test-db config {:store {:backend :mem :id "test-api"}}
        (let [conn (d/connect config)
              _ (populate-test-db conn :user-count 5)
              result (backup/backup-to-directory
                      conn
                      {:path test-dir}
                      :parallel 4
                      :database-id "test-api")]
          (is (:success result))
          (is (pos? (:chunk-count result))))))))
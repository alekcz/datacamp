(ns datacamp.test-runner
  "Test runner for Datacamp test suite"
  (:require [clojure.test :refer [deftest is testing run-tests]]
            [datacamp.directory-test]
            [datacamp.postgres-test]
            [datacamp.mysql-test]
            [datacamp.redis-test]
            [datacamp.integration-test]))

(defn run-all-tests
  "Run all Datacamp tests"
  []
  (println "\n=== Running All Datacamp Tests ===\n")
  (run-tests 'datacamp.directory-test
             'datacamp.postgres-test
             'datacamp.mysql-test
             'datacamp.redis-test
             'datacamp.integration-test))

(defn run-directory-tests
  "Run only directory backup tests"
  []
  (println "\n=== Running Directory Tests ===\n")
  (run-tests 'datacamp.directory-test))

(defn run-backend-tests
  "Run tests for database backends (requires external services)"
  []
  (println "\n=== Running Backend Tests ===\n")
  (println "Note: These tests require running Postgres, MySQL, and Redis instances\n")
  (run-tests 'datacamp.postgres-test
             'datacamp.mysql-test
             'datacamp.redis-test))

(defn run-integration-tests
  "Run integration tests"
  []
  (println "\n=== Running Integration Tests ===\n")
  (run-tests 'datacamp.integration-test))

(defn run-quick-tests
  "Run quick tests (directory + integration, no external dependencies)"
  []
  (println "\n=== Running Quick Tests (No External Dependencies) ===\n")
  (run-tests 'datacamp.directory-test
             'datacamp.integration-test))

(defn print-test-info
  "Print information about available tests"
  []
  (println "\n=== Datacamp Test Suite ===\n")
  (println "Available test commands:")
  (println "  (run-all-tests)         - Run all tests")
  (println "  (run-quick-tests)       - Run tests without external dependencies")
  (println "  (run-directory-tests)   - Run directory backup tests only")
  (println "  (run-backend-tests)     - Run Postgres/MySQL/Redis tests")
  (println "  (run-integration-tests) - Run integration tests")
  (println "\nTest suites:")
  (println "  - directory-test:   Directory backup operations")
  (println "  - postgres-test:    PostgreSQL backend (requires Postgres)")
  (println "  - mysql-test:       MySQL backend (requires MySQL)")
  (println "  - redis-test:       Redis backend (requires Redis)")
  (println "  - integration-test: End-to-end workflows")
  (println "\nEnvironment variables for backend tests:")
  (println "  Postgres: POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB")
  (println "  MySQL:    MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB")
  (println "  Redis:    REDIS_HOST, REDIS_PORT, REDIS_PASSWORD")
  (println))

(defn -main
  "Main entry point for test runner"
  [& args]
  (let [test-type (first args)]
    (case test-type
      "all" (run-all-tests)
      "quick" (run-quick-tests)
      "directory" (run-directory-tests)
      "backend" (run-backend-tests)
      "integration" (run-integration-tests)
      "info" (print-test-info)
      (do
        (println "Usage: lein test-runner [all|quick|directory|backend|integration|info]")
        (print-test-info)))))

(comment
  ;; Run from REPL
  (print-test-info)
  (run-quick-tests)
  (run-all-tests))

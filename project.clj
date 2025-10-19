(defproject datacamp "0.1.0-SNAPSHOT"
  :description "Datahike S3 Backup Library - Production-ready backup solution with streaming, resumable operations, and live sync"
  :url "https://github.com/alekcz/datacamp"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [io.replikativ/datahike "0.6.1607"]
                 [com.cognitect.aws/api "0.8.774"]
                 [com.cognitect.aws/endpoints "871.2.35.6"]
                 [com.cognitect.aws/s3 "868.2.1580.0"]
                 [org.clojure/core.async "1.6.681"]
                 [org.clojure/data.fressian "1.0.0"]
                 [com.taoensso/timbre "6.8.0"]]
  :profiles {:dev {:dependencies [[org.clojure/test.check "1.1.1"]
                                  [nubank/matcher-combinators "3.8.8"]
                                  [io.replikativ/zufall "0.2.9"]
                                  ;; Datahike backend dependencies (test only)
                                  [io.replikativ/datahike-jdbc "0.3.50"]
                                  [io.replikativ/datahike-redis "0.1.7"]
                                  ;; Database drivers for JDBC backends
                                  [org.postgresql/postgresql "42.7.1"]
                                  [com.mysql/mysql-connector-j "8.2.0"]
                                  ;; SLF4J to Timbre adapter for Datahike logging
                                  [com.fzakaria/slf4j-timbre "0.4.0"]]}}
  :test-paths ["test"]
  :repl-options {:init-ns datacamp.core})

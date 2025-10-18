#!/usr/bin/env bb
;; Simple, runnable example of creating and backing up a small Datahike database
;;
;; Run with: lein repl < examples/run_simple_backup.clj
;; Or copy-paste into a REPL

(require '[datahike.api :as d])
(require '[datacamp.core :as backup])
(require '[clojure.java.io :as io])

(println "\n=== Datacamp Simple Backup Example ===\n")

;; 1. Create a small in-memory database
(println "Step 1: Creating in-memory database...")
(def cfg {:store {:backend :mem :id "book-library"}})
(d/create-database cfg)
(def conn (d/connect cfg))

;; 2. Define schema
(println "Step 2: Defining schema...")
(d/transact conn [{:db/ident :book/title
                   :db/valueType :db.type/string
                   :db/cardinality :db.cardinality/one}
                  {:db/ident :book/author
                   :db/valueType :db.type/string
                   :db/cardinality :db.cardinality/one}
                  {:db/ident :book/year
                   :db/valueType :db.type/long
                   :db/cardinality :db.cardinality/one}
                  {:db/ident :book/genre
                   :db/valueType :db.type/string
                   :db/cardinality :db.cardinality/one}])

;; 3. Add some books
(println "Step 3: Adding books to the database...")
(d/transact conn [{:book/title "The Hobbit"
                   :book/author "J.R.R. Tolkien"
                   :book/year 1937
                   :book/genre "Fantasy"}
                  {:book/title "1984"
                   :book/author "George Orwell"
                   :book/year 1949
                   :book/genre "Dystopian"}
                  {:book/title "To Kill a Mockingbird"
                   :book/author "Harper Lee"
                   :book/year 1960
                   :book/genre "Fiction"}
                  {:book/title "Pride and Prejudice"
                   :book/author "Jane Austen"
                   :book/year 1813
                   :book/genre "Romance"}
                  {:book/title "The Great Gatsby"
                   :book/author "F. Scott Fitzgerald"
                   :book/year 1925
                   :book/genre "Fiction"}])

;; 4. Query to show what we have
(println "Step 4: Querying the database...")
(def books (d/q '[:find ?title ?author ?year
                  :where
                  [?e :book/title ?title]
                  [?e :book/author ?author]
                  [?e :book/year ?year]]
                @conn))
(println (format "  Found %d books in the database:" (count books)))
(doseq [[title author year] (sort-by last books)]
  (println (format "    - \"%s\" by %s (%d)" title author year)))

;; 5. Create backup directory
(def backup-dir "examples/backups")
(println (format "\nStep 5: Creating backup in %s..." backup-dir))

;; 6. Perform backup
(def result (backup/backup-to-directory
             conn
             {:path backup-dir}
              :database-id "book-library"
              :compression :gzip))

(if (:success result)
  (do
    (println "\n✓ Backup completed successfully!")
    (println (format "  Backup ID: %s" (:backup-id result)))
    (println (format "  Location: %s" (:path result)))
    (println (format "  Datoms backed up: %d" (:datom-count result)))
    (println (format "  Chunks created: %d" (:chunk-count result)))
    (println (format "  Total size: %s" (:total-size-human result)))
    (println (format "  Duration: %.2f seconds" (:duration-seconds result)))

    ;; 7. List backup files
    (println "\nStep 6: Backup contents:")
    (def backup-path (io/file (:path result)))
    (println "  Files created:")
    (doseq [file (sort-by #(.getName %) (file-seq backup-path))]
      (when (.isFile file)
        (println (format "    - %s (%d bytes)"
                        (.getName file)
                        (.length file)))))

    ;; 8. Verify backup
    (println "\nStep 7: Verifying backup...")
    (def verification (backup/verify-backup-in-directory
                       {:path backup-dir}
                       (:backup-id result)
                       :database-id "book-library"))
    (if (:success verification)
      (do
        (println "  ✓ Backup verification passed!")
        (println (format "    - All chunks present: %s" (:all-chunks-present verification)))
        (println (format "    - Chunk count: %d" (:chunk-count verification))))
      (do
        (println "  ✗ Backup verification failed!")
        (when (:error verification)
          (println (format "    Error: %s" (:error verification))))
        (when (:missing-chunks verification)
          (println (format "    Missing chunks: %s" (:missing-chunks verification)))))))

    ;; 9. Instructions for restore
    (println "\n=== Next Steps ===")
    (println "To restore this backup later, you would:")
    (println "  1. Create a new empty database")
    (println "  2. Use the backup chunks to restore datoms")
    (println "  3. Transact them into the new database")
    (println (format "\nBackup location: %s" (:path result)))
    (println (format "Backup ID: %s" (:backup-id result))))

  (println (format "\n✗ Backup failed: %s" (:error result))))

;; Cleanup
(d/release conn)
(d/delete-database cfg)
(println "\n=== Example Complete ===\n")

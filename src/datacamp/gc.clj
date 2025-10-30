(ns datacamp.gc
  "Optimized garbage collection with resumable marking and batch deletion.
   This wraps Datahike's GC with battle-testable optimizations before they
   are contributed upstream."
  (:require [clojure.set :as set]
            [datahike.api :as d]
            [datahike.index.interface :refer [-mark]]
            [konserve.core :as k]
            [taoensso.timbre :as log]
            [superv.async :refer [<? S go-try <<?]]
            [clojure.core.async :as async]
            [datahike.schema-cache :as sc])
  (:import [java.util Date UUID]
           [java.util.concurrent Executors TimeUnit]))

;; =============================================================================
;; GC Checkpoint Management
;; =============================================================================

(defn create-gc-checkpoint
  "Create a new GC checkpoint for resumable marking"
  [gc-id branches]
  {:gc-id gc-id
   :version 1
   :started-at (Date.)
   :last-checkpoint (Date.)
   :visited #{}
   :reachable #{}
   :pending-branches (set branches)
   :completed-branches #{}
   :current-branch nil
   :stats {:commits-processed 0
           :datoms-marked 0
           :checkpoint-saves 0}})

(defn save-checkpoint!
  "Save GC checkpoint to store under :datacamp namespace"
  [store checkpoint]
  (go-try S
    (log/debug "Saving GC checkpoint" (:gc-id checkpoint))
    (<? S (k/assoc-in store [:datacamp :gc-checkpoint] checkpoint))
    (update-in checkpoint [:stats :checkpoint-saves] inc)))

(defn load-checkpoint
  "Load GC checkpoint from store"
  [store gc-id]
  (go-try S
    (let [checkpoint (<? S (k/get-in store [:datacamp :gc-checkpoint]))]
      (when (and checkpoint (= (:gc-id checkpoint) gc-id))
        (log/info "Loaded existing GC checkpoint" gc-id
                  "with" (count (:visited checkpoint)) "visited commits")
        checkpoint))))

(defn delete-checkpoint!
  "Clean up checkpoint after successful GC"
  [store]
  (go-try S
    (let [datacamp-data (<? S (k/get store :datacamp))]
      (if datacamp-data
        (<? S (k/assoc store :datacamp (dissoc datacamp-data :gc-checkpoint)))
        :no-checkpoint))))

;; =============================================================================
;; Resumable Marking Phase
;; =============================================================================

(defn- get-time [d]
  (.getTime ^Date d))

(defn- mark-commit-reachable
  "Mark all reachable items from a single commit"
  [commit config]
  (let [{:keys [eavt-key avet-key aevt-key
                temporal-eavt-key temporal-avet-key temporal-aevt-key
                schema-meta-key]} commit]
    (try
      (set/union
       (when schema-meta-key #{schema-meta-key})
       (if eavt-key (-mark eavt-key) #{})
       (if aevt-key (-mark aevt-key) #{})
       (if avet-key (-mark avet-key) #{})
       (when (:keep-history? config)
         (set/union
          (if temporal-eavt-key (-mark temporal-eavt-key) #{})
          (if temporal-aevt-key (-mark temporal-aevt-key) #{})
          (if temporal-avet-key (-mark temporal-avet-key) #{}))))
      (catch Exception e
        ;; Handle the case where indices aren't ready for marking
        ;; This can happen with empty or newly created databases
        (if (and (.getMessage e)
                 (.contains (.getMessage e) "flush"))
          (do
            (log/debug "Skipping commit marking - index not ready:" (.getMessage e))
            #{})
          (throw e))))))

(defn- should-save-checkpoint?
  "Determine if we should save a checkpoint based on time and commit count"
  [checkpoint commits-since-checkpoint]
  (let [time-since-checkpoint (- (System/currentTimeMillis)
                                 (.getTime (:last-checkpoint checkpoint)))
        ;; Save every 100 commits or 30 seconds
        should-save? (or (>= commits-since-checkpoint 100)
                        (> time-since-checkpoint 30000))]
    (when should-save?
      (log/debug "Checkpoint criteria met:"
                "commits:" commits-since-checkpoint
                "time:" time-since-checkpoint "ms"))
    should-save?))

(defn reachable-in-branch-resumable
  "Process a single branch with checkpoint support"
  [store branch after-date config checkpoint]
  (go-try S
    (let [head-cid (<? S (k/get-in store [branch :meta :datahike/commit-id]))
          initial-visited (or (:visited checkpoint) #{})
          initial-reachable (or (:reachable checkpoint) #{branch head-cid})]

      (log/info "Processing branch" branch "with" (count initial-visited) "already visited")

      (loop [[to-check & r] (if (contains? initial-visited branch)
                              [] ; Branch already processed
                              [branch])
             visited initial-visited
             reachable initial-reachable
             commits-since-checkpoint 0]

        (if to-check
          (if (visited to-check)
            ;; Skip already visited
            (recur r visited reachable commits-since-checkpoint)
            ;; Process commit
            (let [commit (<? S (k/get store to-check))]
              (if-not commit
                ;; Commit not found, skip
                (do
                  (log/warn "Commit not found:" to-check)
                  (recur r visited reachable commits-since-checkpoint))

                (let [{:keys [datahike/parents datahike/created-at datahike/updated-at]
                       :as meta} (:meta commit)
                      in-range? (> (get-time (or updated-at created-at))
                                  (get-time after-date))
                      commit-reachable (mark-commit-reachable commit config)
                      new-visited (conj visited to-check)
                      new-reachable (set/union reachable #{to-check} commit-reachable)]

                  ;; Save checkpoint if needed
                  (when (should-save-checkpoint? checkpoint commits-since-checkpoint)
                    (<? S (save-checkpoint!
                           store
                           (assoc checkpoint
                                  :visited new-visited
                                  :reachable new-reachable
                                  :current-branch branch
                                  :last-checkpoint (Date.)
                                  :stats (update (:stats checkpoint) :commits-processed
                                               + commits-since-checkpoint)))))

                  (recur (concat r (when in-range? parents))
                         new-visited
                         new-reachable
                         (if (should-save-checkpoint? checkpoint commits-since-checkpoint)
                           0
                           (inc commits-since-checkpoint)))))))

          ;; Branch complete
          {:visited visited
           :reachable reachable
           :branch branch})))))

(defn mark-phase-resumable
  "Resumable marking phase that saves checkpoints"
  [store branches after-date config & {:keys [resume-gc-id checkpoint-interval]
                                       :or {checkpoint-interval 100}}]
  (go-try S
    (let [gc-id (or resume-gc-id (str (UUID/randomUUID)))
          existing-checkpoint (when resume-gc-id
                               (<? S (load-checkpoint store resume-gc-id)))
          checkpoint (or existing-checkpoint
                        (create-gc-checkpoint gc-id branches))]

      (log/info (if existing-checkpoint "Resuming" "Starting")
                "GC mark phase with ID:" gc-id)

      ;; Process remaining branches
      (let [branches-to-process (set/difference
                                 (set branches)
                                 (:completed-branches checkpoint))]

        (log/info "Processing" (count branches-to-process) "branches")

        (loop [remaining branches-to-process
               current-checkpoint checkpoint]

          (if (empty? remaining)
            ;; All branches processed
            (do
              (log/info "Mark phase complete."
                       "Reachable items:" (count (:reachable current-checkpoint)))
              (:reachable current-checkpoint))

            ;; Process next branch
            (let [branch (first remaining)
                  _ (log/info "Processing branch" (inc (count (:completed-branches current-checkpoint)))
                             "of" (count branches) ":" branch)

                  result (<? S (reachable-in-branch-resumable
                               store branch after-date config current-checkpoint))

                  updated-checkpoint (-> current-checkpoint
                                       (update :visited set/union (:visited result))
                                       (update :reachable set/union (:reachable result))
                                       (update :completed-branches conj branch)
                                       (update :pending-branches disj branch)
                                       (assoc :current-branch nil
                                             :last-checkpoint (Date.)))]

              ;; Save checkpoint after each branch
              (<? S (save-checkpoint! store updated-checkpoint))

              (recur (rest remaining) updated-checkpoint))))))))

;; =============================================================================
;; Batch Deletion Sweep Phase
;; =============================================================================

(defn- batch-delete-keys!
  "Delete a batch of keys from the store.
   Uses our batch extensions if available, otherwise falls back to parallel deletes."
  [store keys]
  (go-try S
    ;; Try to use our batch extensions if loaded
    (if-let [batch-dissoc (try
                           (require '[datacamp.konserve-extensions :as ext])
                           (resolve 'datacamp.konserve-extensions/batch-dissoc!)
                           (catch Exception _ nil))]
      ;; Use our batch-dissoc! function
      (<? S (batch-dissoc store keys))

      ;; Fall back to parallel single deletes
      (let [delete-futures (map #(k/dissoc store %) keys)]
        (<? S (async/map (fn [& _] :done) delete-futures))
        (count keys)))))

(defn- create-deletion-batches
  "Split keys to delete into batches"
  [keys-to-delete batch-size]
  (partition-all batch-size keys-to-delete))

(defn sweep-batch!
  "Optimized sweep with batch deletion and progress reporting"
  [store whitelist ts & {:keys [batch-size parallel-batches]
                         :or {batch-size 1000
                              parallel-batches 1}}]
  (go-try S
    (log/info "Starting batch sweep phase with batch size:" batch-size)

    (let [all-metas (<? S (k/keys store))
          start-time (System/currentTimeMillis)

          ;; Pre-filter keys to delete (avoid repeated checks)
          keys-to-delete (reduce
                          (fn [acc {:keys [key last-write] :as meta}]
                            (if (or (contains? whitelist key)
                                   ;; Special handling for datacamp keys
                                   (= key :datacamp)
                                   (<= (.getTime ^Date ts)
                                       (.getTime (if last-write
                                                  ^Date last-write
                                                  ^Date (:konserve.core/timestamp meta)))))
                              acc
                              (conj acc key)))
                          []
                          all-metas)

          total-to-delete (count keys-to-delete)
          batches (create-deletion-batches keys-to-delete batch-size)]

      (log/info "Found" total-to-delete "keys to delete in"
               (count batches) "batches")

      (if (zero? total-to-delete)
        (do
          (log/info "No keys to delete")
          0)

        ;; Process batches with parallelism
        (loop [remaining-batches batches
               total-deleted 0]

          (if (empty? remaining-batches)
            ;; Complete
            (let [duration (- (System/currentTimeMillis) start-time)]
              (log/info "Sweep complete. Deleted" total-deleted "keys in"
                       (/ duration 1000.0) "seconds")
              total-deleted)

            ;; Process next batch(es)
            (let [batches-to-process (take parallel-batches remaining-batches)
                  batch-results (if (= 1 (count batches-to-process))
                                 ;; Single batch - process directly
                                 [(let [batch (first batches-to-process)]
                                    (<? S (batch-delete-keys! store batch)))]
                                 ;; Multiple batches - process in parallel
                                 (<<? S (async/map
                                        vector
                                        (map #(batch-delete-keys! store %)
                                             batches-to-process))))

                  batch-count (reduce + batch-results)
                  new-total (+ total-deleted batch-count)
                  progress-pct (* 100.0 (/ new-total total-to-delete))]

              ;; Progress reporting
              (when (or (>= progress-pct (* 10 (int (/ progress-pct 10))))
                       (>= new-total total-to-delete))
                (log/info (format "Sweep progress: %.1f%% (%d/%d keys deleted)"
                                 progress-pct new-total total-to-delete)))

              (recur (drop parallel-batches remaining-batches)
                     new-total))))))))

;; =============================================================================
;; Main GC Entry Points
;; =============================================================================

(defn gc-storage-optimized!
  "Optimized GC with resumable marking and batch deletion.

   Options:
   - :resume-gc-id - Resume from a previous GC run with this ID
   - :batch-size - Number of keys to delete per batch (default: 1000)
   - :parallel-batches - Number of batches to delete in parallel (default: 1)
   - :checkpoint-interval - Commits between checkpoints (default: 100)
   - :skip-mark-phase - Skip marking if you know what's reachable (dangerous!)
   - :dry-run - Run mark phase only, don't delete anything"
  [db & {:keys [remove-before resume-gc-id batch-size parallel-batches
                checkpoint-interval skip-mark-phase dry-run]
         :or {remove-before (Date. 0)
              batch-size 1000
              parallel-batches 1
              checkpoint-interval 100}}]
  (go-try S
    (let [start-time (System/currentTimeMillis)
          {:keys [config store]} db
          _ (sc/clear-write-cache (:store config))
          branches (<? S (k/get store :branches))]

      (log/info "Starting optimized GC"
               (when resume-gc-id (str "(resuming " resume-gc-id ")"))
               "for" (count branches) "branches")

      ;; Phase 1: Mark (resumable)
      (let [reachable (if skip-mark-phase
                       (do
                         (log/warn "Skipping mark phase - using provided reachable set")
                         #{:branches})
                       (let [marked (<? S (mark-phase-resumable
                                          store branches remove-before config
                                          :resume-gc-id resume-gc-id
                                          :checkpoint-interval checkpoint-interval))]
                         (conj marked :branches)))]

        (log/info "Mark phase complete. Reachable items:" (count reachable))

        (if dry-run
          ;; Dry run - report what would be deleted
          (let [all-metas (<? S (k/keys store))
                would-delete (remove #(contains? reachable (:key %)) all-metas)]
            (log/info "DRY RUN: Would delete" (count would-delete) "keys")
            ;; Clean up checkpoint after successful dry-run
            (<? S (delete-checkpoint! store))
            {:reachable-count (count reachable)
             :would-delete-count (count would-delete)
             :dry-run true})

          ;; Phase 2: Sweep (batch deletion)
          (let [deleted (<? S (sweep-batch! store reachable (Date.)
                                          :batch-size batch-size
                                          :parallel-batches parallel-batches))]

            ;; Clean up checkpoint after successful GC
            (<? S (delete-checkpoint! store))

            (let [duration (- (System/currentTimeMillis) start-time)]
              (log/info "GC complete in" (/ duration 1000.0) "seconds")
              {:reachable-count (count reachable)
               :deleted-count deleted
               :duration-ms duration})))))))

(defn resume-gc!
  "Resume an interrupted GC operation"
  [db gc-id & opts]
  (apply gc-storage-optimized! db :resume-gc-id gc-id opts))

(defn get-gc-status
  "Get the status of an ongoing or interrupted GC"
  [db]
  (go-try S
    (let [{:keys [store]} db
          checkpoint (<? S (k/get-in store [:datacamp :gc-checkpoint]))]
      (if checkpoint
        {:status :in-progress
         :gc-id (:gc-id checkpoint)
         :started-at (:started-at checkpoint)
         :last-checkpoint (:last-checkpoint checkpoint)
         :visited-count (count (:visited checkpoint))
         :reachable-count (count (:reachable checkpoint))
         :completed-branches (count (:completed-branches checkpoint))
         :pending-branches (count (:pending-branches checkpoint))
         :stats (:stats checkpoint)}
        {:status :no-gc-in-progress}))))

;; =============================================================================
;; Store-specific optimizations (can be extended for different backends)
;; =============================================================================

(defn optimize-for-backend
  "Return optimized settings based on the store backend"
  [store-config]
  (case (:backend store-config)
    :s3 {:batch-size 1000      ; S3 can delete 1000 objects per request
         :parallel-batches 3}   ; S3 handles parallel requests well

    :jdbc {:batch-size 5000     ; SQL can handle large IN clauses
           :parallel-batches 1}  ; Better to use single connection

    :file {:batch-size 100      ; File system benefits from smaller batches
           :parallel-batches 10} ; But high parallelism

    ;; Default for unknown backends
    {:batch-size 1000
     :parallel-batches 1}))
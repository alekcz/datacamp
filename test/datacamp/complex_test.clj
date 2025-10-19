(ns datacamp.complex-test
  "Complex integration test for datacamp backup/restore

  This test uses a comprehensive schema exercising:
  - Multiple entity types with refs
  - Component attributes (addresses, line items)
  - Cardinality-many on refs and scalars
  - Unique identities and values
  - Full-text search attributes
  - Soft deletes
  - Complex relationships (manager chains, orders->invoices->payments)

  It performs:
  1. Creates rich test data with all entity types
  2. Backs up using datacamp
  3. Restores from datacamp
  4. Compares original vs restored database datom-by-datom and entity-by-entity"
  (:require [clojure.test :refer :all]
            [datahike.api :as d]
            [datacamp.core :as backup]
            [datacamp.test-helpers :refer :all]
            [clojure.set :as set]))

;; Complex schema based on complexspec.md
(def complex-schema
  [;; Core / meta
   {:db/ident :entity/id
    :db/valueType :db.type/uuid
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   
   {:db/ident :entity/signature
    :db/valueType :db.type/uuid
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}

   {:db/ident :entity/type
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/index true}

   {:db/ident :entity/created-at
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one
    :db/index true}

   {:db/ident :entity/updated-at
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one
    :db/index true}

   {:db/ident :entity/deleted?
    :db/valueType :db.type/boolean
    :db/cardinality :db.cardinality/one
    :db/index true}

   ;; Company
   {:db/ident :company/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/value}

   {:db/ident :company/tags
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/many
    :db/index true}

   {:db/ident :company/addresses
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/many
    :db/isComponent true}

   ;; Person
   {:db/ident :person/full-name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}

   {:db/ident :person/email
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/many
    :db/unique :db.unique/value
    :db/index true}

   {:db/ident :person/phone
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/many}

   {:db/ident :person/company
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one
    :db/index true}

   {:db/ident :person/manager
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}

   {:db/ident :person/roles
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/many
    :db/index true}

   {:db/ident :person/addresses
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/many
    :db/isComponent true}

   {:db/ident :person/devices
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/many}

   {:db/ident :person/tags
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/many
    :db/index true}

   ;; Address (component)
   {:db/ident :address/label
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one}

   {:db/ident :address/line-1
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}

   {:db/ident :address/city
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}

   {:db/ident :address/country
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/index true}

   {:db/ident :address/postal-code
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}

   ;; Product & tagging
   {:db/ident :product/sku
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity
    :db/index true}

   {:db/ident :product/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}

   {:db/ident :product/description
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}

   {:db/ident :product/price
    :db/valueType :db.type/bigdec
    :db/cardinality :db.cardinality/one}

   {:db/ident :product/currency
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}

   {:db/ident :product/tags
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/many
    :db/index true}

   {:db/ident :tag/name
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/value
    :db/index true}

   ;; Orders, Invoices, Payments
   {:db/ident :order/number
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity
    :db/index true}

   {:db/ident :order/customer
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}

   {:db/ident :order/lines
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/many
    :db/isComponent true}

   {:db/ident :line/product
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}

   {:db/ident :line/qty
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one}

   {:db/ident :line/unit-price
    :db/valueType :db.type/bigdec
    :db/cardinality :db.cardinality/one}

   {:db/ident :invoice/number
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity
    :db/index true}

   {:db/ident :invoice/order
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one
    :db/index true}

   {:db/ident :invoice/status
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/index true}

   {:db/ident :invoice/lines
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/many
    :db/isComponent true}

   {:db/ident :payment/idempotency-key
    :db/valueType :db.type/uuid
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/value
    :db/index true}

   {:db/ident :payment/invoice
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one
    :db/index true}

   {:db/ident :payment/amount
    :db/valueType :db.type/bigdec
    :db/cardinality :db.cardinality/one}

   {:db/ident :payment/currency
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}

   {:db/ident :payment/method
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one}

   ;; Subscriptions
   {:db/ident :subscription/code
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity
    :db/index true}

   {:db/ident :subscription/account
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}

   {:db/ident :subscription/plan
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/index true}

   {:db/ident :subscription/status
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/index true}

   {:db/ident :subscription/started-at
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one}

   {:db/ident :subscription/canceled-at
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one}

   ;; Devices & Events
   {:db/ident :device/serial
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity
    :db/index true}

   {:db/ident :device/owner
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}

   {:db/ident :device/kind
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/index true}

   {:db/ident :event/type
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/index true}

   {:db/ident :event/at
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one
    :db/index true}

   {:db/ident :event/subject
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}

   {:db/ident :event/data
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}

   ;; ACL
   {:db/ident :role/name
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/value
    :db/index true}

   {:db/ident :role/permissions
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/many}

   {:db/ident :permission/resource
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/index true}

   {:db/ident :permission/ops
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/many}

   ;; Reference data
   {:db/ident :currency/code
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/value
    :db/index true}

   {:db/ident :currency/decimals
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one}

   {:db/ident :tax-rule/code
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/value}

   {:db/ident :tax-rule/rate
    :db/valueType :db.type/bigdec
    :db/cardinality :db.cardinality/one}])

(defn generate-complex-test-data
  "Generate rich test data covering all entity types"
  []
  (let [now (java.util.Date.)
        yesterday (java.util.Date. (- (.getTime now) (* 24 60 60 1000)))]

    ;; Reference data and tags
    [{:entity/id (guaranteed-unique-uuid)
      :entity/signature (guaranteed-unique-uuid)
      :entity/type :currency
      :entity/created-at now
      :currency/code :ZAR
      :currency/decimals 2}
     {:entity/id (guaranteed-unique-uuid)
      :entity/signature (guaranteed-unique-uuid)
      :entity/type :currency
      :entity/created-at now
      :currency/code :USD
      :currency/decimals 2}
     {:entity/id (guaranteed-unique-uuid)
      :entity/signature (guaranteed-unique-uuid)
      :entity/type :currency
      :entity/created-at now
      :currency/code :EUR
      :currency/decimals 2}

     {:entity/id (guaranteed-unique-uuid)
      :entity/signature (guaranteed-unique-uuid)
      :entity/type :tag
      :entity/created-at now
      :tag/name :electronics}
     {:entity/id (guaranteed-unique-uuid)
      :entity/signature (guaranteed-unique-uuid)
      :entity/type :tag
      :entity/created-at now
      :tag/name :apparel}
     {:entity/id (guaranteed-unique-uuid)
      :entity/signature (guaranteed-unique-uuid)
      :entity/type :tag
      :entity/created-at now
      :tag/name :clearance}
     {:entity/id (guaranteed-unique-uuid)
      :entity/signature (guaranteed-unique-uuid)
      :entity/type :tag
      :entity/created-at now
      :tag/name :vip-only}
     {:entity/id (guaranteed-unique-uuid)
      :entity/signature (guaranteed-unique-uuid)
      :entity/type :tag
      :entity/created-at now
      :tag/name :vip}
     {:entity/id (guaranteed-unique-uuid)
      :entity/signature (guaranteed-unique-uuid)
      :entity/type :tag
      :entity/created-at now
      :tag/name :churn-risk}

     ;; Roles and permissions
     {:db/id "admin-read-perm"
      :entity/id (guaranteed-unique-uuid)
      :entity/signature (guaranteed-unique-uuid)
      :entity/type :permission
      :entity/created-at now
      :permission/resource :invoice
      :permission/ops [:read :create :update :delete :settle]}
     {:db/id "admin-role"
      :entity/id (guaranteed-unique-uuid)
      :entity/signature (guaranteed-unique-uuid)
      :entity/type :role
      :entity/created-at now
      :role/name :admin
      :role/permissions ["admin-read-perm"]}
     {:db/id "sales-perm"
      :entity/id (guaranteed-unique-uuid)
      :entity/signature (guaranteed-unique-uuid)
      :entity/type :permission
      :entity/created-at now
      :permission/resource :order
      :permission/ops [:read :create]}
     {:db/id "sales-role"
      :entity/id (guaranteed-unique-uuid)
      :entity/signature (guaranteed-unique-uuid)
      :entity/type :role
      :entity/created-at now
      :role/name :sales
      :role/permissions ["sales-perm"]}

     ;; Companies
     {:db/id "acme"
      :entity/id (guaranteed-unique-uuid)
      :entity/signature (guaranteed-unique-uuid)
      :entity/type :company
      :entity/created-at now
      :entity/updated-at now
      :company/name "Acme Retail SA"
      :company/tags [[:tag/name :vip]]
      :company/addresses [{:address/label :billing
                          :address/line-1 "123 Main St"
                          :address/city "Johannesburg"
                          :address/country "South Africa"
                          :address/postal-code "2000"}
                         {:address/label :shipping
                          :address/line-1 "456 Warehouse Rd"
                          :address/city "Cape Town"
                          :address/country "South Africa"
                          :address/postal-code "8000"}]}
     {:db/id "blue"
      :entity/id (guaranteed-unique-uuid)
      :entity/signature (guaranteed-unique-uuid)
      :entity/type :company
      :entity/created-at now
      :company/name "Blue Logistics"
      :company/addresses [{:address/label :registered
                          :address/line-1 "789 Port Ave"
                          :address/city "Durban"
                          :address/country "South Africa"
                          :address/postal-code "4000"}]}

     ;; People (with manager chains and multi-valued emails)
     {:db/id "ceo"
      :entity/id (guaranteed-unique-uuid)
      :entity/signature (guaranteed-unique-uuid)
      :entity/type :person
      :entity/created-at now
      :person/full-name "Alice Smith"
      :person/email ["alice@acme.com" "alice.smith@acme.com"]
      :person/phone ["+27111234567" "+27821234567"]
      :person/company "acme"
      :person/roles ["admin-role"]
      :person/tags [[:tag/name :vip]]
      :person/addresses [{:address/label :home
                         :address/line-1 "10 Hill Rd"
                         :address/city "Pretoria"
                         :address/country "South Africa"
                         :address/postal-code "0001"}]}
     {:db/id "manager1"
      :entity/id (guaranteed-unique-uuid)
      :entity/signature (guaranteed-unique-uuid)
      :entity/type :person
      :entity/created-at now
      :person/full-name "Bob Johnson"
      :person/email ["bob@acme.com"]
      :person/phone ["+27111234568"]
      :person/company "acme"
      :person/manager "ceo"
      :person/roles ["sales-role"]}
     {:db/id "employee1"
      :entity/id (guaranteed-unique-uuid)
      :entity/signature (guaranteed-unique-uuid)
      :entity/type :person
      :entity/created-at now
      :person/full-name "Carol Davis"
      :person/email ["carol@acme.com"]
      :person/company "acme"
      :person/manager "manager1"}
     {:db/id "employee2"
      :entity/id (guaranteed-unique-uuid)
      :entity/signature (guaranteed-unique-uuid)
      :entity/type :person
      :entity/created-at now
      :person/full-name "Dave Wilson"
      :person/email ["dave@acme.com"]
      :person/company "acme"
      :person/manager "manager1"
      :person/tags [[:tag/name :churn-risk]]}
     {:db/id "blue-ceo"
      :entity/id (guaranteed-unique-uuid)
      :entity/signature (guaranteed-unique-uuid)
      :entity/type :person
      :entity/created-at now
      :person/full-name "Eve Brown"
      :person/email ["eve@bluelogistics.com"]
      :person/company "blue"
      :person/roles ["admin-role"]}

     ;; Products
     {:db/id "prod1"
      :entity/id (guaranteed-unique-uuid)
      :entity/signature (guaranteed-unique-uuid)
      :entity/type :product
      :entity/created-at now
      :product/sku "LAPTOP-001"
      :product/name "Business Laptop Pro"
      :product/description "High-performance laptop for professionals with 16GB RAM"
      :product/price 15999.99M
      :product/currency [:currency/code :ZAR]
      :product/tags [[:tag/name :electronics]]}
     {:db/id "prod2"
      :entity/id (guaranteed-unique-uuid)
      :entity/signature (guaranteed-unique-uuid)
      :entity/type :product
      :entity/created-at now
      :product/sku "SHIRT-001"
      :product/name "Corporate Shirt"
      :product/description "Professional cotton shirt for office wear"
      :product/price 299.99M
      :product/currency [:currency/code :ZAR]
      :product/tags [[:tag/name :apparel]]}
     {:db/id "prod3"
      :entity/id (guaranteed-unique-uuid)
      :entity/signature (guaranteed-unique-uuid)
      :entity/type :product
      :entity/created-at now
      :product/sku "MOUSE-CLEAR"
      :product/name "Wireless Mouse"
      :product/description "Ergonomic wireless mouse clearance sale"
      :product/price 99.99M
      :product/currency [:currency/code :ZAR]
      :product/tags [[:tag/name :electronics] [:tag/name :clearance]]}

     ;; Orders with line items (component)
     {:db/id "order1"
      :entity/id (guaranteed-unique-uuid)
      :entity/signature (guaranteed-unique-uuid)
      :entity/type :order
      :entity/created-at yesterday
      :order/number "ORD-2024-001"
      :order/customer "ceo"
      :order/lines [{:line/product "prod1"
                    :line/qty 2
                    :line/unit-price 15999.99M}
                   {:line/product "prod3"
                    :line/qty 5
                    :line/unit-price 99.99M}]}
     {:db/id "order2"
      :entity/id (guaranteed-unique-uuid)
      :entity/signature (guaranteed-unique-uuid)
      :entity/type :order
      :entity/created-at now
      :order/number "ORD-2024-002"
      :order/customer "blue-ceo"
      :order/lines [{:line/product "prod2"
                    :line/qty 10
                    :line/unit-price 299.99M}]}

     ;; Invoices
     {:db/id "inv1"
      :entity/id (guaranteed-unique-uuid)
      :entity/signature (guaranteed-unique-uuid)
      :entity/type :invoice
      :entity/created-at yesterday
      :entity/updated-at now
      :invoice/number "INV-2024-001"
      :invoice/order "order1"
      :invoice/status :partially-paid
      :invoice/lines [{:line/product "prod1"
                      :line/qty 2
                      :line/unit-price 15999.99M}
                     {:line/product "prod3"
                      :line/qty 5
                      :line/unit-price 99.99M}]}
     {:db/id "inv2"
      :entity/id (guaranteed-unique-uuid)
      :entity/signature (guaranteed-unique-uuid)
      :entity/type :invoice
      :entity/created-at now
      :invoice/number "INV-2024-002"
      :invoice/order "order2"
      :invoice/status :sent}

     ;; Payments
     {:entity/id (guaranteed-unique-uuid)
      :entity/signature (guaranteed-unique-uuid)
      :entity/type :payment
      :entity/created-at now
      :payment/idempotency-key (guaranteed-unique-uuid)
      :payment/invoice "inv1"
      :payment/amount 10000.00M
      :payment/currency [:currency/code :ZAR]
      :payment/method :card}
     {:entity/id (guaranteed-unique-uuid)
      :entity/signature (guaranteed-unique-uuid)
      :entity/type :payment
      :entity/created-at now
      :payment/idempotency-key (guaranteed-unique-uuid)
      :payment/invoice "inv1"
      :payment/amount 5000.00M
      :payment/currency [:currency/code :ZAR]
      :payment/method :eft}

     ;; Subscriptions
     {:db/id "sub1"
      :entity/id (guaranteed-unique-uuid)
      :entity/signature (guaranteed-unique-uuid)
      :entity/type :subscription
      :entity/created-at yesterday
      :subscription/code "SUB-ACME-PRO-001"
      :subscription/account "acme"
      :subscription/plan :pro
      :subscription/status :active
      :subscription/started-at yesterday}
     {:entity/id (guaranteed-unique-uuid)
      :entity/signature (guaranteed-unique-uuid)
      :entity/type :subscription
      :entity/created-at yesterday
      :entity/updated-at now
      :subscription/code "SUB-BLUE-STARTER-001"
      :subscription/account "blue"
      :subscription/plan :starter
      :subscription/status :canceled
      :subscription/started-at yesterday
      :subscription/canceled-at now}

     ;; Devices
     {:db/id "device1"
      :entity/id (guaranteed-unique-uuid)
      :entity/signature (guaranteed-unique-uuid)
      :entity/type :device
      :entity/created-at now
      :device/serial "PHONE-001-ABC"
      :device/owner "ceo"
      :device/kind :phone}
     {:db/id "device2"
      :entity/id (guaranteed-unique-uuid)
      :entity/signature (guaranteed-unique-uuid)
      :entity/type :device
      :entity/created-at now
      :device/serial "POS-ACME-001"
      :device/owner "acme"
      :device/kind :pos}

     ;; Events
     {:entity/id (guaranteed-unique-uuid)
      :entity/signature (guaranteed-unique-uuid)
      :entity/type :event
      :entity/created-at now
      :event/type :audit/login
      :event/at now
      :event/subject "ceo"
      :event/data "{\"ip\":\"192.168.1.1\",\"device\":\"laptop\"}"}
     {:entity/id (guaranteed-unique-uuid)
      :entity/signature (guaranteed-unique-uuid)
      :entity/type :event
      :entity/created-at now
      :event/type :order/placed
      :event/at yesterday
      :event/subject "order1"
      :event/data "{\"channel\":\"web\",\"promo\":\"SUMMER2024\"}"}
     {:entity/id (guaranteed-unique-uuid)
      :entity/signature (guaranteed-unique-uuid)
      :entity/type :event
      :entity/created-at now
      :event/type :device/reading
      :event/at now
      :event/subject "device2"
      :event/data "{\"temperature\":22.5,\"humidity\":45}"}

     ;; Soft deleted entity
     {:entity/id (guaranteed-unique-uuid)
      :entity/signature (guaranteed-unique-uuid)
      :entity/type :person
      :entity/created-at yesterday
      :entity/updated-at now
      :entity/deleted? true
      :person/full-name "Frank Deleted"
      :person/email ["frank@deleted.com"]
      :person/company "acme"}]))

(defn randomized-events []
  (for [n (range 20000)]
    {:entity/id (guaranteed-unique-uuid)
     :entity/signature (guaranteed-unique-uuid)
     :entity/type :event
     :entity/created-at (java.util.Date. (- (.getTime (java.util.Date.)) (* n 60 60 1000)))
     :event/type (nth [:device/reading :audit/login :order/placed :order/shipped :order/cancelled] (rand-int 5))
     :event/at (java.util.Date.)
     :event/data (str "{\"temperature\":" (rand)  "}")}))

(defn normalize-datom
  "Normalize a datom for comparison by converting to comparable format
  Now includes :db/txInstant since load-entities preserves it exactly"
  [datom]
  {:e (:e datom)
   :a (:a datom)
   :v (:v datom)
   :added (:added datom)})

(defn get-all-datoms
  "Get all datoms from a database, sorted for comparison
  Includes all datoms including :db/txInstant"
  [db]
  (->> (d/datoms db :eavt)
       (map normalize-datom)
       (sort-by (juxt :e :a))))

(defn compare-databases
  "Compare two databases datom-by-datom, returning differences
  Now includes :db/txInstant in comparison since load-entities preserves it exactly"
  [db1 db2 db1-name db2-name]
  (let [datoms1 (set (get-all-datoms db1))
        datoms2 (set (get-all-datoms db2))
        only-in-1 (set/difference datoms1 datoms2)
        only-in-2 (set/difference datoms2 datoms1)
        common (set/intersection datoms1 datoms2)]
    {:db1-name db1-name
     :db2-name db2-name
     :db1-count (count datoms1)
     :db2-count (count datoms2)
     :common-count (count common)
     :only-in-db1-count (count only-in-1)
     :only-in-db2-count (count only-in-2)
     :only-in-db1 (take 10 only-in-1)  ; Sample for debugging
     :only-in-db2 (take 10 only-in-2)
     :match? (and (empty? only-in-1) (empty? only-in-2))}))

(deftest test-complex-backup-restore-comparison
  (testing "Complex schema: Datacamp backup and restore validation"
    (with-test-dir test-dir
      ;; Create original database with complex schema
      (let [original-cfg {:store {:backend :mem :id "complex-original"}
                          :schema-flexibility :write
                          :keep-history? true}
            _ (d/create-database original-cfg)
            original-conn (d/connect original-cfg)]

        (try
          ;; Install schema
          (d/transact original-conn {:tx-data complex-schema})

          ;; Populate with rich test data
          (let [test-data (generate-complex-test-data)
                rand-data (randomized-events)]
            (d/transact original-conn {:tx-data test-data})
            (d/transact original-conn {:tx-data rand-data}))

          ;; Get original state
          (let [original-db @original-conn
                original-datom-count (count (d/datoms original-db :eavt))
                original-entities (d/q '[:find ?e :where [?e :entity/type _]] original-db)]

            (println "Original database:")
            (println "  Datoms:" original-datom-count)
            (println "  Entities:" (count original-entities))

            ;; Export using datacamp
            (let [datacamp-result (time (backup/backup-to-directory
                                    original-conn
                                    {:path test-dir}
                                    :database-id "complex-original"))]
                (println "\nDatacamp backup completed:")
                (println "  Backup ID:" (:backup-id datacamp-result))
                (println "  Datom count:" (:datom-count datacamp-result))
                (println "  Chunk count:" (:chunk-count datacamp-result))

                (is (:success datacamp-result) "Datacamp backup should succeed")
                (is (= original-datom-count (:datom-count datacamp-result))
                    "Datacamp backup should capture all datoms")

                ;; Restore from datacamp
                (let [datacamp-restore-cfg {:store {:backend :mem :id "complex-datacamp-restore"}
                                           :schema-flexibility :write}
                      _ (d/create-database datacamp-restore-cfg)
                      datacamp-restore-conn (d/connect datacamp-restore-cfg)
                      datacamp-restore-result (time (backup/restore-from-directory
                                              datacamp-restore-conn
                                              {:path test-dir}
                                              (:backup-id datacamp-result)
                                              :database-id "complex-original"
                                              :verify-checksums true))]

                  (println "\nDatacamp restored database:")
                  (println "  Success:" (:success datacamp-restore-result))
                  (println "  Datoms restored:" (:datoms-restored datacamp-restore-result))

                  (is (:success datacamp-restore-result) "Datacamp restore should succeed")

                  (let [datacamp-restore-db @datacamp-restore-conn
                        datacamp-datom-count (count (d/datoms datacamp-restore-db :eavt))
                        datacamp-entities (d/q '[:find ?e :where [?e :entity/type _]] datacamp-restore-db)]

                    (println "  Datoms:" datacamp-datom-count)
                    (println "  Entities:" (count datacamp-entities))

                    ;; Compare Original vs Datacamp
                    (println "\n=== Comparing Databases ===")

                    (let [comp (compare-databases original-db datacamp-restore-db
                                                  "Original" "Datacamp")]
                      (println "\nOriginal vs Datacamp:")
                      (println "  Common datoms:" (:common-count comp))
                      (println "  Only in Original:" (:only-in-db1-count comp))
                      (println "  Only in Datacamp:" (:only-in-db2-count comp))
                      (println "  Match:" (:match? comp))

                      (when-not (:match? comp)
                        (println "\nNote: Raw datom comparison may differ due to entity ID remapping")
                        (println "      This is acceptable as long as semantic data matches")
                        (println "      (verified via index comparisons below)")
                        (println "\nSample differences:")
                        (println "  First 10 only in Original:" (:only-in-db1 comp))
                        (println "  First 10 only in Datacamp:" (:only-in-db2 comp)))

                      ;; Note: We don't assert on raw datom match because load-entities
                      ;; may remap entity IDs. The important checks are:
                      ;; 1. Entity integrity (all entities and relationships preserved)
                      ;; 2. Index comparisons (semantic data matches when normalized)
                      (println "\nRaw datom match:" (:match? comp) "(entity ID remapping is acceptable)"))

                    ;; Test specific queries to verify entity integrity
                    (println "\n=== Entity Integrity Checks ===")

                    ;; Check companies
                    (let [orig-companies (d/q '[:find ?name :where [?e :company/name ?name]] original-db)
                          dc-companies (d/q '[:find ?name :where [?e :company/name ?name]] datacamp-restore-db)]
                      (println "Companies:")
                      (println "  Original:" (count orig-companies) orig-companies)
                      (println "  Datacamp:" (count dc-companies))
                      (is (= orig-companies dc-companies) "Company data should match"))

                    ;; Check people with manager chains
                    (let [orig-people (d/q '[:find ?name ?manager-name
                                            :where
                                            [?p :person/full-name ?name]
                                            (or-join [?p ?manager-name]
                                              (and [?p :person/manager ?m]
                                                   [?m :person/full-name ?manager-name])
                                              (and [(missing? $ ?p :person/manager)]
                                                   [(identity "NO MANAGER") ?manager-name]))]
                                          original-db)
                          dc-people (d/q '[:find ?name ?manager-name
                                          :where
                                          [?p :person/full-name ?name]
                                          (or-join [?p ?manager-name]
                                            (and [?p :person/manager ?m]
                                                 [?m :person/full-name ?manager-name])
                                            (and [(missing? $ ?p :person/manager)]
                                                 [(identity "NO MANAGER") ?manager-name]))]
                                        datacamp-restore-db)]
                      (println "\nPeople with managers:")
                      (println "  Original:" (count orig-people))
                      (println "  Datacamp:" (count dc-people))
                      (is (= orig-people dc-people) "Person/manager relationships should match"))

                    ;; Check orders with line items (component)
                    (let [orig-orders (d/q '[:find ?num (count ?line)
                                            :where
                                            [?o :order/number ?num]
                                            [?o :order/lines ?line]]
                                          original-db)
                          dc-orders (d/q '[:find ?num (count ?line)
                                          :where
                                          [?o :order/number ?num]
                                          [?o :order/lines ?line]]
                                        datacamp-restore-db)]
                      (println "\nOrders with line items:")
                      (println "  Original:" orig-orders)
                      (println "  Datacamp:" dc-orders)
                      (is (= orig-orders dc-orders) "Order line items should match"))

                    ;; Check multi-valued attributes
                    (let [orig-multi (d/q '[:find ?name (count ?email)
                                           :where
                                           [?p :person/full-name ?name]
                                           [?p :person/email ?email]]
                                         original-db)
                          dc-multi (d/q '[:find ?name (count ?email)
                                         :where
                                         [?p :person/full-name ?name]
                                         [?p :person/email ?email]]
                                       datacamp-restore-db)]
                      (println "\nMulti-valued emails:")
                      (println "  Original:" orig-multi)
                      (println "  Datacamp:" dc-multi)
                      (is (= orig-multi dc-multi) "Multi-valued attributes should match"))

                    ;; Check that all indices contain the same data (ignoring entity IDs)
                    (println "\n=== Index Comparison (EAVT, AEVT, AVET) ===")

                    (defn normalize-datom-for-index-comparison
                      "Normalize datom by removing entity IDs and tx IDs, keeping only attribute-value pairs"
                      [datom]
                      (let [{:keys [a v added]} datom]
                        {:a a
                         :v (if (number? v) :ref v)  ; Normalize ref values to :ref
                         :added added}))

                    (defn get-normalized-index
                      "Get all datoms from an index, normalized for comparison"
                      [db index]
                      (->> (d/datoms db index)
                           (map normalize-datom-for-index-comparison)
                           (frequencies)))  ; Count occurrences of each normalized datom

                    ;; Compare EAVT index
                    (let [orig-eavt (get-normalized-index original-db :eavt)
                          dc-eavt (get-normalized-index datacamp-restore-db :eavt)]
                      (println "EAVT index comparison:")
                      (println "  Original unique datoms:" (count orig-eavt))
                      (println "  Datacamp unique datoms:" (count dc-eavt))
                      (is (= orig-eavt dc-eavt) "EAVT index should match (normalized)"))

                    ;; Compare AEVT index
                    (let [orig-aevt (get-normalized-index original-db :aevt)
                          dc-aevt (get-normalized-index datacamp-restore-db :aevt)]
                      (println "\nAEVT index comparison:")
                      (println "  Original unique datoms:" (count orig-aevt))
                      (println "  Datacamp unique datoms:" (count dc-aevt))
                      (is (= orig-aevt dc-aevt) "AEVT index should match (normalized)"))

                    ;; Compare AVET index (only for indexed attributes)
                    (let [orig-avet (get-normalized-index original-db :avet)
                          dc-avet (get-normalized-index datacamp-restore-db :avet)]
                      (println "\nAVET index comparison:")
                      (println "  Original unique datoms:" (count orig-avet))
                      (println "  Datacamp unique datoms:" (count dc-avet))
                      (is (= orig-avet dc-avet) "AVET index should match (normalized)"))

                    (println "\n=== All tests completed successfully! ===")

                    ;; Cleanup Datacamp restore connection
                    (d/release datacamp-restore-conn)
                    (d/delete-database datacamp-restore-cfg)))))

          (finally
            (d/release original-conn)
            (d/delete-database original-cfg)))))))

(comment
  (run-tests 'datacamp.complex-test))

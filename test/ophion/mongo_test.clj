(ns ophion.mongo-test
  (:require
   [clojure.test :refer :all]
   [taoensso.timbre :as log]
   [ophion.config :as config]
   [ophion.mongo :as mongo]))

(def variant-query
  [{:label "Biosample"}
   {:match
    [[{:in ["hasSample"]}
      {:where {:name "CCLE"}}]
     [{:in ["variantFor"]}
      {:out ["variantIn"]}
      {:where {:symbol "BRAF"}}]]}])

(def variant-mongo
  [{:$match
    {:label "variantFor"
     :to {:$in (map :gid samples)}}}
   {:$project
    {:gid "$from"}}
   {:$skip 0}
   {:$limit 10000}])

(defn biosample-in-cohort
  [])

(defn variant-in-biosample
  [biosample]
  {:$match
   {:label "variantInBiosample"
    :to {:$in (map :gid biosample)}}}
  {:$project
   {:gid "$from"}}
  {:$skip 0}
  {:$limit 10000})

(deftest samples-with-variants
  (testing "finding all samples with a variant in a given gene"
    (let [query []])))

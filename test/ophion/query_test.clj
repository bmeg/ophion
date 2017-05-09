(ns ophion.query-test
  (:require
   [clojure.test :refer :all]
   [taoensso.timbre :as log]
   [ophion.config :as config]
   [ophion.db :as db]
   [ophion.query :as query])
  (:import
   [org.apache.tinkerpop.gremlin.tinkergraph.structure TinkerGraph]))

(def gods-graph
  {:vertexes
   [{:label "location" :gid "sky" :properties {:name "sky"}}
    {:label "location" :gid "sea" :properties {:name "sea"}}
    {:label "location" :gid "tartarus" :properties {:name "tartarus"}}
    {:label "titan" :gid "saturn" :properties {:name "saturn" :age 10000}}
    {:label "god" :gid "jupiter" :properties {:name "jupiter" :age 5000}}
    {:label "god" :gid "neptune" :properties {:name "neptune" :age 4500}}
    {:label "god" :gid "pluto" :properties {:name "pluto" :age 4000}}
    {:label "demigod" :gid "hercules" :properties {:name "hercules" :age 30}}
    {:label "human" :gid "alcmene" :properties {:name "alcmene" :age 45}}
    {:label "monster" :gid "nemean" :properties {:name "nemean"}}
    {:label "monster" :gid "hydra" :properties {:name "hydra"}}
    {:label "monster" :gid "cerberus" :properties {:name "cerberus"}}]
   :edges
   [{:from-label "god" :from "jupiter" :label "father" :to-label "god" :to "saturn"}
    {:from-label "god" :from "jupiter" :label "brother" :to-label "god" :to "neptune"}
    {:from-label "god" :from "jupiter" :label "brother" :to-label "god" :to "pluto"}
    {:from-label "god" :from "jupiter" :label "lives" :to-label "location" :to "sky" :properties {:reason "likes wind" :much 0.3}}
    {:from-label "god" :from "neptune" :label "brother" :to-label "god" :to "jupiter"}
    {:from-label "god" :from "neptune" :label "brother" :to-label "god" :to "pluto"}
    {:from-label "god" :from "neptune" :label "lives" :to-label "location" :to "sea" :properties {:reason "likes waves" :much 0.4}}
    {:from-label "god" :from "pluto" :label "brother" :to-label "god" :to "jupiter"}
    {:from-label "god" :from "pluto" :label "brother" :to-label "god" :to "neptune"}
    {:from-label "god" :from "pluto" :label "lives" :to-label "location" :to "tartarus" :properties {:reason "likes death" :much 0.5}}
    {:from-label "demigod" :from "hercules" :label "father" :to-label "god" :to "jupiter"}
    {:from-label "demigod" :from "hercules" :label "mother" :to-label "human" :to "alcmene"}
    {:from-label "demigod" :from "hercules" :label "battled" :to-label "monster" :to "nemean" :properties {:trial 1}}
    {:from-label "demigod" :from "hercules" :label "battled" :to-label "monster" :to "hydra" :properties {:trial 2}}
    {:from-label "monster" :from "hercules" :label "battled" :to-label "monster" :to "cerberus" :properties {:trial 12}}
    {:from-label "monster" :from "cerberus" :label "lives" :to-label "location" :to "tartarus"}]})

(def graph
  (query/ingest-graph! (TinkerGraph/open) gods-graph))

(def queries
  {:empty []

   ;; traversals
   :V [{:V []}]
   :E [{:E []}]
   :in [{:has-label ["location"]}
        {:in {}}]
   :out [{:has-label ["god"]}
         {:out {}}]
   :in-edge [{:has-label ["monster"]}
            {:in-edge []}]
   :out-edge [{:has-label ["demigod"]}
             {:out-edge []}]
   :in-vertex [{:has-label ["demigod"]}
               {:out-edge []}
               {:in-vertex []}]
   :out-vertex [{:has-label ["demigod"]}
                {:out-edge []}
                {:out-vertex []}]

   ;; traversal manipulation
   :select [{:has-label [:god]}
            {:as [:gods]}
            {:out-edge [:lives]}
            {:has {:key :much :value 0.5}}
            {:in-vertex :place}
            {:as [:places]}
            {:select [:gods :places]}
            {:by {:key :name}}
            {:order {:key :name :ascending true}}]
   
   :string-values [{:has-label ["god"]}
                   {:values ["name"]}]
   :number-values [{:has-label ["god"]}
                   {:values ["age"]}]

   :properties [{:has-label ["monster"]}
                {:properties []}]
   :property-map [{:has-label ["monster"]}
                  {:property-map []}]
   :limit [{:limit 3}]
   :order [{:order {:key :name :ascending true}}
           {:values [:name]}]

   :range [{:range {:lower 4 :upper 7}}]
   :count [{:count true}]
   :dedup [{:has-label ["demigod"]}
           {:out-edge []}
           {:out-vertex []}
           {:dedup []}]
   :path [{:has {:key :name :value "saturn"}}
          {:in [:father]}
          {:out [:brother]}
          {:out [:lives]}
          {:path true}]
   :aggregate [{:order {:key :name :ascending true}}
               {:values ["name"]}
               {:aggregate "everyone"}]
   :weird-group [{:where
            {:query
             [{:out-edge []}]}}
           {:group
            {:by
             [{:query
               [{:out-edge []}
                {:label true}]}]}}]
   :group [{:group
            {:by
             [{:query [{:label true}]}
              {:key "name"}]}}]

   :group-count-query [{:group-count
                         {:query
                          [{:out [:lives]}
                           {:label true}]}}]

   :group-count-key [{:group-count {:key :age}}]

   ;; conditions and traversals
   :is [{:has-label ["god"]}
        {:limit 1}
        {:values ["age"]}
        {:is {:eq 4000}}]
   :has-value [{:has {:key "name" :value "sky"}}]
   :has-condition [{:has {:key "age" :condition {:gt 3000}}}]
   :has-label [{:has-label ["location"]}]
   :has-not [{:has-not "age"}]

   :match [{:match
            [[{:as [:a]}
              {:out [:lives]}
              {:has {:key "name" :value "tartarus"}}]
             [{:as [:a]}
              {:in [:battled]}
              {:out [:mother]}
              {:has {:key "name" :value "alcmene"}}]]}
           {:select [:a]}
           {:values [:name]}]})

(defn test-query
  [key]
  (into
   []
   (query/evaluate graph (get queries key))))

(deftest empty-test
  (testing "if the query is empty it should just return all vertexes"
    (let [result (test-query :empty)]
      (is (= (count result) 12)))))

(deftest vertexes-test
  (testing "all the vertexes"
    (let [result (test-query :V)]
      (is (= (count result) 12)))))

(deftest edges-test
  (testing "all the edges"
    (let [result (test-query :E)]
      (is (= (count result) 16)))))

(deftest in-test
  (testing "in"
    (let [result (test-query :in)]
      (is (= (count result) 4))
      (is (= #{"god" "monster"} (set (map #(.label %) result)))))))

(deftest out-test
  (testing "out"
    (let [result (test-query :out)]
      (is (= (count result) 10))
      (is (= #{"god" "location" "titan"} (set (map #(.label %) result)))))))

(deftest in-edge-test
  (testing "in-edge"
    (let [result (test-query :in-edge)]
      (is (= (count result) 3))
      (is (= #{"battled"} (set (map #(.label %) result)))))))

(deftest out-edge-test
  (testing "out-edge"
    (let [result (test-query :out-edge)]
      (is (= (count result) 5))
      (is (= #{"father" "mother" "battled"} (set (map #(.label %) result)))))))

(deftest in-vertex-test
  (testing "in-vertex"
    (let [result (test-query :in-vertex)]
      (is (= (count result) 5))
      (is (= #{"god" "monster" "human"} (set (map #(.label %) result)))))))

(deftest out-vertex-test
  (testing "out-vertex"
    (let [result (test-query :out-vertex)]
      (is (= (count result) 5))
      (is (= #{"demigod"} (set (map #(.label %) result))))
      (is (= #{"hercules"} (set (map #(.value (.property % "name")) result)))))))

(deftest select-test
  (testing "select/as/by"
    (let [result (test-query :select)]
      (is (= (count result) 1))
      (is (= {"gods" "pluto" "places" "tartarus"} (first result))))))

(deftest string-values-test
  (testing "string values"
    (let [result (test-query :string-values)]
      (is (= #{"jupiter" "neptune" "pluto"} (set result))))))

(deftest number-values-test
  (testing "number values"
    (let [result (test-query :number-values)]
      (is (= #{5000 4500 4000} (set result))))))

(deftest properties-test
  (testing "properties"
    (let [result (test-query :properties)
          outcome (vals
                   (reduce
                    (fn [m vp]
                      (let [key (.key vp)
                            value (.value vp)
                            id (.id (.element vp))]
                        (assoc-in m [id key] value)))
                    {} result))]
      (is (= (count outcome) 3))
      (is (= #{"nemean" "hydra" "cerberus"} (set (map #(get % "name") outcome)))))))

(deftest property-map-test
  (testing "property-map"
    (let [result (test-query :property-map)]
      (log/info result)
      (is (= (count result) 3))
      (is (= #{"nemean" "hydra" "cerberus"}
             (set
              (map
               #(.value
                 (first
                  (get % "name")))
               result)))))))

(deftest dedup-test
  (testing "dedup"
    (let [result (test-query :dedup)]
      (is (= (count result) 1))
      (is (= #{"demigod"} (set (map #(.label %) result))))
      (is (= #{"hercules"} (set (map #(.value (.property % "name")) result)))))))

(deftest limit-test
  (testing "limit"
    (let [result (test-query :limit)]
      (is (= (count result) 3)))))

(deftest order-test
  (testing "order"
    (let [result (test-query :order)]
      (is (= "saturn" (first (drop 8 result)))))))

(deftest range-test
  (testing "range"
    (let [result (test-query :range)]
      (is (= (count result) 3)))))

(deftest count-test
  (testing "count"
    (let [result (test-query :count)]
      (is (= (first result) 12)))))

(deftest path-test
  (testing "path"
    (let [result (test-query :path)]
      (is (= (count result) 2))
      (is (= ["titan" "god" "god" "location"] (mapv #(.label %) (first result)))))))

(deftest aggregate-test
  (testing "aggregate"
    (let [result (test-query :aggregate)]
      (is (= (count result) 12))
      (is (= (first result) "alcmene")))))

(deftest weird-group-test
  (testing "weird group"
    (let [result (test-query :weird-group)
          groups (first result)]
      (is (= (count result) 1))
      (is (= (count groups) 2))
      (is (= (keys groups) ["mother" "lives"]))
      (is (= (.label (first (get groups "mother"))) "demigod")))))

(deftest group-test
  (testing "group"
    (let [result (test-query :group)
          groups (first result)]
      (is (= (count result) 1))
      (is (= (count groups) 6))
      (is (= (set (keys groups)) #{"location" "titan" "god" "demigod" "monster" "human"}))
      (is (= 3 (count (get groups "god")))))))

(deftest group-count-key-test
  (testing "group-count :key"
    (let [result (test-query :group-count-key)
          groups (first result)]
      (is (= (count result) 1))
      (is (= (count groups) 6))
      (is (= (set (keys groups)) #{10000 5000 4500 4000 45 30}))
      (is (= 1 (get groups 10000))))))

;; (deftest group-count-query-test
;;   (testing "group-count :query"
;;     (let [result (test-query :group-count-query)
;;           groups (first result)]
;;       (is (= (count result) 1))
;;       (is (= (count groups) 6))
;;       (is (= (set (keys groups)) #{"location" "titan" "god" "demigod" "monster" "human"}))
;;       (is (= 3 (count (get groups "god")))))))

(deftest filter-test
  (testing "has condition"
    (let [result (test-query :has-condition)]
      (is (= (count result) 4)))))

(deftest match-test
  (testing "match"
    (let [result (test-query :match)]
      (is (= (count result) 1))
      (is (= (first result) "cerberus")))))







;; (def genes (query/evaluate graph [{:has {:key :gid :value "individualCohort:TCGA-BRCA"}}
;;                             {:out ["hasMember"]}
;;                             {:in ["biosampleOfIndividual"]}
;;                             {:in ["variantInBiosample"]}
;;                             {:out ["variantInGene"]}
;;                             {:limit 10}]))

;; (query/evaluate graph [{:has {:key :gid :value "individualCohort:TCGA-BRCA"}}
;;                        {:out ["hasMember"]}
;;                        {:in ["biosampleOfIndividual"]}
;;                        {:in ["variantInBiosample"]}
;;                        {:out ["variantInGene"]}
;;                        {:group-count {:key :symbol}}])
